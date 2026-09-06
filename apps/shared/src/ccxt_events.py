"""Converters from CCXT unified structures to bus events.

The watchers call these right after a CCXT ``watch_*`` call returns, passing
the receive timestamp they took at that moment. Nothing here touches Redis,
and nothing here knows the order id conventions of the maker app: a caller
that can resolve an order to a strategy passes the result in.
"""

from decimal import Decimal
from typing import Any

from apps.shared.src.events import (
    AssetBalance,
    BalanceEvent,
    BookEvent,
    Fill,
    Level,
    Liquidity,
    OrderEvent,
    OrderState,
    Side,
    TradeEvent,
)

# Keys of a CCXT balance structure that are not asset codes.
_BALANCE_META_KEYS: frozenset[str] = frozenset(
    {"info", "timestamp", "datetime", "free", "used", "total", "debt"}
)


def _levels(raw: list[Any], depth: int) -> list[Level]:
    """
    Convert CCXT ``[price, size, ...]`` rows into ``(price, size)`` pairs.

    Parameters
    ----------
    raw : list[Any]
        One side of a CCXT order book.
    depth : int
        Maximum number of levels to keep.

    Returns
    -------
    list[Level]
        Top ``depth`` levels as float pairs.
    """
    return [(float(row[0]), float(row[1])) for row in raw[:depth]]


def _optional_int(value: Any) -> int | None:
    """
    Coerce a CCXT millisecond timestamp to ``int`` or ``None``.

    Parameters
    ----------
    value : Any
        Raw timestamp field.

    Returns
    -------
    int | None
        The timestamp, or None if missing.
    """
    return None if value is None else int(value)


def book_event_from_ccxt(
    venue: str,
    symbol: str,
    order_book: dict[str, Any],
    *,
    seq: int,
    ts_recv: int,
    depth: int,
) -> BookEvent:
    """
    Build a ``BookEvent`` from a CCXT order book.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    order_book : dict[str, Any]
        CCXT unified order book with ``bids``, ``asks`` and ``timestamp``.
    seq : int
        Per-stream sequence number.
    ts_recv : int
        Local receive time in nanoseconds.
    depth : int
        Number of levels per side to publish.

    Returns
    -------
    BookEvent
        The event.
    """
    return BookEvent(
        ts_recv=ts_recv,
        venue=venue,
        symbol=symbol,
        seq=seq,
        ts_exch=_optional_int(order_book.get("timestamp")),
        bids=_levels(order_book.get("bids", []), depth),
        asks=_levels(order_book.get("asks", []), depth),
    )


def trade_event_from_ccxt(
    venue: str,
    symbol: str,
    trade: dict[str, Any],
    *,
    seq: int,
    ts_recv: int,
) -> TradeEvent:
    """
    Build a ``TradeEvent`` from one CCXT unified trade.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    trade : dict[str, Any]
        CCXT unified trade with ``id``, ``timestamp``, ``side``, ``price``
        and ``amount``.
    seq : int
        Per-stream sequence number.
    ts_recv : int
        Local receive time in nanoseconds.

    Returns
    -------
    TradeEvent
        The event.
    """
    side_raw = trade.get("side")
    side = Side(side_raw) if side_raw in ("buy", "sell") else None
    trade_id = trade.get("id")
    return TradeEvent(
        ts_recv=ts_recv,
        venue=venue,
        symbol=symbol,
        seq=seq,
        ts_exch=_optional_int(trade.get("timestamp")),
        trade_id=None if trade_id is None else str(trade_id),
        side=side,
        price=float(trade["price"]),
        amount=float(trade["amount"]),
    )


def _float(value: Any) -> float:
    """
    Coerce a CCXT balance figure to ``float``, treating missing as zero.

    Parameters
    ----------
    value : Any
        Raw figure, possibly None.

    Returns
    -------
    float
        The figure.
    """
    return 0.0 if value is None else float(value)


def balance_event_from_ccxt(
    venue: str,
    balance: dict[str, Any],
    *,
    seq: int,
    ts_recv: int,
) -> BalanceEvent:
    """
    Build a ``BalanceEvent`` from a CCXT unified balance structure.

    Only asset entries are kept; the ``info``, ``free``, ``used``, ``total``
    and timestamp keys of the CCXT structure are dropped. Missing figures
    are recorded as zero.

    Parameters
    ----------
    venue : str
        CCXT short id.
    balance : dict[str, Any]
        CCXT unified balance.
    seq : int
        Per-stream sequence number.
    ts_recv : int
        Local receive time in nanoseconds.

    Returns
    -------
    BalanceEvent
        The event.
    """
    balances: dict[str, AssetBalance] = {}
    for asset, figures in balance.items():
        if asset in _BALANCE_META_KEYS or not isinstance(figures, dict):
            continue
        balances[asset] = AssetBalance(
            free=_float(figures.get("free")),
            used=_float(figures.get("used")),
            total=_float(figures.get("total")),
        )
    return BalanceEvent(
        ts_recv=ts_recv,
        venue=venue,
        seq=seq,
        ts_exch=_optional_int(balance.get("timestamp")),
        balances=balances,
    )


# CCXT order statuses mapped to the states the order manager publishes.
_ORDER_STATES: dict[str, OrderState] = {
    "open": OrderState.OPEN,
    "closed": OrderState.FILLED,
    "canceled": OrderState.CANCELLED,
    "cancelled": OrderState.CANCELLED,
    "expired": OrderState.EXPIRED,
    "rejected": OrderState.REJECTED,
}


def _decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    """
    Coerce a CCXT figure to ``Decimal`` without going through binary float.

    Parameters
    ----------
    value : Any
        Raw figure, possibly None.
    default : Decimal | None
        Returned when the figure is missing.

    Returns
    -------
    Decimal | None
        The figure.
    """
    if value is None:
        return default
    return Decimal(str(value))


def order_state_from_ccxt(status: Any, filled: Decimal) -> OrderState:
    """
    Map a CCXT order status to an ``OrderState``.

    An order CCXT still calls ``open`` is reported as partially filled once
    anything has executed against it, because that is the transition a
    strategy reacts to. An unknown status is reported as ``OPEN`` rather
    than dropped: losing an order update is worse than mislabelling one.

    Parameters
    ----------
    status : Any
        CCXT unified order status.
    filled : Decimal
        Cumulative filled size.

    Returns
    -------
    OrderState
        The state.
    """
    state = _ORDER_STATES.get(str(status), OrderState.OPEN)
    if state is OrderState.OPEN and filled > 0:
        return OrderState.PARTIALLY_FILLED
    return state


def fill_from_ccxt(order: dict[str, Any], previous_filled: Decimal) -> Fill | None:
    """
    Derive the fill an order update represents, if any.

    CCXT reports cumulative ``filled``, so the size of the fill that caused
    an update is the difference from the last update seen for that order.
    The price is the order's running average rather than the price of this
    fill alone, which CCXT only exposes when the venue attaches trades to
    the update; over a single fill the two are equal, and over several they
    differ by the spread the order was filled across.

    Parameters
    ----------
    order : dict[str, Any]
        CCXT unified order.
    previous_filled : Decimal
        Cumulative filled size at the previous update of this order.

    Returns
    -------
    Fill | None
        The fill, or None if nothing executed since the previous update.
    """
    filled = _decimal(order.get("filled"), Decimal(0)) or Decimal(0)
    amount = filled - previous_filled
    if amount <= 0:
        return None
    price = _decimal(order.get("average")) or _decimal(order.get("price"))
    if price is None:
        return None
    fee = order.get("fee") or {}
    liquidity_raw = order.get("takerOrMaker")
    return Fill(
        price=price,
        amount=amount,
        fee=_decimal(fee.get("cost")),
        fee_currency=fee.get("currency"),
        liquidity=(
            Liquidity(liquidity_raw) if liquidity_raw in ("maker", "taker") else None
        ),
        venue_trade_id=None,
    )


def order_event_from_ccxt(
    venue: str,
    order: dict[str, Any],
    *,
    strategy: str,
    ts_recv: int,
    previous_filled: Decimal = Decimal(0),
    tags: dict[str, str] | None = None,
) -> OrderEvent:
    """
    Build an ``OrderEvent`` from a CCXT order update.

    Parameters
    ----------
    venue : str
        CCXT short id.
    order : dict[str, Any]
        CCXT unified order.
    strategy : str
        Strategy key the order belongs to, as carried on the intent.
    ts_recv : int
        Local receive time in nanoseconds.
    previous_filled : Decimal
        Cumulative filled size at the previous update of this order, used to
        derive ``last_fill``.
    tags : dict[str, str] | None
        Labels to carry on the event.

    Returns
    -------
    OrderEvent
        The event.
    """
    filled = _decimal(order.get("filled"), Decimal(0)) or Decimal(0)
    side_raw = order.get("side")
    venue_order_id = order.get("id")
    return OrderEvent(
        ts_recv=ts_recv,
        intent_id=str(order.get("clientOrderId") or venue_order_id or ""),
        strategy=strategy,
        venue=venue,
        symbol=str(order.get("symbol") or ""),
        state=order_state_from_ccxt(order.get("status"), filled),
        side=Side(side_raw) if side_raw in ("buy", "sell") else None,
        ts_exch=_optional_int(order.get("timestamp")),
        venue_order_id=None if venue_order_id is None else str(venue_order_id),
        filled=filled,
        remaining=_decimal(order.get("remaining"), Decimal(0)) or Decimal(0),
        avg_price=_decimal(order.get("average")),
        last_fill=fill_from_ccxt(order, previous_filled),
        tags=dict(tags or {}),
    )
