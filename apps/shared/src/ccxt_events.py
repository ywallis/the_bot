"""Converters from CCXT unified structures to bus events.

The watchers call these right after a CCXT ``watch_*`` call returns, passing
the receive timestamp they took at that moment. Nothing here touches Redis.
"""

from typing import Any

from apps.shared.src.events import (
    AssetBalance,
    BalanceEvent,
    BookEvent,
    Level,
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
