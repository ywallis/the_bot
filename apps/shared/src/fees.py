"""Fee schedule model, resolution helpers and CCXT loaders.

The schedule answers, per venue and symbol, what a trade costs: the maker
and taker rates, which currency the fee is charged in, and the smallest
notional the venue accepts. Rates are fetched rather than fixed because fee
tiers follow the account's rolling volume and balance; which currency the
fee is charged in is a policy declared in configuration and verified against
the fee currencies venues actually report on fills.
"""

import logging
from decimal import Decimal
from typing import Any

from ccxt.base.decimal_to_precision import TICK_SIZE

from apps.shared.src.config import FEE_POLICY_KEYWORDS, VenueConfig
from apps.shared.src.events import FeeScheduleEvent, FeeSource, Side, SymbolFees

logger = logging.getLogger(__name__)

# CCXT ``precisionMode`` value meaning ``precision.amount`` is a step size
# rather than a number of decimal places. Taken from CCXT rather than
# written out: the two modes are plain ints and easy to transpose.
TICK_SIZE_MODE = TICK_SIZE


def fee_currency_for(policy: str, side: Side, base: str, quote: str) -> str:
    """
    Resolve which currency a fee is charged in for one side of a symbol.

    Parameters
    ----------
    policy : str
        The venue's declared policy: one of ``FEE_POLICY_KEYWORDS`` or an
        explicit asset code.
    side : Side
        Which side of the pair is being traded.
    base : str
        The symbol's base asset.
    quote : str
        The symbol's quote asset.

    Returns
    -------
    str
        The asset code the fee is charged in.
    """
    match policy:
        case "quote":
            return quote
        case "base":
            return base
        case "received":
            return base if side is Side.BUY else quote
        case _:
            return policy


def fee_in_base(policy: str, side: Side, base: str, quote: str) -> bool:
    """
    Return whether a fee on this side of a symbol is charged in base.

    Fees in base erode the asset whose balance the system keeps stable and
    must be compensated in hedge sizing; fees in quote come out of the
    USDT leg and are a cost of doing business instead.

    Parameters
    ----------
    policy : str
        The venue's declared policy, see ``fee_currency_for``.
    side : Side
        Which side of the pair is being traded.
    base : str
        The symbol's base asset.
    quote : str
        The symbol's quote asset.

    Returns
    -------
    bool
        True if the fee is charged in the base asset.
    """
    return fee_currency_for(policy, side, base, quote) == base


def _rate(value: Any) -> Decimal | None:
    """
    Coerce a CCXT fee rate to ``Decimal`` without going through float.

    Parameters
    ----------
    value : Any
        Raw rate, possibly None.

    Returns
    -------
    Decimal | None
        The rate.
    """
    if value is None:
        return None
    return Decimal(str(value))


def _min_cost(market: dict[str, Any]) -> float | None:
    """
    Return the smallest order notional a market accepts.

    Parameters
    ----------
    market : dict[str, Any]
        CCXT unified market.

    Returns
    -------
    float | None
        The minimum notional in quote currency, None if unreported.
    """
    raw = ((market.get("limits") or {}).get("cost") or {}).get("min")
    return None if raw is None else float(raw)


def _amount_precision(client: Any, market: dict[str, Any]) -> int | None:
    """
    Return how many decimal places an order amount may carry.

    CCXT reports precision either as decimal places or as a step size,
    depending on the client's ``precisionMode``; a step size of 0.0001 is
    reported here as 4. Quantizing to this many places is a multiple of the
    step in both modes.

    A fractional value is read as a step size whatever the mode says. The
    two readings differ by orders of magnitude - truncating 0.0001 to an
    integer gives 0, which quantizes every sub-unit hedge to nothing - so a
    venue that contradicts its own mode is worth surviving.

    Parameters
    ----------
    client : Any
        The CCXT client, whose ``precisionMode`` decides how to read the
        market's precision.
    market : dict[str, Any]
        CCXT unified market.

    Returns
    -------
    int | None
        Decimal places, None if unreported.
    """
    raw = (market.get("precision") or {}).get("amount")
    if raw is None:
        return None
    value = Decimal(str(raw))
    if getattr(client, "precisionMode", None) != TICK_SIZE_MODE:
        if value == value.to_integral_value():
            return max(0, int(value))
        logger.warning(
            f"{getattr(client, 'id', 'venue')} reports decimal places but "
            f"{value} is a step size; reading it as one"
        )
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int):
        return None
    return max(0, -exponent)


def _first_rate(*candidates: Any) -> Decimal | None:
    """
    Return the first candidate that is set, as a ``Decimal``.

    Parameters
    ----------
    *candidates : Any
        Rate candidates in priority order, each possibly None.

    Returns
    -------
    Decimal | None
        The first set candidate, None if all are.
    """
    for candidate in candidates:
        rate = _rate(candidate)
        if rate is not None:
            return rate
    return None


async def fee_schedule_from_client(
    client: Any,
    venue: VenueConfig,
    symbols: set[str],
    ts_recv: int,
) -> FeeScheduleEvent:
    """
    Build the fee schedule event for one venue.

    Rates resolve per symbol from the first of: the venue's configured
    static override, the account's fetched trading fees (its current tier),
    and the default rates of the loaded market. Entry metadata (minimum
    notional, amount precision) always comes from the market. The account's
    rates are fetched only where the client's ``has`` map says the venue
    supports it; a failed fetch falls back to market defaults rather than
    taking the feed down.

    Parameters
    ----------
    client : Any
        A CCXT exchange client. Typed loosely because optional endpoints
        are looked up on its ``has`` map.
    venue : VenueConfig
        The venue, whose configured fee policy and static overrides apply.
    symbols : set[str]
        The symbols the schedule covers.
    ts_recv : int
        Local receive time in nanoseconds.

    Returns
    -------
    FeeScheduleEvent
        The schedule, covering the requested symbols the venue lists.
    """
    markets = client.markets or {}
    has = getattr(client, "has", {}) or {}
    fetched: dict[str, dict[str, Any]] = {}
    if has.get("fetchTradingFees"):
        try:
            fees = await client.fetch_trading_fees()
            if isinstance(fees.get("trading"), dict):
                fees = fees["trading"]
            fetched = {s: f for s, f in fees.items() if isinstance(f, dict)}
        except Exception as error:  # noqa: BLE001, rates fall back to markets
            logger.warning(f"Could not fetch trading fees on {client.id}: {error}")
    else:
        logger.debug(f"{client.id} cannot fetch trading fees, using market defaults")

    entries: list[SymbolFees] = []
    for symbol in sorted(symbols):
        market = markets.get(symbol)
        if market is None:
            logger.warning(
                f"No market {symbol} on {client.id}, leaving it out of the schedule"
            )
            continue
        account = fetched.get(symbol, {})
        entries.append(
            SymbolFees(
                symbol=symbol,
                maker=_first_rate(
                    venue.maker_fee, account.get("maker"), market.get("maker")
                ),
                taker=_first_rate(
                    venue.taker_fee, account.get("taker"), market.get("taker")
                ),
                min_cost=_min_cost(market),
                amount_precision=_amount_precision(client, market),
            )
        )

    if venue.maker_fee is not None and venue.taker_fee is not None:
        source = FeeSource.CONFIG
    elif fetched:
        source = FeeSource.TRADING_FEES
    else:
        source = FeeSource.MARKETS
    return FeeScheduleEvent(
        ts_recv=ts_recv,
        venue=venue.id,
        fee_currency=venue.fee_currency,
        symbols=entries,
        source=source,
    )


def fees_snapshot_key(venue: str) -> str:
    """
    Return the snapshot key for a venue's fee schedule.

    The key holds the encoded ``FeeScheduleEvent``, so consumers that poll
    rather than follow the stream can decode it with the shared codec.

    Parameters
    ----------
    venue : str
        CCXT short id.

    Returns
    -------
    str
        Redis key.
    """
    return f"fees-{venue}"


def schedule_for(event: FeeScheduleEvent, symbol: str) -> SymbolFees | None:
    """
    Return the schedule entry for a symbol.

    Parameters
    ----------
    event : FeeScheduleEvent
        The fee schedule of a venue.
    symbol : str
        CCXT symbol.

    Returns
    -------
    SymbolFees | None
        The entry, None if the venue does not list the symbol.
    """
    for entry in event.symbols:
        if entry.symbol == symbol:
            return entry
    return None


def base_quote(symbol: str) -> tuple[str, str]:
    """
    Split a CCXT symbol into its base and quote assets.

    Parameters
    ----------
    symbol : str
        CCXT symbol, e.g. ``BASE/QUOTE``.

    Returns
    -------
    tuple[str, str]
        Base and quote asset codes.

    Raises
    ------
    ValueError
        If the symbol is not of the form ``BASE/QUOTE``.
    """
    base, slash, quote = symbol.partition("/")
    if not slash or not base or not quote:
        raise ValueError(f"Symbol {symbol!r} is not of the form BASE/QUOTE")
    return base, quote


__all__ = [
    "FEE_POLICY_KEYWORDS",
    "TICK_SIZE_MODE",
    "base_quote",
    "fee_currency_for",
    "fee_in_base",
    "fee_schedule_from_client",
    "fees_snapshot_key",
    "schedule_for",
]
