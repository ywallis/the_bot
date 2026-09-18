"""Tests for the fee schedule model, resolution helpers and loaders."""

from decimal import Decimal
from typing import Any

import ccxt
import pytest
from ccxt.base.decimal_to_precision import DECIMAL_PLACES, TICK_SIZE

from apps.shared.src.config import VenueConfig
from apps.shared.src.events import FeeSource, Side
from apps.shared.src.fees import (
    TICK_SIZE_MODE,
    base_quote,
    fee_currency_for,
    fee_in_base,
    fee_schedule_from_client,
    schedule_for,
)

SYMBOL = "BTC/USDT"


def fake_client(
    markets: dict[str, Any] | None = None,
    fees: dict[str, Any] | None = None,
    has_fees: bool = True,
    precision_mode: int = DECIMAL_PLACES,
) -> Any:
    """Return a CCXT client stand-in with markets and a fee endpoint."""

    class FakeClient:
        id = "venue_a"
        name = "Venue A"
        has = {"fetchTradingFees": has_fees}

        def __init__(self) -> None:
            self.markets = dict(markets or {})
            self.fees = dict(fees or {})
            # CCXT names this attribute in camelCase. A stand-in that spells
            # it any other way makes the loader read every market as if it
            # reported decimal places, which is how this was missed once.
            self.precisionMode = precision_mode

        async def fetch_trading_fees(self) -> dict[str, Any]:
            return dict(self.fees)

    return FakeClient()


def market(**overrides: Any) -> dict[str, Any]:
    """Return a CCXT market structure in the shape the loader reads."""
    raw: dict[str, Any] = {
        "symbol": SYMBOL,
        "maker": 0.002,
        "taker": 0.005,
        "limits": {"cost": {"min": 1.0}},
        "precision": {"amount": 6},
    }
    raw.update(overrides)
    return raw


def entry_of(event: Any, symbol: str = SYMBOL) -> Any:
    """Return the schedule entry for a symbol."""
    found = schedule_for(event, symbol)
    assert found is not None
    return found


# Fee currency resolution ----------------------------------------------------


@pytest.mark.parametrize(
    ("policy", "side", "expected"),
    [
        ("quote", Side.BUY, "USDT"),
        ("quote", Side.SELL, "USDT"),
        ("base", Side.BUY, "BTC"),
        ("base", Side.SELL, "BTC"),
        ("received", Side.BUY, "BTC"),
        ("received", Side.SELL, "USDT"),
        ("BGB", Side.BUY, "BGB"),
        ("BGB", Side.SELL, "BGB"),
    ],
)
def test_fee_currency_for(policy, side, expected):
    """The policy decides which asset pays the fee on each side."""
    assert fee_currency_for(policy, side, "BTC", "USDT") == expected


def test_a_fee_in_base_erodes_the_asset_we_keep_stable():
    """Base fees must be compensated in sizing; quote fees are a USDT cost."""
    assert fee_in_base("received", Side.BUY, "BTC", "USDT")
    assert not fee_in_base("received", Side.SELL, "BTC", "USDT")
    assert not fee_in_base("quote", Side.BUY, "BTC", "USDT")
    assert fee_in_base("base", Side.SELL, "BTC", "USDT")


# Loader ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rates_come_from_markets_by_default():
    """Without account rates, the loaded market's defaults are used."""
    client = fake_client(markets={SYMBOL: market()}, has_fees=False)
    event = await fee_schedule_from_client(
        client, VenueConfig(id="venue_a", name="Venue A"), {SYMBOL}, ts_recv=1
    )
    assert event.source is FeeSource.MARKETS
    assert event.fee_currency == "quote"
    entry = entry_of(event)
    assert entry.maker == Decimal("0.002")
    assert entry.taker == Decimal("0.005")
    assert entry.min_cost == 1.0
    assert entry.amount_precision == 6


@pytest.mark.asyncio
async def test_account_tier_overrides_market_defaults():
    """Fetched trading fees reflect the account's current tier."""
    client = fake_client(
        markets={SYMBOL: market()},
        fees={SYMBOL: {"maker": 0.001, "taker": 0.004}},
    )
    event = await fee_schedule_from_client(
        client, VenueConfig(id="venue_a", name="Venue A"), {SYMBOL}, ts_recv=1
    )
    assert event.source is FeeSource.TRADING_FEES
    entry = entry_of(event)
    assert entry.maker == Decimal("0.001")
    assert entry.taker == Decimal("0.004")


@pytest.mark.asyncio
async def test_config_overrides_win_and_skip_the_fetch():
    """Static configured rates beat everything and mark the schedule."""
    client = fake_client(
        markets={SYMBOL: market()},
        fees={SYMBOL: {"maker": 0.001, "taker": 0.004}},
    )
    venue = VenueConfig(id="venue_a", name="Venue A", maker_fee=0.0, taker_fee=0.002)
    event = await fee_schedule_from_client(client, venue, {SYMBOL}, ts_recv=1)
    assert event.source is FeeSource.CONFIG
    entry = entry_of(event)
    assert entry.maker == Decimal("0")
    assert entry.taker == Decimal("0.002")


@pytest.mark.asyncio
async def test_a_failed_fee_fetch_falls_back_to_markets():
    """A venue that errors on the fee endpoint still yields a schedule."""
    client = fake_client(markets={SYMBOL: market()})

    async def broken() -> dict[str, Any]:
        raise RuntimeError("endpoint gone")

    client.fetch_trading_fees = broken  # type: ignore[method-assign]
    event = await fee_schedule_from_client(
        client, VenueConfig(id="venue_a", name="Venue A"), {SYMBOL}, ts_recv=1
    )
    assert event.source is FeeSource.MARKETS
    assert entry_of(event).taker == Decimal("0.005")


@pytest.mark.asyncio
async def test_a_trading_fees_wrapper_is_unwrapped():
    """Some venues nest the account rates under a ``trading`` key."""
    client = fake_client(
        markets={SYMBOL: market()},
        fees={"trading": {SYMBOL: {"maker": 0.001, "taker": 0.004}}},
    )
    event = await fee_schedule_from_client(
        client, VenueConfig(id="venue_a", name="Venue A"), {SYMBOL}, ts_recv=1
    )
    assert event.source is FeeSource.TRADING_FEES
    assert entry_of(event).maker == Decimal("0.001")


@pytest.mark.asyncio
async def test_an_unlisted_symbol_is_left_out():
    """A symbol the venue does not list cannot carry a schedule entry."""
    client = fake_client(markets={SYMBOL: market()})
    event = await fee_schedule_from_client(
        client,
        VenueConfig(id="venue_a", name="Venue A"),
        {SYMBOL, "ETH/USDT"},
        ts_recv=1,
    )
    assert [s.symbol for s in event.symbols] == [SYMBOL]


@pytest.mark.asyncio
async def test_tick_size_precision_is_reported_as_decimals():
    """A step size of 0.0001 means four decimal places."""
    client = fake_client(
        markets={SYMBOL: market(precision={"amount": 0.0001})},
        precision_mode=TICK_SIZE,
    )
    event = await fee_schedule_from_client(
        client, VenueConfig(id="venue_a", name="Venue A"), {SYMBOL}, ts_recv=1
    )
    assert entry_of(event).amount_precision == 4


def test_tick_size_mode_matches_ccxt():
    """
    The tick size mode is CCXT's, not a number typed from memory.

    ``DECIMAL_PLACES`` and ``TICK_SIZE`` are plain ints one keystroke apart.
    Reading a step size as decimal places truncates 0.0001 to 0, which
    quantizes every sub-unit hedge to nothing, so this is pinned.
    """
    assert TICK_SIZE_MODE == TICK_SIZE
    assert TICK_SIZE_MODE != DECIMAL_PLACES
    assert ccxt.Exchange().precisionMode == TICK_SIZE


@pytest.mark.asyncio
async def test_a_fractional_precision_is_a_step_whatever_the_mode_says():
    """A venue contradicting its own mode is read as a step size, loudly."""
    client = fake_client(
        markets={SYMBOL: market(precision={"amount": 0.001})},
        precision_mode=DECIMAL_PLACES,
    )
    event = await fee_schedule_from_client(
        client, VenueConfig(id="venue_a", name="Venue A"), {SYMBOL}, ts_recv=1
    )
    assert entry_of(event).amount_precision == 3


@pytest.mark.asyncio
async def test_unreported_terms_are_none():
    """Missing market fields decode as None rather than zero."""
    bare = market(
        maker=None,
        taker=None,
        limits={"cost": {"min": None}},
        precision={},
    )
    client = fake_client(markets={SYMBOL: bare}, has_fees=False)
    event = await fee_schedule_from_client(
        client, VenueConfig(id="venue_a", name="Venue A"), {SYMBOL}, ts_recv=1
    )
    entry = entry_of(event)
    assert entry.maker is None
    assert entry.taker is None
    assert entry.min_cost is None
    assert entry.amount_precision is None


@pytest.mark.asyncio
async def test_schedule_for_misses_unknown_symbols():
    """A symbol outside the schedule returns None."""
    client = fake_client(markets={SYMBOL: market()}, has_fees=False)
    event = await fee_schedule_from_client(
        client, VenueConfig(id="venue_a", name="Venue A"), {SYMBOL}, ts_recv=1
    )
    assert schedule_for(event, "ETH/USDT") is None


# Symbol parsing -------------------------------------------------------------


def test_base_quote_splits_the_symbol():
    """A CCXT symbol splits at the slash."""
    assert base_quote("BTC/USDT") == ("BTC", "USDT")


def test_base_quote_rejects_malformed_symbols():
    """Anything without both halves is an error, not a silent split."""
    for bad in ("BTCUSDT", "/USDT", "BTC/"):
        with pytest.raises(ValueError, match="BASE/QUOTE"):
            base_quote(bad)
