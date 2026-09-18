"""Tests for the fee schedule feed handler."""

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import fees
from apps.shared.src import events
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
    VenueConfig,
)
from apps.shared.src.events import FeeSource, FeeScheduleEvent, SymbolFees
from apps.shared.src.streams import StreamPublisher

VENUE = VenueConfig(id="venue_a", name="Venue A", fee_currency="received")
OTHER_VENUE = VenueConfig(id="venue_b", name="Venue B")
SYMBOL = "BTC/USDT"


def fake_client(
    markets: dict[str, Any] | None = None,
    fees: dict[str, Any] | None = None,
) -> Any:
    """Return a CCXT client stand-in with markets and a fee endpoint."""

    class FakeClient:
        id = "venue_a"
        name = "Venue A"
        has = {"fetchTradingFees": True}

        def __init__(self) -> None:
            self.markets = markets or {}

        async def load_markets(self) -> None:
            return None

        async def fetch_trading_fees(self) -> dict[str, Any]:
            return dict(fees or {})

    return FakeClient()


def schedule_event() -> FeeScheduleEvent:
    """Return a schedule event as the loader would build it."""
    return FeeScheduleEvent(
        ts_recv=1,
        venue="venue_a",
        fee_currency="received",
        symbols=[
            SymbolFees(
                symbol=SYMBOL,
                maker=None,
                taker=None,
                min_cost=1.0,
                amount_precision=6,
            )
        ],
        source=FeeSource.MARKETS,
    )


def stop_after(passes: int):
    """Return a sleep stub that cancels the loop after n passes."""

    async def stop(_seconds: float) -> None:
        nonlocal seen
        seen += 1
        if seen >= passes:
            raise asyncio.CancelledError

    seen = 0
    return stop


# Publication ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_fees_writes_event_and_snapshot_key():
    """The stream gets the event and the key holds the same event decodable."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    event = schedule_event()

    await fees.publish_fees(redis, publisher, event)

    entries = await redis.xrange("acct:fees:venue_a")
    assert len(entries) == 1
    assert events.from_stream_fields(entries[0][1]) == event

    stored = events.decode(await redis.get("fees-venue_a"))
    assert stored == event


# The watch loop -------------------------------------------------------------


@pytest.mark.asyncio
async def test_watch_fees_publishes_the_schedule(monkeypatch):
    """The loop loads markets, builds the schedule and publishes it."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = fake_client(
        markets={SYMBOL: {"symbol": SYMBOL, "maker": 0.002, "taker": 0.005}},
        fees={SYMBOL: {"maker": 0.001, "taker": 0.004}},
    )

    monkeypatch.setattr(fees.asyncio, "sleep", stop_after(1))
    with pytest.raises(asyncio.CancelledError):
        await fees.watch_fees(client, VENUE, {SYMBOL}, 3600, redis, publisher)

    entries = await redis.xrange("acct:fees:venue_a")
    event = events.from_stream_fields(entries[0][1])
    assert isinstance(event, FeeScheduleEvent)
    assert event.source is FeeSource.TRADING_FEES
    assert event.symbols[0].taker == Decimal("0.004")


@pytest.mark.asyncio
async def test_watch_fees_survives_a_failed_pass(monkeypatch):
    """A pass whose markets cannot even load must not take the feed down."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = fake_client()

    async def broken() -> None:
        raise RuntimeError("endpoint gone")

    client.load_markets = broken  # type: ignore[method-assign]

    monkeypatch.setattr(fees.asyncio, "sleep", stop_after(1))
    with pytest.raises(asyncio.CancelledError):
        await fees.watch_fees(client, VENUE, {SYMBOL}, 3600, redis, publisher)

    assert await redis.xlen("acct:fees:venue_a") == 0


@pytest.mark.asyncio
async def test_watch_fees_retries_on_the_next_interval(monkeypatch):
    """A pass that fails is followed by a pass that succeeds."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = fake_client(
        markets={SYMBOL: {"symbol": SYMBOL, "maker": 0.002, "taker": 0.005}}
    )
    calls = 0

    async def flaky() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("first pass fails")

    client.load_markets = flaky  # type: ignore[method-assign]

    monkeypatch.setattr(fees.asyncio, "sleep", stop_after(2))
    with pytest.raises(asyncio.CancelledError):
        await fees.watch_fees(client, VENUE, {SYMBOL}, 3600, redis, publisher)

    assert calls == 2
    assert await redis.xlen("acct:fees:venue_a") == 1


# Task building --------------------------------------------------------------


def config_for(venues: tuple[VenueConfig, ...], production: bool) -> AppConfig:
    """Return a config with one strategy trading one venue."""
    return AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=venues,
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=production,
                subscriptions=(Subscription(venue="venue_a", symbol=SYMBOL),),
                params={},
            ),
        ),
    )


def test_a_venue_without_a_client_gets_no_fee_watcher():
    """A venue declared but not traded on has no schedule to fetch."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    tasks = fees.build_tasks(
        config_for((VENUE,), production=True), {}, redis, publisher, True
    )
    assert tasks == []


@pytest.mark.asyncio
async def test_build_tasks_serves_only_production_symbols(monkeypatch):
    """The schedule covers what the running strategies trade, no more."""
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(VENUE, OTHER_VENUE),
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=True,
                subscriptions=(Subscription(venue="venue_a", symbol=SYMBOL),),
                params={},
            ),
            StrategyConfig(
                identifier="tst",
                type="single_edge_liquidity",
                production=False,
                subscriptions=(Subscription(venue="venue_b", symbol="ETH/USDT"),),
                params={},
            ),
        ),
    )
    client = fake_client(
        markets={
            SYMBOL: {"symbol": SYMBOL, "maker": 0.002, "taker": 0.005},
            "ETH/USDT": {"symbol": "ETH/USDT", "maker": 0.002, "taker": 0.005},
        }
    )
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    tasks = fees.build_tasks(config, {"venue_a": client}, redis, publisher, True)
    assert len(tasks) == 1

    monkeypatch.setattr(fees.asyncio, "sleep", stop_after(1))
    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(*tasks)

    event = events.decode(await redis.get("fees-venue_a"))
    assert isinstance(event, FeeScheduleEvent)
    assert [s.symbol for s in event.symbols] == [SYMBOL]
