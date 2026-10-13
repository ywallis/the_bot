"""Tests for the position and funding feed handler."""

import asyncio
from decimal import Decimal

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import derivatives_feed
from apps.maker.src.derivatives_feed import (
    PositionFeed,
    feed_targets,
    publish_funding_once,
    wakes_position,
)
from apps.shared.src import events
from apps.shared.src.config import parse_app_config
from apps.shared.src.events import (
    FundingEvent,
    OrderEvent,
    OrderState,
    PositionEvent,
    PositionSide,
    Side,
)
from apps.shared.src.streams import StreamPublisher

PERP = "BASE/USDT:USDT"
OTHER = "ALT/USDT:USDT"


class FakeDerivativesClient:
    """A CCXT stand-in answering positions and funding from canned data."""

    def __init__(self, positions=None, funding=None):
        """Store the canned answers."""
        self.markets = {
            PERP: {"contractSize": 10},
            OTHER: {"contractSize": 1},
        }
        self.positions = positions or []
        self.funding = funding or {}
        self.position_calls = 0

    async def fetch_positions(self, symbols):
        """Return the canned positions."""
        self.position_calls += 1
        return list(self.positions)

    async def fetch_funding_rate(self, symbol):
        """Return the canned funding structure, or raise."""
        answer = self.funding[symbol]
        if isinstance(answer, Exception):
            raise answer
        return answer


async def read_events(redis, stream):
    """Decode every entry of a stream."""
    return [events.from_stream_fields(f) for _, f in await redis.xrange(stream)]


@pytest.mark.asyncio
async def test_a_short_is_published_in_base_and_a_missing_symbol_as_flat():
    """Contracts become signed base, and silence about a symbol means flat."""
    redis = fakeredis.FakeRedis()
    client = FakeDerivativesClient(
        positions=[
            {"symbol": PERP, "contracts": 3, "side": "short", "entryPrice": 2.5},
        ]
    )
    feed = PositionFeed(
        client, "venueperp", {PERP, OTHER}, redis, StreamPublisher(100), 5
    )
    published = await feed.fetch_once()
    assert len(published) == 2
    stored = await read_events(redis, "acct:position:venueperp")
    by_symbol = {e.symbol: e for e in stored if isinstance(e, PositionEvent)}
    assert by_symbol[PERP].side is PositionSide.SHORT
    assert by_symbol[PERP].base_amount == Decimal(-30)
    assert by_symbol[PERP].contract_size == Decimal(10)
    assert by_symbol[OTHER].side is PositionSide.FLAT
    assert by_symbol[OTHER].base_amount == 0
    assert [e.seq for e in stored] == [1, 2]


@pytest.mark.asyncio
async def test_an_unchanged_position_waits_for_the_heartbeat():
    """A second identical fetch publishes nothing until the heartbeat is due."""
    redis = fakeredis.FakeRedis()
    client = FakeDerivativesClient(
        positions=[{"symbol": PERP, "contracts": 1, "side": "long"}]
    )
    feed = PositionFeed(client, "venueperp", {PERP}, redis, StreamPublisher(100), 5)
    assert len(await feed.fetch_once()) == 1
    assert await feed.fetch_once() == []
    client.positions = [{"symbol": PERP, "contracts": 2, "side": "long"}]
    assert len(await feed.fetch_once()) == 1
    feed.heartbeat_s = 0
    assert len(await feed.fetch_once()) == 1


@pytest.mark.asyncio
async def test_an_empty_side_does_not_hide_the_real_position():
    """A venue listing a zero-size entry next to the real one is read right."""
    redis = fakeredis.FakeRedis()
    client = FakeDerivativesClient(
        positions=[
            {"symbol": PERP, "contracts": 4, "side": "short"},
            {"symbol": PERP, "contracts": 0, "side": "long"},
        ]
    )
    feed = PositionFeed(client, "venueperp", {PERP}, redis, StreamPublisher(100), 5)
    (event,) = await feed.fetch_once()
    assert event.base_amount == Decimal(-40)


@pytest.mark.asyncio
async def test_a_fill_wakes_the_position_feed():
    """A fill reported on the venue makes the feed fetch before its poll."""
    redis = fakeredis.FakeRedis()
    client = FakeDerivativesClient()
    feed = PositionFeed(client, "venueperp", {PERP}, redis, StreamPublisher(100), 60)
    runner = asyncio.create_task(feed.run())
    watcher = asyncio.create_task(
        derivatives_feed.watch_fills(redis, {"venueperp": feed}, 50, "0-0")
    )
    try:
        await asyncio.sleep(0.05)
        assert client.position_calls == 1
        await StreamPublisher(100).publish(
            redis,
            OrderEvent(
                ts_recv=1,
                intent_id="t-1_inv_q",
                strategy="matching",
                venue="venueperp",
                symbol=PERP,
                state=OrderState.FILLED,
                side=Side.SELL,
                filled=Decimal(1),
            ),
        )
        for _ in range(40):
            if client.position_calls >= 2:
                break
            await asyncio.sleep(0.05)
        assert client.position_calls == 2
    finally:
        runner.cancel()
        watcher.cancel()


def test_only_reports_of_filled_size_wake_the_feed():
    """An accepted or unfilled cancelled order cannot have moved a position."""
    base = dict(
        ts_recv=1,
        intent_id="i",
        strategy="s",
        venue="v",
        symbol=PERP,
        side=Side.BUY,
    )
    assert not wakes_position(OrderEvent(state=OrderState.OPEN, **base))
    assert not wakes_position(OrderEvent(state=OrderState.CANCELLED, **base))
    assert wakes_position(
        OrderEvent(state=OrderState.CANCELLED, filled=Decimal(1), **base)
    )


@pytest.mark.asyncio
async def test_funding_is_published_and_a_failure_skips_only_its_symbol():
    """One symbol's error does not stop the others' rates going out."""
    redis = fakeredis.FakeRedis()
    client = FakeDerivativesClient(
        funding={
            PERP: {
                "fundingRate": 0.0001,
                "interval": "8h",
                "markPrice": 2.5,
                "nextFundingTimestamp": 1_800_000_000_000,
            },
            OTHER: RuntimeError("down"),
        }
    )
    published = await publish_funding_once(
        client, "venueperp", [OTHER, PERP], redis, StreamPublisher(100)
    )
    assert [e.symbol for e in published] == [PERP]
    (event,) = await read_events(redis, f"md:funding:venueperp:{PERP}")
    assert isinstance(event, FundingEvent)
    assert event.funding_rate == 0.0001
    assert event.interval_s == 28_800
    assert event.next_funding_ts == 1_800_000_000_000


def test_feed_targets_follow_the_subscriptions():
    """Only symbols whose subscription lists the feed are covered."""
    config = parse_app_config(
        {
            "venues": [
                {"id": "venuea", "name": "A"},
                {"id": "venueperp", "name": "P", "market_type": "swap"},
            ],
            "strategies": [
                {
                    "identifier": "inv",
                    "type": "inventory",
                    "production": True,
                    "subscriptions": [
                        {"venue": "venuea", "symbol": "BASE/USDT"},
                        {
                            "venue": "venueperp",
                            "symbol": PERP,
                            "feeds": ["book", "position", "funding"],
                        },
                        {"venue": "venueperp", "symbol": OTHER, "feeds": ["position"]},
                    ],
                }
            ],
        }
    )
    assert feed_targets(config, "position", True) == {"venueperp": {PERP, OTHER}}
    assert feed_targets(config, "funding", True) == {"venueperp": {PERP}}
    assert feed_targets(config, "funding", False) == {}


@pytest.mark.asyncio
async def test_main_idles_when_nothing_subscribes():
    """An exit would read as a crash to the orchestrator, so main stays up."""
    config = parse_app_config(
        {
            "venues": [{"id": "venuea", "name": "A"}],
            "strategies": [
                {
                    "identifier": "mk",
                    "type": "maker",
                    "production": True,
                    "subscriptions": [{"venue": "venuea", "symbol": "BASE/USDT"}],
                }
            ],
        }
    )
    task = asyncio.create_task(derivatives_feed.main(config, {}))
    await asyncio.sleep(0.05)
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
