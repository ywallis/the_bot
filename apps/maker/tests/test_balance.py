"""Tests for the balance feed handler."""

import json

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import balance
from apps.shared.src import events
from apps.shared.src.events import BalanceEvent
from apps.shared.src.streams import StreamPublisher

TS = 1_757_160_000_123_456_789


@pytest.mark.asyncio
async def test_publish_balance_writes_event_and_legacy_key():
    """The snapshot gets a ms timestamp and the stream gets a BalanceEvent."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    ccxt_balance = {
        "info": {},
        "USDT": {"free": 10.0, "used": 0.0, "total": 10.0},
        "free": {"USDT": 10.0},
    }
    await balance.publish_balance(redis, publisher, "mexc", ccxt_balance, TS)

    legacy = json.loads(await redis.get("balance-mexc"))
    assert legacy["timestamp"] == TS // 1_000_000
    assert legacy["USDT"]["free"] == 10.0

    entries = await redis.xrange("acct:balance:mexc")
    assert len(entries) == 1
    event = events.from_stream_fields(entries[0][1])
    assert isinstance(event, BalanceEvent)
    assert event.seq == 1
    assert event.ts_recv == TS
    assert event.ts_exch == TS // 1_000_000
    assert set(event.balances) == {"USDT"}


def test_merge_balance_keeps_currencies_a_delta_leaves_out():
    """A delta carrying one currency is overlaid onto the last full snapshot."""
    full = {
        "info": {"n": 1},
        "timestamp": 1,
        "USDT": {"free": 50.0, "used": 0.0, "total": 50.0},
        "BASE": {"free": 100.0, "used": 20.0, "total": 120.0},
        "free": {"USDT": 50.0, "BASE": 100.0},
        "used": {"USDT": 0.0, "BASE": 20.0},
        "total": {"USDT": 50.0, "BASE": 120.0},
    }
    delta = {
        "info": {"n": 2},
        "timestamp": 2,
        "BASE": {"free": 120.0, "used": 0.0, "total": 120.0},
        "free": {"BASE": 120.0},
        "used": {"BASE": 0.0},
        "total": {"BASE": 120.0},
    }
    merged = balance.merge_balance(full, delta)
    assert merged["USDT"] == {"free": 50.0, "used": 0.0, "total": 50.0}
    assert merged["BASE"] == {"free": 120.0, "used": 0.0, "total": 120.0}
    assert merged["free"] == {"USDT": 50.0, "BASE": 120.0}
    assert merged["used"] == {"USDT": 0.0, "BASE": 0.0}
    assert merged["timestamp"] == 2 and merged["info"] == {"n": 2}
    # The inputs are left alone.
    assert full["BASE"]["free"] == 100.0


def test_merge_balance_without_a_previous_snapshot_is_the_update():
    """Before the first fetch there is nothing to overlay onto."""
    update = {"USDT": {"free": 1.0, "used": None, "total": 1.0}, "timestamp": 3}
    merged = balance.merge_balance(None, update)
    assert merged["USDT"] == update["USDT"]
    assert merged["free"] == {"USDT": 1.0}
    assert merged["used"] == {}
    assert merged["timestamp"] == 3


@pytest.mark.asyncio
async def test_watch_balance_publishes_complete_snapshots():
    """What reaches the stream after a delta still names every currency."""

    class Client:
        id = "venue_a"
        name = "Venue A"

        def __init__(self):
            self.updates = [
                {
                    "USDT": {"free": 50.0, "used": 0.0, "total": 50.0},
                    "BASE": {"free": 100.0, "used": 0.0, "total": 100.0},
                },
                {"BASE": {"free": 80.0, "used": 20.0, "total": 100.0}},
            ]

        async def fetch_balance(self):
            return self.updates.pop(0)

        async def watch_balance(self):
            if not self.updates:
                raise RuntimeError("done")
            return self.updates.pop(0)

    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    cache: balance.BalanceCache = {}
    client = Client()
    await balance.fetch_balance(client, redis, publisher, cache)  # ty: ignore[invalid-argument-type]
    with pytest.raises(RuntimeError):
        await balance.watch_balance(client, redis, publisher, cache)  # ty: ignore[invalid-argument-type]

    entries = await redis.xrange("acct:balance:venue_a")
    assert len(entries) == 2
    second = events.from_stream_fields(entries[1][1])
    assert isinstance(second, BalanceEvent)
    assert second.balances["USDT"].free == 50.0
    assert second.balances["BASE"].free == 80.0
    assert second.balances["BASE"].used == 20.0
    legacy = json.loads(await redis.get("balance-venue_a"))
    assert set(legacy["free"]) == {"USDT", "BASE"}
