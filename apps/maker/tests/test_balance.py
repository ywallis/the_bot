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
