"""Tests for the stream publisher and stream enumeration."""

from pathlib import Path

import pytest
from fakeredis import aioredis as fakeredis

from apps.shared.src import events
from apps.shared.src.config import parse_app_config
from apps.shared.src.events import BookEvent, TradeEvent
from apps.shared.src.streams import StreamPublisher, configured_streams, stream_path

TS = 1_757_160_000_000_000_000
VENUES = [{"id": "gate", "name": "Gate.io"}, {"id": "mexc", "name": "Mexc"}]


def book(seq: int, venue: str = "gate") -> BookEvent:
    """Return a small book event."""
    return BookEvent(
        ts_recv=TS + seq,
        venue=venue,
        symbol="BTC/USDT",
        seq=seq,
        ts_exch=None,
        bids=[(1.0, 1.0)],
        asks=[(1.1, 1.0)],
    )


def test_next_seq_is_per_stream():
    """Each stream has its own counter starting at 1."""
    publisher = StreamPublisher(maxlen=10)
    assert publisher.next_seq("a") == 1
    assert publisher.next_seq("a") == 2
    assert publisher.next_seq("b") == 1


@pytest.mark.asyncio
async def test_publish_routes_and_decodes():
    """An event lands on its stream and decodes back to the same struct."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    event = book(1)
    stream = await publisher.publish(redis, event)
    assert stream == "md:book:gate:BTC/USDT"
    entries = await redis.xrange(stream)
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields["type"] == "book"
    assert events.from_stream_fields(fields) == event


@pytest.mark.asyncio
async def test_publish_with_prefix():
    """The namespace prefix is applied to the stream name."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100, prefix="bt:run1")
    stream = await publisher.publish(redis, book(1))
    assert stream == "bt:run1:md:book:gate:BTC/USDT"
    assert await redis.xlen(stream) == 1


@pytest.mark.asyncio
async def test_xadd_on_pipeline():
    """Queued XADDs on a pipeline are sent on execute."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    async with redis.pipeline(transaction=False) as pipe:
        publisher.xadd(pipe, book(1))
        publisher.xadd(pipe, book(2))
        assert await redis.xlen("md:book:gate:BTC/USDT") == 0
        await pipe.execute()
    assert await redis.xlen("md:book:gate:BTC/USDT") == 2


@pytest.mark.asyncio
async def test_maxlen_bounds_the_stream():
    """Streams are trimmed to roughly the configured maximum length."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=5)
    for seq in range(1, 51):
        await publisher.publish(redis, book(seq))
    # Approximate trimming may keep a little more than maxlen, never much.
    assert await redis.xlen("md:book:gate:BTC/USDT") <= 50
    assert await redis.xlen("md:book:gate:BTC/USDT") >= 5


def test_configured_streams_covers_feeds_venues_and_oms():
    """Every stream a running system can produce is enumerated once."""
    config = parse_app_config(
        {
            "venues": VENUES,
            "strategies": [
                {
                    "identifier": "lat",
                    "type": "latency_arb",
                    "production": True,
                    "subscriptions": [
                        {"venue": "gate", "symbol": "BTC/USDT", "feeds": ["book", "trade"]},
                        {"venue": "mexc", "symbol": "SOL/USDT"},
                    ],
                },
                {
                    "identifier": "tst",
                    "type": "latency_arb",
                    "production": False,
                    "subscriptions": [{"venue": "mexc", "symbol": "ETH/USDT"}],
                },
            ],
        }
    )
    assert configured_streams(config, production=True) == [
        "acct:balance:gate",
        "acct:balance:mexc",
        "md:book:gate:BTC/USDT",
        "md:book:mexc:SOL/USDT",
        "md:trade:gate:BTC/USDT",
        "oms:events",
        "oms:intents",
        "oms:latency",
    ]
    assert "md:book:mexc:ETH/USDT" in configured_streams(config, production=False)
    assert "md:book:mexc:ETH/USDT" in configured_streams(config, production=None)


def test_stream_path_layout():
    """Stream names map to nested directories with safe symbol names."""
    root = Path("/tmp/rec")
    assert stream_path(root, "md:book:gate:ALPH/USDT") == root / "md/book/gate/ALPH-USDT"
    assert stream_path(root, "acct:balance:gate") == root / "acct/balance/gate"
    assert stream_path(root, "oms:events") == root / "oms/events"


@pytest.mark.parametrize("bad", ["md::x", "..:y", "md:.:z", ""])
def test_stream_path_rejects_unsafe_names(bad):
    """Empty or dot components are refused."""
    with pytest.raises(ValueError):
        stream_path(Path("/tmp/rec"), bad)


def test_trade_event_routes_to_trade_stream():
    """Sanity check that routing follows the event type."""
    event = TradeEvent(
        ts_recv=TS,
        venue="mexc",
        symbol="SOL/USDT",
        seq=1,
        ts_exch=None,
        trade_id=None,
        side=None,
        price=1.0,
        amount=1.0,
    )
    assert events.stream_for(event) == "md:trade:mexc:SOL/USDT"
