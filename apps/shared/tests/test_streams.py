"""Tests for the stream publisher and stream enumeration."""

from pathlib import Path

import pytest
from fakeredis import aioredis as fakeredis

from apps.shared.src import events
from apps.shared.src.config import parse_app_config
from apps.shared.src.events import BookEvent, TradeEvent
from apps.shared.src.streams import (
    StreamPublisher,
    bucket_of_file,
    configured_streams,
    recording_files,
    sealed_files,
    stream_path,
    stream_tail,
)

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


def test_recording_files_matches_both_suffixes(tmp_path: Path):
    """Compressed recordings are found despite Path.suffix reporting .zst."""
    for name in ("2026-09-06T13.jsonl", "2026-09-06T12.jsonl.zst", "notes.txt"):
        (tmp_path / name).write_bytes(b"")
    (tmp_path / "subdir").mkdir()
    assert [p.name for p in recording_files(tmp_path)] == [
        "2026-09-06T12.jsonl.zst",
        "2026-09-06T13.jsonl",
    ]


def test_recording_files_on_missing_directory(tmp_path: Path):
    """A stream that has never been recorded has no files."""
    assert recording_files(tmp_path / "absent") == []


def test_sealed_files_excludes_the_live_bucket(tmp_path: Path):
    """The newest bucket is never sealed: the recorder still holds it open."""
    for name in ("2026-09-06T12.jsonl", "2026-09-06T13.jsonl", "2026-09-06T14.jsonl"):
        (tmp_path / name).write_bytes(b"")
    assert [p.name for p in sealed_files(tmp_path)] == [
        "2026-09-06T12.jsonl",
        "2026-09-06T13.jsonl",
    ]


def test_sealed_files_with_one_or_no_buckets(tmp_path: Path):
    """A single bucket is the live one, so nothing is sealed."""
    assert sealed_files(tmp_path) == []
    (tmp_path / "2026-09-06T12.jsonl").write_bytes(b"")
    assert sealed_files(tmp_path) == []


def test_sealed_files_returns_both_halves_of_an_interrupted_compaction(tmp_path: Path):
    """A bucket left with both encodings is sealed as a unit, not split."""
    names = ("2026-09-06T12.jsonl", "2026-09-06T12.jsonl.zst", "2026-09-06T13.jsonl")
    for name in names:
        (tmp_path / name).write_bytes(b"")
    assert [p.name for p in sealed_files(tmp_path)] == [
        "2026-09-06T12.jsonl",
        "2026-09-06T12.jsonl.zst",
    ]


def test_bucket_of_file(tmp_path: Path):
    """The bucket is the name without either recording suffix."""
    assert bucket_of_file(Path("2026-09-06T12.jsonl")) == "2026-09-06T12"
    assert bucket_of_file(Path("2026-09-06T12.jsonl.zst")) == "2026-09-06T12"
    assert bucket_of_file(Path("notes.txt")) is None


@pytest.mark.asyncio
async def test_stream_tail_is_the_last_id():
    """Resuming from the tail delivers everything published after it."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=10)
    await publisher.publish(redis, book(1))
    tail = await stream_tail(redis, "md:book:gate:BTC/USDT")
    await publisher.publish(redis, book(2))

    response = await redis.xread({"md:book:gate:BTC/USDT": tail})
    delivered = [events.from_stream_fields(f) for _id, f in response[0][1]]
    assert [e.seq for e in delivered] == [2]


@pytest.mark.asyncio
async def test_stream_tail_of_an_empty_stream_is_the_start():
    """A stream with no entries yet has nothing to skip."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    assert await stream_tail(redis, "md:book:gate:BTC/USDT") == "0-0"
