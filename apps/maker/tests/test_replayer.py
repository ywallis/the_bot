"""Tests for the stream replayer."""

from compression import zstd
from pathlib import Path
from typing import Any

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import replayer as rp
from apps.maker.src.recorder import format_line
from apps.shared.src import events
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
    VenueConfig,
)
from apps.shared.src.events import (
    AssetBalance,
    BalanceEvent,
    BookEvent,
    OrderEvent,
    OrderState,
    Side,
    from_stream_fields,
    prefixed,
)
from apps.shared.src.runtime import Clock, Runtime, Strategy
from apps.shared.src.streams import stream_path

# 2026-09-06T14:00:00Z in nanoseconds; the tests live in the hours around it.
T14 = 1_788_703_200 * rp.NS_PER_S
HOUR = 3600 * rp.NS_PER_S
MINUTE = 60 * rp.NS_PER_S
BOOK_A = "md:book:venue_a:BASE/QUOTE"
BOOK_B = "md:book:venue_b:BASE/QUOTE"
BALANCE_A = "acct:balance:venue_a"


def entry_id(ts_ns: int, offset_ms: int = 0) -> str:
    """Return a stream id whose millisecond prefix trails ``ts_ns`` a little."""
    return f"{ts_ns // 1_000_000 + offset_ms}-0"


def book(ts_recv: int, seq: int, venue: str = "venue_a") -> BookEvent:
    """Return a one-level book."""
    return BookEvent(
        ts_recv=ts_recv,
        venue=venue,
        symbol="BASE/QUOTE",
        seq=seq,
        ts_exch=None,
        bids=[(0.34, 100.0)],
        asks=[(0.35, 100.0)],
    )


def balance(ts_recv: int, seq: int, free: str = "10") -> BalanceEvent:
    """Return a balance snapshot."""
    return BalanceEvent(
        ts_recv=ts_recv,
        venue="venue_a",
        seq=seq,
        ts_exch=None,
        balances={"QUOTE": AssetBalance(free=float(free), used=0.0, total=float(free))},
    )


def order_event(ts_recv: int) -> OrderEvent:
    """Return an order event, which carries no sequence number."""
    return OrderEvent(
        ts_recv=ts_recv,
        intent_id="i1",
        strategy="s_x",
        venue="venue_a",
        symbol="BASE/QUOTE",
        state=OrderState.OPEN,
        side=Side.SELL,
    )


def line(event: events.AnyEvent, offset_ms: int = 0) -> bytes:
    """Return the recorder line for an event, id derived from its ``ts_recv``."""
    return format_line(
        entry_id(event.ts_recv, offset_ms),
        events.event_type(event).value,
        events.encode(event),
    )


def write(
    root: Path, stream: str, bucket: str, lines: list[bytes], compressed: bool = False
) -> Path:
    """Write recorder lines into a bucket file of a stream and return the path."""
    directory = stream_path(root, stream)
    directory.mkdir(parents=True, exist_ok=True)
    body = b"".join(lines)
    if compressed:
        path = directory / f"{bucket}.jsonl.zst"
        path.write_bytes(zstd.compress(body))
    else:
        path = directory / f"{bucket}.jsonl"
        path.write_bytes(body)
    return path


# Records ------------------------------------------------------------------


def test_parse_line_keeps_the_payload_byte_for_byte():
    """The payload is not re-encoded, and its head fields are read."""
    event = book(T14, 7)
    raw = events.encode(event)
    record = rp.parse_line(BOOK_A, format_line("5-1", "book", raw))
    assert record == rp.Record(BOOK_A, "5-1", "book", raw, T14, 7)
    assert events.decode(record.data) == event


def test_parse_line_without_a_sequence_number():
    """An order event has no ``seq`` and parses with None."""
    record = rp.parse_line("oms:events", line(order_event(T14)))
    assert record.seq is None
    assert record.ts_recv == T14


@pytest.mark.parametrize(
    "bad",
    [
        b"not json",
        b'{"id":"1-0","type":"book"}',
        b'{"id":"1-0","type":"book","data":{}}',
    ],
)
def test_parse_line_rejects_what_is_not_a_recording(bad: bytes):
    """A non-recorder line or a payload without ``ts_recv`` is an error."""
    with pytest.raises(rp.msgspec.MsgspecError):
        rp.parse_line(BOOK_A, bad)


def test_compressed_and_plain_recordings_read_the_same(tmp_path: Path):
    """``open_recording`` picks the codec from the full file name."""
    lines = [line(book(T14 + i * MINUTE, i)) for i in range(1, 4)]
    plain = write(tmp_path, BOOK_A, "2026-09-06T14", lines)
    packed = write(tmp_path, BOOK_B, "2026-09-06T14", lines, compressed=True)
    assert list(rp.iter_file(BOOK_A, plain)) == [
        rp.Record(BOOK_A, r.entry_id, r.event_type, r.data, r.ts_recv, r.seq)
        for r in rp.iter_file(BOOK_B, packed)
    ]
    assert [r.seq for r in rp.iter_file(BOOK_A, plain)] == [1, 2, 3]


def test_iter_file_skips_and_reports_malformed_lines(tmp_path: Path):
    """A bad line is skipped, located, and does not stop the rest."""
    lines = [line(book(T14, 1)), b"garbage\n", b"\n", line(book(T14 + MINUTE, 2))]
    path = write(tmp_path, BOOK_A, "2026-09-06T14", lines)
    malformed: list[str] = []
    assert [r.seq for r in rp.iter_file(BOOK_A, path, malformed)] == [1, 2]
    assert malformed == [f"{path}:2"]


# Buckets and ranges -------------------------------------------------------


def test_bucket_window_is_widened_by_one_hour_either_side():
    """A range reads the hour before its start and the hour after its end."""
    assert rp.bucket_window(T14 + 30 * MINUTE, T14 + 70 * MINUTE) == (
        "2026-09-06T13",
        "2026-09-06T16",
    )
    assert rp.bucket_window(None, None) == (None, None)
    assert rp.bucket_window(T14, None) == ("2026-09-06T13", None)


def test_in_window_compares_bucket_names():
    """Bucket names sort chronologically, so the window is a string range."""
    assert rp.in_window("2026-09-06T13", "2026-09-06T13", "2026-09-06T16")
    assert rp.in_window("2026-09-06T16", "2026-09-06T13", "2026-09-06T16")
    assert not rp.in_window("2026-09-06T12", "2026-09-06T13", None)
    assert not rp.in_window("2026-09-06T17", None, "2026-09-06T16")
    assert rp.in_window("2026-09-07T00", None, None)


def test_one_file_per_bucket_prefers_the_uncompressed_copy(tmp_path: Path):
    """A bucket left in both forms by an interrupted compaction replays once."""
    write(
        tmp_path, BOOK_A, "2026-09-06T13", [line(book(T14 - HOUR, 1))], compressed=True
    )
    write(tmp_path, BOOK_A, "2026-09-06T14", [line(book(T14, 2))], compressed=True)
    plain = write(tmp_path, BOOK_A, "2026-09-06T14", [line(book(T14, 2))])
    (stream_path(tmp_path, BOOK_A) / "notes.txt").write_text("ignored")
    files = rp.select_files(stream_path(tmp_path, BOOK_A), None, None)
    assert [p.name for p in files] == ["2026-09-06T13.jsonl.zst", "2026-09-06T14.jsonl"]
    assert files[1] == plain


def test_select_files_applies_the_window(tmp_path: Path):
    """Only buckets inside the widened window are read."""
    for hour in range(10, 19):
        write(tmp_path, BOOK_A, f"2026-09-06T{hour}", [])
    files = rp.select_files(
        stream_path(tmp_path, BOOK_A), T14 + 30 * MINUTE, T14 + 70 * MINUTE
    )
    assert [rp.bucket_of_file(p) for p in files] == [
        "2026-09-06T13",
        "2026-09-06T14",
        "2026-09-06T15",
        "2026-09-06T16",
    ]
    assert rp.select_files(tmp_path / "missing", None, None) == []


def test_iter_stream_filters_on_ts_recv_not_on_the_file(tmp_path: Path):
    """A record in the widened files is kept only if its time is in range."""
    write(tmp_path, BOOK_A, "2026-09-06T13", [line(book(T14 - MINUTE, 1))])
    # An entry received at 14:59:59 but added to the stream in the 15 bucket.
    late = book(T14 + HOUR - 1, 3)
    write(
        tmp_path,
        BOOK_A,
        "2026-09-06T14",
        [line(book(T14, 2)), line(late, offset_ms=2000)],
    )
    write(tmp_path, BOOK_A, "2026-09-06T15", [line(book(T14 + HOUR, 4))])
    records = list(
        rp.iter_stream(BOOK_A, stream_path(tmp_path, BOOK_A), T14, T14 + HOUR)
    )
    assert [r.seq for r in records] == [2, 3]


def test_latest_before_scans_back_across_files(tmp_path: Path):
    """The snapshot in force at a time may be days old; it is still found."""
    directory = stream_path(tmp_path, BALANCE_A)
    write(
        tmp_path,
        BALANCE_A,
        "2026-09-04T09",
        [line(balance(T14 - 2 * 24 * HOUR, 1, "5"))],
    )
    write(
        tmp_path,
        BALANCE_A,
        "2026-09-04T10",
        [line(balance(T14 - 2 * 24 * HOUR + HOUR, 2, "7"))],
    )
    write(tmp_path, BALANCE_A, "2026-09-06T14", [line(balance(T14 + MINUTE, 3, "9"))])
    latest = rp.latest_before(BALANCE_A, directory, T14)
    assert latest is not None
    assert latest.seq == 2
    assert rp.latest_before(BALANCE_A, directory, T14 - 3 * 24 * HOUR) is None


def test_merge_orders_across_streams_but_never_within_one():
    """The merge is on ``ts_recv`` heads; a stream's own order is preserved."""

    def record(stream: str, ts: int, seq: int) -> rp.Record:
        return rp.Record(stream, f"{seq}-0", "book", b"{}", ts, seq)

    # Stream B has publish jitter: its second record was received before
    # its first but added to the stream after it.
    a = [record("a", 10, 1), record("a", 30, 2), record("a", 50, 3)]
    b = [record("b", 20, 1), record("b", 15, 2), record("b", 40, 3)]
    merged = list(rp.merge([iter(a), iter(b)]))
    assert [(r.stream, r.seq) for r in merged] == [
        ("a", 1),
        ("b", 1),
        ("b", 2),
        ("a", 2),
        ("b", 3),
        ("a", 3),
    ]


def test_gap_detector_reports_gaps_and_resets():
    """A missing number is a gap, a lower one a reset, no number is ignored."""

    def record(stream: str, seq: int | None) -> rp.Record:
        return rp.Record(stream, f"{seq or 0}-0", "book", b"{}", 0, seq)

    detector = rp.GapDetector()
    assert detector.check(record("a", 1)) is None
    assert detector.check(record("a", 2)) is None
    gap = detector.check(record("a", 5))
    assert gap == rp.SeqGap("a", "5-0", 2, 5)
    assert not gap.is_reset
    reset = detector.check(record("a", 1))
    assert reset is not None and reset.is_reset
    assert detector.check(record("b", 4)) is None  # first seen on b
    assert detector.check(record("oms", None)) is None


def test_pacer_maps_recorded_time_onto_wall_time():
    """Delays follow the recorded spacing divided by the speed."""
    pacer = rp.Pacer(speed=2.0)
    assert pacer.delay(T14, now=100.0) == 0.0
    assert pacer.delay(T14 + 4 * rp.NS_PER_S, now=100.0) == pytest.approx(2.0)
    assert pacer.delay(T14 + 4 * rp.NS_PER_S, now=101.5) == pytest.approx(0.5)
    assert pacer.delay(T14 - rp.NS_PER_S, now=100.0) == 0.0  # jitter, never negative
    assert rp.Pacer(speed=0).delay(T14 + HOUR, now=0.0) == 0.0
    with pytest.raises(ValueError):
        rp.Pacer(speed=-1)


def test_parse_time_reads_iso_8601_as_utc_by_default():
    """A zone is honoured, its absence means UTC."""
    assert rp.parse_time("2026-09-06T14:00:00Z") == T14
    assert rp.parse_time("2026-09-06T16:00:00+02:00") == T14
    assert rp.parse_time("2026-09-06 14:00") == T14
    assert rp.parse_time("2026-09-06T14:00:00.250Z") == T14 + 250_000_000


def test_replay_streams_leaves_out_the_oms_streams_unless_asked():
    """A backtest produces its own order streams."""
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(VenueConfig(id="venue_a", name="A"),),
        strategies=(
            StrategyConfig(
                identifier="s",
                type="t",
                production=False,
                subscriptions=(Subscription(venue="venue_a", symbol="BASE/QUOTE"),),
                params={},
            ),
        ),
    )
    assert rp.replay_streams(config, None, include_oms=False) == [BALANCE_A, BOOK_A]
    assert set(rp.replay_streams(config, None, include_oms=True)) >= rp.OMS_STREAMS


# Replaying -----------------------------------------------------------------


def recording(root: Path) -> dict[str, list[events.AnyEvent]]:
    """
    Write a small two-venue recording and return the events per stream.

    Venue A's book runs 13:30 to 15:30 with a sequence gap at 14:20 and a
    reset at 15:00; venue B's book runs through 14:xx; venue A's balance
    changed once at 09:00 and once at 14:10.
    """
    books_a = [
        book(T14 - 30 * MINUTE, 1),
        book(T14 + 5 * MINUTE, 2),
        book(T14 + 20 * MINUTE, 4),  # 3 is missing
        book(T14 + 40 * MINUTE, 5),
        book(T14 + HOUR, 1),  # producer restarted
        book(T14 + 90 * MINUTE, 2),
    ]
    books_b = [
        book(T14 + 10 * MINUTE, 1, "venue_b"),
        book(T14 + 30 * MINUTE, 2, "venue_b"),
    ]
    balances = [balance(T14 - 5 * HOUR, 1, "5"), balance(T14 + 10 * MINUTE, 2, "8")]
    write(root, BOOK_A, "2026-09-06T13", [line(books_a[0])])
    write(
        root, BOOK_A, "2026-09-06T14", [line(e) for e in books_a[1:4]], compressed=True
    )
    write(root, BOOK_A, "2026-09-06T15", [line(e) for e in books_a[4:]])
    write(root, BOOK_B, "2026-09-06T14", [line(e) for e in books_b])
    write(root, BALANCE_A, "2026-09-06T09", [line(balances[0])])
    write(root, BALANCE_A, "2026-09-06T14", [line(balances[1])])
    return {BOOK_A: books_a, BOOK_B: books_b, BALANCE_A: balances}


@pytest.mark.asyncio
async def test_replay_publishes_the_range_under_the_prefix_with_original_ids(
    tmp_path: Path,
):
    """Entries land on prefixed streams, byte-exact, in id order per stream."""
    expected = recording(tmp_path)
    redis = fakeredis.FakeRedis()
    replayer = rp.Replayer(
        tmp_path,
        [BOOK_A, BOOK_B, BALANCE_A, "oms:events"],
        "bt:r1",
        start_ns=T14,
        end_ns=T14 + HOUR,
    )
    report = await replayer.run(redis)

    # The book before the range primes the stream, then the three in range.
    replayed = expected[BOOK_A][:4]
    entries = await redis.xrange(prefixed("bt:r1", BOOK_A))
    assert [i.decode() for i, _ in entries] == [entry_id(e.ts_recv) for e in replayed]
    assert [f[b"data"] for _, f in entries] == [events.encode(e) for e in replayed]
    assert [from_stream_fields(f) for _, f in entries] == replayed
    assert await redis.exists(BOOK_A) == 0  # nothing on the live stream

    assert report.published == {BOOK_A: 4, BOOK_B: 2, BALANCE_A: 2}
    assert report.total == 8
    assert sorted(report.primed) == [BALANCE_A, BOOK_A]
    assert report.first_ts_recv == T14 + 5 * MINUTE
    assert report.last_ts_recv == T14 + 40 * MINUTE
    assert report.rejected == {}
    assert report.malformed == []
    assert [(g.stream, g.previous, g.current) for g in report.gaps] == [(BOOK_A, 2, 4)]


@pytest.mark.asyncio
async def test_replay_primes_snapshot_streams_with_the_state_before_the_range(
    tmp_path: Path,
):
    """The balance in force at the range start is replayed first."""
    expected = recording(tmp_path)
    redis = fakeredis.FakeRedis()
    replayer = rp.Replayer(tmp_path, [BOOK_A, BALANCE_A], "bt:r1", start_ns=T14)
    report = await replayer.run(redis)

    balances = await redis.xrange(prefixed("bt:r1", BALANCE_A))
    assert [from_stream_fields(f) for _, f in balances] == expected[BALANCE_A]
    books = await redis.xrange(prefixed("bt:r1", BOOK_A))
    assert [from_stream_fields(f) for _, f in books] == expected[BOOK_A]
    assert sorted(report.primed) == [BALANCE_A, BOOK_A]
    # The whole tail was replayed, gap and reset included.
    assert [g.is_reset for g in report.gaps] == [False, True]
    assert report.first_ts_recv == T14 + 5 * MINUTE


@pytest.mark.asyncio
async def test_replay_order_across_streams_follows_ts_recv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Publication interleaves streams by receive time."""
    recording(tmp_path)
    redis = fakeredis.FakeRedis()
    replayer = rp.Replayer(
        tmp_path, [BOOK_A, BOOK_B, BALANCE_A], "bt:r1", start_ns=T14, end_ns=T14 + HOUR
    )
    order: list[tuple[str, int]] = []

    async def publish(redis: Any, records: list[rp.Record]) -> None:
        order.extend((r.stream, r.ts_recv) for r in records)

    monkeypatch.setattr(replayer, "publish", publish)
    await replayer.run(redis)
    assert order == [
        (BALANCE_A, T14 - 5 * HOUR),  # primed
        (BOOK_A, T14 - 30 * MINUTE),  # primed
        (BOOK_A, T14 + 5 * MINUTE),
        (BOOK_B, T14 + 10 * MINUTE),
        (BALANCE_A, T14 + 10 * MINUTE),
        (BOOK_A, T14 + 20 * MINUTE),
        (BOOK_B, T14 + 30 * MINUTE),
        (BOOK_A, T14 + 40 * MINUTE),
    ]


@pytest.mark.asyncio
async def test_replaying_into_a_used_prefix_is_refused_by_redis(tmp_path: Path):
    """Original ids make a second replay collide instead of duplicating."""
    recording(tmp_path)
    redis = fakeredis.FakeRedis()
    await rp.Replayer(tmp_path, [BOOK_B], "bt:r1").run(redis)
    report = await rp.Replayer(tmp_path, [BOOK_B], "bt:r1").run(redis)
    # Redis refuses an id equal to or below the stream top. fakeredis only
    # refuses the one below, so the second entry cannot be asserted on here;
    # the refusal itself, and its accounting, can.
    assert report.rejected[BOOK_B] >= 1
    assert report.rejected[BOOK_B] + report.published.get(BOOK_B, 0) == 2


@pytest.mark.asyncio
async def test_a_stream_without_a_recording_is_skipped(tmp_path: Path, caplog: Any):
    """A configured stream nobody recorded warns and does not abort."""
    recording(tmp_path)
    redis = fakeredis.FakeRedis()
    report = await rp.Replayer(
        tmp_path, [BOOK_B, "md:trade:venue_a:BASE/QUOTE"], "bt:r1"
    ).run(redis)
    assert report.published == {BOOK_B: 2}
    assert "No recording for md:trade:venue_a:BASE/QUOTE" in caplog.text


def test_an_empty_prefix_is_refused(tmp_path: Path):
    """Replaying onto the live streams is never what anyone means."""
    with pytest.raises(ValueError):
        rp.Replayer(tmp_path, [BOOK_A], "")


@pytest.mark.asyncio
async def test_paced_batches_flush_before_every_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Records due together share a batch; a wait separates batches."""
    recording(tmp_path)
    replayer = rp.Replayer(
        tmp_path, [BOOK_A, BOOK_B], "bt:r1", start_ns=T14, end_ns=T14 + HOUR, speed=1.0
    )
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(replayer, "sleep", sleep)
    batches = [[r.ts_recv for r in b] async for b in replayer.batches()]
    # The primed snapshot anchors the pacer, then each later record is its
    # own batch: they are minutes apart and the pacer sleeps between them.
    assert batches == [
        [T14 - 30 * MINUTE],
        [T14 + 5 * MINUTE],
        [T14 + 10 * MINUTE],
        [T14 + 20 * MINUTE],
        [T14 + 30 * MINUTE],
        [T14 + 40 * MINUTE],
    ]
    assert len(sleeps) == 5
    assert sleeps[0] == pytest.approx(35 * 60)


@pytest.mark.asyncio
async def test_a_runtime_under_the_prefix_reads_the_replay(tmp_path: Path):
    """A strategy on a replay clock reads the replay from its start, in time order."""
    recording(tmp_path)
    redis = fakeredis.FakeRedis(decode_responses=True)
    await rp.Replayer(
        tmp_path, [BOOK_A, BALANCE_A], "bt:r1", start_ns=T14, end_ns=T14 + HOUR
    ).run(redis)

    seen: list[events.AnyEvent] = []

    class Probe(Strategy):
        async def on_book(self, event: BookEvent) -> None:
            seen.append(event)

        async def on_balance(self, event: BalanceEvent) -> None:
            seen.append(event)

    config = AppConfig(
        redis=RedisConfig(), market_data=MarketDataConfig(), venues=(), strategies=()
    )
    strategy = StrategyConfig(
        identifier="probe",
        type="probe",
        production=False,
        subscriptions=(Subscription(venue="venue_a", symbol="BASE/QUOTE"),),
        params={},
    )
    runtime = Runtime(
        redis, config, strategy, Probe(), clock=Clock.replay(), prefix="bt:r1"
    )
    await runtime.start()
    assert seen == []  # a replayed prefix is read from its start, not primed
    await runtime.step()
    # The primed snapshots first, then the range, merged across streams.
    assert [(type(e).__name__, e.ts_recv) for e in seen] == [
        ("BalanceEvent", T14 - 5 * HOUR),
        ("BookEvent", T14 - 30 * MINUTE),
        ("BookEvent", T14 + 5 * MINUTE),
        ("BalanceEvent", T14 + 10 * MINUTE),
        ("BookEvent", T14 + 20 * MINUTE),
        ("BookEvent", T14 + 40 * MINUTE),
    ]
    assert runtime.clock.now() == T14 + 40 * MINUTE


def test_parse_args_reads_the_command_line():
    """The run id is positional; times parse to nanoseconds."""
    args = rp.parse_args(
        ["r1", "--start", "2026-09-06T14:00Z", "--speed", "2", "--include-oms"]
    )
    assert args.run_id == "r1"
    assert args.start == T14
    assert args.end is None
    assert args.speed == 2.0
    assert args.include_oms is True
    assert args.root is None


@pytest.mark.asyncio
async def test_main_replays_under_bt_run_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """``main`` derives the prefix, the streams and the root from its inputs."""
    recording(tmp_path)
    redis = fakeredis.FakeRedis()
    monkeypatch.setattr(rp, "Redis", lambda **kwargs: redis)
    monkeypatch.setattr(rp, "ConnectionPool", lambda **kwargs: None)
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(
            VenueConfig(id="venue_a", name="A"),
            VenueConfig(id="venue_b", name="B"),
        ),
        strategies=(
            StrategyConfig(
                identifier="s",
                type="t",
                production=rp.production,
                subscriptions=(
                    Subscription(venue="venue_a", symbol="BASE/QUOTE"),
                    Subscription(venue="venue_b", symbol="BASE/QUOTE"),
                ),
                params={},
            ),
        ),
    )
    args = rp.parse_args(
        ["run7", "--root", str(tmp_path), "--start", "2026-09-06T14:00Z"]
    )
    report = await rp.main(config, args)
    assert report.published == {BOOK_A: 6, BOOK_B: 2, BALANCE_A: 2}
    assert await redis.xlen("bt:run7:" + BOOK_B) == 2
