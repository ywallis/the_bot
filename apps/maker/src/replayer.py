"""Stream replayer: recordings back onto the bus under a backtest prefix.

The replayer is the recorder run backwards. It reads the JSON Lines files the
recorder wrote, plain or zstd-compressed, merges the streams on ``ts_recv``
and ``XADD``s every entry under ``bt:<run_id>:`` with its original id and
its payload byte for byte, so a strategy pointed at that prefix with a replay
clock sees exactly what the live strategy saw.

Three things are deliberate:

- **Per-stream order is never changed.** The merge picks the stream whose
  next record has the smallest ``ts_recv`` but does not sort within a
  stream, so ids stay increasing on every replayed stream and a recording
  with publish jitter replays without reordering what one producer emitted.
- **A range is a filename filter widened by one bucket either side**, then
  a ``ts_recv`` filter over what those files hold. A bucket is named after
  the ``XADD`` time, which trails ``ts_recv`` by however long the producer
  took, so the file for the hour a range starts in is not the only file
  that can hold its first record.
- **Snapshot streams are primed.** A balance is published on change, so the
  balance in force at the start of a range was recorded hours or days
  before it. The last book and balance before the range start are replayed
  first, otherwise a strategy replayed from mid-day judges every quote
  insolvent, which is how the first live run of the runtime stood still.

A sequence gap or reset on any stream is reported, never spliced over. See
``docs/design/event-driven-framework.md`` section 9.
"""

import argparse
import asyncio
import heapq
import io
import logging
import time
from itertools import islice
from collections.abc import AsyncIterator, Iterable, Iterator
from compression import zstd
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import msgspec
from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import ResponseError

import apps.shared.src.logging_config as logging_config
from apps.maker.src.recorder import BUCKET_FORMAT, BUCKET_SECONDS
from apps.shared.src.config import AppConfig, load_app_config
from apps.shared.src.events import (
    DATA_FIELD,
    INTENTS_STREAM,
    LATENCY_STREAM,
    ORDER_EVENTS_STREAM,
    TYPE_FIELD,
    backtest_prefix,
    prefixed,
)
from apps.shared.src.streams import (
    COMPRESSED_SUFFIX,
    bucket_of_file,
    configured_streams,
    read_progress,
    recording_files,
    replay_done_key,
    stream_path,
)
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

NS_PER_S = 1_000_000_000
# Streams a backtest produces itself. Replaying the live ones alongside would
# hand the simulated broker orders it never placed.
OMS_STREAMS = frozenset({INTENTS_STREAM, ORDER_EVENTS_STREAM, LATENCY_STREAM})
# Streams whose latest entry is complete state, see ``runtime.snapshot_streams``.
BOOK_STREAM_PREFIX = "md:book:"
BALANCE_STREAM_PREFIX = "acct:balance:"
SNAPSHOT_STREAM_PREFIXES = (BOOK_STREAM_PREFIX, BALANCE_STREAM_PREFIX)

# How much of a balance stream to replay. A backtest with a simulated broker
# wants the balance in force at the start and nothing after it, since the
# recorded changes are the fills of the live run, not the simulated one.
BALANCES_PRIME = "prime"
BALANCES_ALL = "all"
BALANCES_NONE = "none"
BALANCE_MODES = (BALANCES_PRIME, BALANCES_ALL, BALANCES_NONE)

# How often a following replayer re-reads consumer progress while it waits.
FOLLOW_POLL_S = 0.01
FOLLOW_LOG_EVERY_S = 5.0


# Records ------------------------------------------------------------------


class _Line(msgspec.Struct):
    """One recorder line. ``data`` is kept raw so the replay is byte-exact."""

    id: str
    type: str
    data: msgspec.Raw


class _Head(msgspec.Struct):
    """The two payload fields the replayer reads. Everything else is opaque."""

    ts_recv: int
    seq: int | None = None


_line_decoder = msgspec.json.Decoder(_Line)
_head_decoder = msgspec.json.Decoder(_Head)


@dataclass(frozen=True, slots=True)
class Record:
    """
    One recorded stream entry, ready to be published again.

    Attributes
    ----------
    stream : str
        Unprefixed stream name.
    entry_id : str
        Original Redis stream id.
    event_type : str
        Value of the entry's ``type`` field.
    data : bytes
        Raw JSON payload, exactly as recorded.
    ts_recv : int
        The payload's receive timestamp in nanoseconds.
    seq : int | None
        The payload's per-stream sequence number, if the event has one.
    """

    stream: str
    entry_id: str
    event_type: str
    data: bytes
    ts_recv: int
    seq: int | None


def parse_line(stream: str, line: bytes) -> Record:
    """
    Parse one recorder line into a record.

    Parameters
    ----------
    stream : str
        Unprefixed stream name the line was recorded from.
    line : bytes
        The line, with or without its newline.

    Returns
    -------
    Record
        The record.

    Raises
    ------
    msgspec.ValidationError
        If the line is not a recorder line or its payload has no ``ts_recv``.
    msgspec.DecodeError
        If the line is not JSON.
    """
    parsed = _line_decoder.decode(line)
    data = bytes(parsed.data)
    head = _head_decoder.decode(data)
    return Record(stream, parsed.id, parsed.type, data, head.ts_recv, head.seq)


def open_recording(path: Path) -> io.BufferedIOBase:
    """
    Open a recording for reading, whether compressed or not.

    Parameters
    ----------
    path : Path
        A ``.jsonl`` or ``.jsonl.zst`` file.

    Returns
    -------
    io.BufferedIOBase
        A binary file object yielding lines.
    """
    if path.name.endswith(COMPRESSED_SUFFIX):
        return zstd.open(path, "rb")
    return open(path, "rb")


def iter_file(
    stream: str, path: Path, malformed: list[str] | None = None
) -> Iterator[Record]:
    """
    Yield every record in one recording, in file order.

    A line that does not parse is logged and skipped, since a bad line in a
    recording is a fact about the past that stopping the replay cannot fix.

    Parameters
    ----------
    stream : str
        Unprefixed stream name the file belongs to.
    path : Path
        The recording.
    malformed : list[str] | None
        If given, receives ``"<path>:<line number>"`` for every skipped line.

    Yields
    ------
    Record
        Records in the order they were recorded.
    """
    with open_recording(path) as f:
        for number, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                yield parse_line(stream, line)
            except (msgspec.DecodeError, msgspec.ValidationError) as error:
                logger.error(f"Skipping {path}:{number}: {error}")
                if malformed is not None:
                    malformed.append(f"{path}:{number}")


# Buckets and ranges -------------------------------------------------------


def bucket_of_ns(ts_ns: int) -> str:
    """
    Return the UTC hour bucket a nanosecond timestamp falls in.

    Parameters
    ----------
    ts_ns : int
        Nanoseconds since the epoch.

    Returns
    -------
    str
        ``YYYY-MM-DDTHH``.
    """
    seconds = ts_ns // NS_PER_S
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime(BUCKET_FORMAT)


def bucket_window(
    start_ns: int | None, end_ns: int | None
) -> tuple[str | None, str | None]:
    """
    Return the first and last bucket that can hold records of a range.

    Widened by one bucket either side: a file is named after the ``XADD``
    time of its entries, which trails ``ts_recv``, and a lagging recorder
    can put an entry belonging to one hour into the file of the next.

    Parameters
    ----------
    start_ns : int | None
        Inclusive start of the range, or None for the beginning of the
        recording.
    end_ns : int | None
        Exclusive end of the range, or None for the end of the recording.

    Returns
    -------
    tuple[str | None, str | None]
        Lowest and highest bucket names to read, None meaning unbounded.
        Bucket names sort chronologically, so they compare as strings.
    """
    margin = BUCKET_SECONDS * NS_PER_S
    first = None if start_ns is None else bucket_of_ns(start_ns - margin)
    last = None if end_ns is None else bucket_of_ns(end_ns + margin)
    return first, last


def in_window(bucket: str, first: str | None, last: str | None) -> bool:
    """
    Return whether a bucket lies inside a window returned by ``bucket_window``.

    Parameters
    ----------
    bucket : str
        ``YYYY-MM-DDTHH``.
    first : str | None
        Lowest bucket, or None.
    last : str | None
        Highest bucket, or None.

    Returns
    -------
    bool
        True if the bucket is inside the window.
    """
    if first is not None and bucket < first:
        return False
    return last is None or bucket <= last


def one_file_per_bucket(files: Iterable[Path]) -> list[Path]:
    """
    Drop the compressed copy of a bucket that also exists uncompressed.

    Both exist when a compaction was interrupted between rename and unlink.
    They hold the same entries, and replaying both would replay each twice.

    Parameters
    ----------
    files : Iterable[Path]
        Recording files, as returned by ``recording_files``.

    Returns
    -------
    list[Path]
        One file per bucket, oldest first, the uncompressed one preferred.
    """
    chosen: dict[str, Path] = {}
    for path in files:
        bucket = bucket_of_file(path)
        if bucket is None:
            continue
        current = chosen.get(bucket)
        if current is None or current.name.endswith(COMPRESSED_SUFFIX):
            chosen[bucket] = path
    return [chosen[bucket] for bucket in sorted(chosen)]


def select_files(
    directory: Path, start_ns: int | None, end_ns: int | None
) -> list[Path]:
    """
    Return the recordings of one stream that can hold records of a range.

    Parameters
    ----------
    directory : Path
        Directory returned by ``stream_path``.
    start_ns : int | None
        Inclusive start of the range, or None.
    end_ns : int | None
        Exclusive end of the range, or None.

    Returns
    -------
    list[Path]
        Files whose bucket is in the widened window, oldest first, one per
        bucket.
    """
    first, last = bucket_window(start_ns, end_ns)
    files = one_file_per_bucket(recording_files(directory))
    return [
        path for path in files if in_window(bucket_of_file(path) or "", first, last)
    ]


def files_before(directory: Path, start_ns: int) -> list[Path]:
    """
    Return the recordings of one stream that may hold records before a time.

    Parameters
    ----------
    directory : Path
        Directory returned by ``stream_path``.
    start_ns : int
        The time, nanoseconds since the epoch.

    Returns
    -------
    list[Path]
        Files whose bucket does not start after the time, oldest first. The
        bucket of the time itself is included: its file can hold records
        from before it.
    """
    _, last = bucket_window(None, start_ns)
    files = one_file_per_bucket(recording_files(directory))
    return [path for path in files if in_window(bucket_of_file(path) or "", None, last)]


def in_range(ts_recv: int, start_ns: int | None, end_ns: int | None) -> bool:
    """
    Return whether a timestamp lies in ``[start_ns, end_ns)``.

    Parameters
    ----------
    ts_recv : int
        Nanoseconds since the epoch.
    start_ns : int | None
        Inclusive start, or None.
    end_ns : int | None
        Exclusive end, or None.

    Returns
    -------
    bool
        True if the timestamp is inside the range.
    """
    if start_ns is not None and ts_recv < start_ns:
        return False
    return end_ns is None or ts_recv < end_ns


def is_balance_stream(stream: str) -> bool:
    """
    Return whether a stream carries balance snapshots.

    Parameters
    ----------
    stream : str
        Unprefixed stream name.

    Returns
    -------
    bool
        True for ``acct:balance:*``.
    """
    return stream.startswith(BALANCE_STREAM_PREFIX)


def is_snapshot_stream(stream: str) -> bool:
    """
    Return whether a stream's latest entry is complete state.

    Parameters
    ----------
    stream : str
        Unprefixed stream name.

    Returns
    -------
    bool
        True for book and balance streams.
    """
    return stream.startswith(SNAPSHOT_STREAM_PREFIXES)


def parse_time(text: str) -> int:
    """
    Parse an ISO 8601 timestamp into nanoseconds since the epoch.

    Parameters
    ----------
    text : str
        For example ``2026-09-07T16:09:00Z`` or ``2026-09-07 16:09+02:00``.
        A value without a zone is read as UTC.

    Returns
    -------
    int
        Nanoseconds since the epoch.
    """
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp()) * NS_PER_S + parsed.microsecond * 1000


# Reading a stream ----------------------------------------------------------


def iter_stream(
    stream: str,
    directory: Path,
    start_ns: int | None,
    end_ns: int | None,
    malformed: list[str] | None = None,
) -> Iterator[Record]:
    """
    Yield one stream's records inside a range, in recorded order.

    Parameters
    ----------
    stream : str
        Unprefixed stream name.
    directory : Path
        Directory returned by ``stream_path``.
    start_ns : int | None
        Inclusive start, or None.
    end_ns : int | None
        Exclusive end, or None.
    malformed : list[str] | None
        Receives the locations of skipped lines, see ``iter_file``.

    Yields
    ------
    Record
        Records with ``ts_recv`` in the range.
    """
    for path in select_files(directory, start_ns, end_ns):
        for record in iter_file(stream, path, malformed):
            if in_range(record.ts_recv, start_ns, end_ns):
                yield record


def latest_before(stream: str, directory: Path, start_ns: int) -> Record | None:
    """
    Return the last record of a stream received before a time.

    Scans files newest first and stops at the first file holding one, so a
    balance stream that changed days ago is found without reading the
    whole recording.

    Parameters
    ----------
    stream : str
        Unprefixed stream name.
    directory : Path
        Directory returned by ``stream_path``.
    start_ns : int
        The time, nanoseconds since the epoch.

    Returns
    -------
    Record | None
        The record, or None if nothing precedes the time.
    """
    for path in reversed(files_before(directory, start_ns)):
        latest = None
        for record in iter_file(stream, path):
            if record.ts_recv < start_ns:
                latest = record
        if latest is not None:
            return latest
    return None


def merge(iterators: Iterable[Iterator[Record]]) -> Iterator[Record]:
    """
    Merge per-stream record iterators on ``ts_recv``.

    Each input is consumed in its own order and never reordered, so ids stay
    increasing per stream. Ties keep input order.

    Parameters
    ----------
    iterators : Iterable[Iterator[Record]]
        One iterator per stream, each in recorded order.

    Yields
    ------
    Record
        Records across streams, earliest ``ts_recv`` head first.
    """
    yield from heapq.merge(*iterators, key=lambda record: record.ts_recv)


# Reporting -----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SeqGap:
    """
    A break in a stream's sequence numbers.

    Attributes
    ----------
    stream : str
        Unprefixed stream name.
    entry_id : str
        Id of the entry at which the break was seen.
    previous : int
        Sequence number of the entry before it.
    current : int
        Sequence number of the entry itself.
    """

    stream: str
    entry_id: str
    previous: int
    current: int

    @property
    def is_reset(self) -> bool:
        """
        Return whether the sequence went backwards.

        Returns
        -------
        bool
            True for a reset, which is what a producer restart looks like,
            False for entries missing between two that were recorded.
        """
        return self.current <= self.previous


class GapDetector:
    """Track sequence numbers per stream and report gaps and resets."""

    def __init__(self) -> None:
        """Initialize the detector with no stream seen."""
        self._last: dict[str, int] = {}

    def check(self, record: Record) -> SeqGap | None:
        """
        Note a record's sequence number and report a break if there is one.

        Parameters
        ----------
        record : Record
            The record. One without a sequence number is ignored.

        Returns
        -------
        SeqGap | None
            The break, or None if the sequence continued.
        """
        if record.seq is None:
            return None
        previous = self._last.get(record.stream)
        self._last[record.stream] = record.seq
        if previous is None or record.seq == previous + 1:
            return None
        return SeqGap(record.stream, record.entry_id, previous, record.seq)


@dataclass
class ReplayReport:
    """
    What a replay did.

    Attributes
    ----------
    published : dict[str, int]
        Entries published per unprefixed stream.
    primed : list[str]
        Streams a pre-range snapshot was published for.
    gaps : list[SeqGap]
        Sequence breaks crossed.
    rejected : dict[str, int]
        Entries Redis refused per stream, because an entry with an equal or
        higher id was already there. That is a replay into a prefix already
        used, or a recording with duplicates.
    malformed : list[str]
        Recorder lines that did not parse, as ``<path>:<line>``.
    first_ts_recv : int | None
        Earliest ``ts_recv`` published, priming excluded.
    last_ts_recv : int | None
        Latest ``ts_recv`` published.
    """

    published: dict[str, int] = field(default_factory=dict)
    primed: list[str] = field(default_factory=list)
    gaps: list[SeqGap] = field(default_factory=list)
    rejected: dict[str, int] = field(default_factory=dict)
    malformed: list[str] = field(default_factory=list)
    first_ts_recv: int | None = None
    last_ts_recv: int | None = None

    @property
    def total(self) -> int:
        """
        Return the number of entries published.

        Returns
        -------
        int
            Sum over streams.
        """
        return sum(self.published.values())


# Pacing --------------------------------------------------------------------


class Pacer:
    """
    Map recorded time onto wall time at a chosen speed.

    Attributes
    ----------
    speed : float
        Recorded seconds per wall second. 0 means no pacing at all.
    """

    def __init__(self, speed: float) -> None:
        """
        Initialize the pacer, anchored on the first timestamp it is asked about.

        Parameters
        ----------
        speed : float
            Recorded seconds per wall second, 1 for real time. 0 disables
            pacing.

        Raises
        ------
        ValueError
            If the speed is negative.
        """
        if speed < 0:
            raise ValueError(f"Replay speed must be zero or positive, got {speed}")
        self.speed = speed
        self._first_ts: int | None = None
        self._start_wall: float = 0.0

    def delay(self, ts_recv: int, now: float | None = None) -> float:
        """
        Return how long to wait before a timestamp is due.

        Parameters
        ----------
        ts_recv : int
            Recorded time, nanoseconds since the epoch.
        now : float | None
            Wall time in seconds, ``time.monotonic()`` if omitted.

        Returns
        -------
        float
            Seconds to wait, 0 if the timestamp is already due, is earlier
            than one seen before, or pacing is off.
        """
        if self.speed == 0:
            return 0.0
        wall = time.monotonic() if now is None else now
        if self._first_ts is None:
            self._first_ts = ts_recv
            self._start_wall = wall
            return 0.0
        due = self._start_wall + (ts_recv - self._first_ts) / NS_PER_S / self.speed
        return max(0.0, due - wall)


# Replaying -----------------------------------------------------------------


def replay_streams(
    config: AppConfig, production: bool | None, include_oms: bool
) -> list[str]:
    """
    Enumerate the streams a replay publishes.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    production : bool | None
        Production filter for subscriptions, see ``AppConfig.subscriptions``.
    include_oms : bool
        Whether to replay the order management streams too. A backtest
        produces its own; an analysis replay may want the recorded ones.

    Returns
    -------
    list[str]
        Sorted, unprefixed stream names.
    """
    streams = configured_streams(config, production)
    if include_oms:
        return streams
    return [stream for stream in streams if stream not in OMS_STREAMS]


class Replayer:
    """
    Publish a recording under a backtest prefix.

    Attributes
    ----------
    root : Path
        Recording root directory.
    streams : list[str]
        Unprefixed streams to replay.
    prefix : str
        Key prefix every entry is published under.
    start_ns : int | None
        Inclusive start of the range, or None.
    end_ns : int | None
        Exclusive end of the range, or None.
    pacer : Pacer
        The pacing in effect.
    batch : int
        Entries per pipeline when several are due at once.
    report : ReplayReport
        What has been done so far.
    """

    def __init__(
        self,
        root: Path,
        streams: Iterable[str],
        prefix: str,
        *,
        start_ns: int | None = None,
        end_ns: int | None = None,
        speed: float = 0.0,
        batch: int = 500,
        balances: str = BALANCES_ALL,
        follow: Iterable[str] = (),
        lookahead_s: float = 1.0,
    ) -> None:
        """
        Initialize the replayer.

        Parameters
        ----------
        root : Path
            Recording root directory, ``recorder.root`` in the config.
        streams : Iterable[str]
            Unprefixed streams to replay. One without a recording is skipped
            with a warning.
        prefix : str
            Key prefix, e.g. ``bt:run1``.
        start_ns : int | None
            Inclusive start of the range, or None for the whole recording.
        end_ns : int | None
            Exclusive end of the range, or None.
        speed : float
            Recorded seconds per wall second, 0 for as fast as possible.
        batch : int
            Entries per pipeline when several are due at once.
        balances : str
            ``BALANCES_ALL`` replays balance streams like any other,
            ``BALANCES_PRIME`` publishes only the snapshot in force at the
            start (the primed one, or the first in range when there is no
            start), ``BALANCES_NONE`` skips them.
        follow : Iterable[str]
            Consumer names whose ``replay_progress_key`` this replayer
            follows: it waits for all of them to appear and then never
            publishes more than ``lookahead_s`` of recorded time beyond the
            slowest. Empty to publish freely.
        lookahead_s : float
            Recorded seconds the replay may run ahead of a followed consumer.

        Raises
        ------
        ValueError
            If the prefix is empty, which would publish onto the live
            streams, or the balance mode is unknown.
        """
        if not prefix:
            raise ValueError("A replay needs a prefix; an empty one is the live bus")
        if balances not in BALANCE_MODES:
            raise ValueError(
                f"Balance mode must be one of {BALANCE_MODES}, got {balances!r}"
            )
        self.root = root
        self.streams = list(streams)
        self.prefix = prefix
        self.start_ns = start_ns
        self.end_ns = end_ns
        self.pacer = Pacer(speed)
        self.batch = batch
        self.balances = balances
        self.follow = list(follow)
        self.lookahead_ns = int(lookahead_s * NS_PER_S)
        self.report = ReplayReport()

    def records(self) -> Iterator[Record]:
        """
        Yield every record to publish, primed snapshots first, then merged.

        Yields
        ------
        Record
            Records in publication order.
        """
        primed: list[Record] = []
        iterators: list[Iterator[Record]] = []
        for stream in self.streams:
            if is_balance_stream(stream) and self.balances == BALANCES_NONE:
                continue
            directory = stream_path(self.root, stream)
            if not recording_files(directory):
                logger.warning(f"No recording for {stream} under {directory}")
                continue
            latest = None
            if self.start_ns is not None and is_snapshot_stream(stream):
                latest = latest_before(stream, directory, self.start_ns)
                if latest is not None:
                    primed.append(latest)
                    self.report.primed.append(stream)
            records = iter_stream(
                stream, directory, self.start_ns, self.end_ns, self.report.malformed
            )
            if is_balance_stream(stream) and self.balances == BALANCES_PRIME:
                # Only the balance in force at the start: the primed one if
                # there was one, else the first in range stands in for it.
                records = iter(()) if latest is not None else islice(records, 1)
            iterators.append(records)
        yield from sorted(primed, key=lambda record: record.ts_recv)
        yield from merge(iterators)

    async def batches(self) -> AsyncIterator[list[Record]]:
        """
        Group records into batches that are due together.

        Unpaced, a batch is simply ``batch`` records. Paced, a batch is
        flushed before every wait, so nothing due is held back while the
        replayer sleeps for something later.

        Yields
        ------
        list[Record]
            Non-empty batches in publication order.
        """
        pending: list[Record] = []
        for record in self.records():
            delay = self.pacer.delay(record.ts_recv)
            if delay > 0:
                if pending:
                    yield pending
                    pending = []
                await self.sleep(delay)
            pending.append(record)
            if len(pending) >= self.batch:
                yield pending
                pending = []
        if pending:
            yield pending

    async def sleep(self, seconds: float) -> None:
        """
        Wait between two paced batches.

        Parameters
        ----------
        seconds : float
            Wall seconds to wait.
        """
        await asyncio.sleep(seconds)

    async def publish(self, redis: Any, records: list[Record]) -> None:
        """
        Publish one batch through a pipeline and account for the outcome.

        Parameters
        ----------
        redis : Any
            A ``redis.asyncio.Redis`` client.
        records : list[Record]
            The batch.
        """
        pipe = redis.pipeline(transaction=False)
        for record in records:
            pipe.xadd(
                prefixed(self.prefix, record.stream),
                {TYPE_FIELD: record.event_type, DATA_FIELD: record.data},
                id=record.entry_id,
            )
        results = await pipe.execute(raise_on_error=False)
        for record, result in zip(records, results, strict=True):
            if isinstance(result, ResponseError):
                count = self.report.rejected.get(record.stream, 0)
                if count == 0:
                    logger.error(
                        f"Redis refused {record.entry_id} on {record.stream}: {result}"
                    )
                self.report.rejected[record.stream] = count + 1
                continue
            self.report.published[record.stream] = (
                self.report.published.get(record.stream, 0) + 1
            )
            # Primed snapshots precede the range by construction; they are
            # not what the replay covers, so they do not set its first time.
            if self.report.first_ts_recv is None and in_range(
                record.ts_recv, self.start_ns, self.end_ns
            ):
                self.report.first_ts_recv = record.ts_recv
            self.report.last_ts_recv = record.ts_recv

    async def run(self, redis: Any) -> ReplayReport:
        """
        Replay everything and return the report.

        Parameters
        ----------
        redis : Any
            A ``redis.asyncio.Redis`` client.

        Returns
        -------
        ReplayReport
            What was published, primed, skipped and refused.
        """
        gaps = GapDetector()
        await self.wait_for_consumers(redis)
        async for batch in self.batches():
            for record in batch:
                gap = gaps.check(record)
                if gap is not None:
                    kind = "reset" if gap.is_reset else "gap"
                    logger.warning(
                        f"Sequence {kind} on {gap.stream} at {gap.entry_id}: "
                        f"{gap.previous} -> {gap.current}"
                    )
                    self.report.gaps.append(gap)
            await self.throttle(redis)
            await self.publish(redis, batch)
        await redis.set(replay_done_key(self.prefix), self.report.last_ts_recv or 0)
        return self.report

    async def slowest_consumer(self, redis: Any) -> int | None:
        """
        Return the progress of the followed consumer that is furthest behind.

        Parameters
        ----------
        redis : Any
            A ``redis.asyncio.Redis`` client.

        Returns
        -------
        int | None
            The smallest ``ts_recv`` any followed consumer has reached, or
            None if one has not reported yet or nothing is followed.
        """
        if not self.follow:
            return None
        progress = await read_progress(redis, self.prefix, self.follow)
        if any(value is None for value in progress.values()):
            return None
        return min(cast(dict[str, int], progress).values())

    async def wait_for_consumers(self, redis: Any) -> None:
        """
        Block until every followed consumer has reported progress once.

        Parameters
        ----------
        redis : Any
            A ``redis.asyncio.Redis`` client.
        """
        if not self.follow:
            return
        waited = 0.0
        while await self.slowest_consumer(redis) is None:
            if waited % FOLLOW_LOG_EVERY_S < FOLLOW_POLL_S:
                progress = await read_progress(redis, self.prefix, self.follow)
                missing = sorted(
                    name for name, value in progress.items() if value is None
                )
                logger.info(f"Waiting for {missing} to start under {self.prefix}")
            await self.sleep(FOLLOW_POLL_S)
            waited += FOLLOW_POLL_S

    async def throttle(self, redis: Any) -> None:
        """
        Wait until the slowest followed consumer is within the lookahead.

        Parameters
        ----------
        redis : Any
            A ``redis.asyncio.Redis`` client.
        """
        if not self.follow or self.report.last_ts_recv is None:
            return
        while True:
            slowest = await self.slowest_consumer(redis)
            if (
                slowest is not None
                and self.report.last_ts_recv - slowest <= self.lookahead_ns
            ):
                return
            await self.sleep(FOLLOW_POLL_S)


def log_report(report: ReplayReport, prefix: str) -> None:
    """
    Log a replay's outcome.

    Parameters
    ----------
    report : ReplayReport
        The report.
    prefix : str
        The prefix replayed into.
    """
    logger.info(f"Replayed {report.total} entries under {prefix}:")
    for stream, count in sorted(report.published.items()):
        primed = " (primed)" if stream in report.primed else ""
        logger.info(f"  {stream}: {count}{primed}")
    if report.gaps:
        resets = sum(1 for gap in report.gaps if gap.is_reset)
        logger.warning(
            f"Crossed {len(report.gaps)} sequence breaks, {resets} of them resets"
        )
    if report.rejected:
        logger.error(f"Redis refused {sum(report.rejected.values())} entries")
    if report.malformed:
        logger.error(f"Skipped {len(report.malformed)} malformed lines")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parse the command line.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, ``sys.argv[1:]`` if omitted.

    Returns
    -------
    argparse.Namespace
        ``run_id``, ``start``, ``end`` (nanoseconds or None), ``speed``,
        ``root`` (Path or None), ``include_oms``, ``balances``, ``follow``
        and ``lookahead``.
    """
    parser = argparse.ArgumentParser(
        description="Replay a recording onto Redis under bt:<run_id>."
    )
    parser.add_argument("run_id", help="backtest run id; streams go under bt:<run_id>")
    parser.add_argument(
        "--start", type=parse_time, default=None, help="inclusive ISO 8601 start, UTC"
    )
    parser.add_argument(
        "--end", type=parse_time, default=None, help="exclusive ISO 8601 end, UTC"
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="recorded seconds per wall second; 1 is real time, 0 is unpaced (default)",
    )
    parser.add_argument(
        "--root", type=Path, default=None, help="recording root, default recorder.root"
    )
    parser.add_argument(
        "--include-oms",
        action="store_true",
        help="also replay the recorded order management streams",
    )
    parser.add_argument(
        "--balances",
        choices=BALANCE_MODES,
        default=BALANCES_ALL,
        help="how much of the balance streams to replay; 'prime' for a simulated broker",
    )
    parser.add_argument(
        "--follow",
        metavar="NAME",
        action="append",
        default=[],
        help="consumer to stay within the lookahead of; repeatable",
    )
    parser.add_argument(
        "--lookahead",
        type=float,
        default=1.0,
        help="recorded seconds the replay may run ahead of a followed consumer",
    )
    return parser.parse_args(argv)


async def main(config: AppConfig, args: argparse.Namespace) -> ReplayReport:
    """
    Run a replay against the configured Redis.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    args : argparse.Namespace
        Parsed command line, see ``parse_args``.

    Returns
    -------
    ReplayReport
        The report.
    """
    prefix = backtest_prefix(args.run_id)
    root = args.root if args.root is not None else Path(config.recorder.root)
    replayer = Replayer(
        root,
        replay_streams(config, production, args.include_oms),
        prefix,
        start_ns=args.start,
        end_ns=args.end,
        speed=args.speed,
        balances=args.balances,
        follow=args.follow,
        lookahead_s=args.lookahead,
    )
    window = f"{args.start or 'start'} to {args.end or 'end'}"
    logger.info(
        f"Replaying {len(replayer.streams)} streams from {root.resolve()} "
        f"({window}) under {prefix} at speed {args.speed or 'unpaced'}"
    )
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=4
    )
    redis = Redis(decode_responses=False, connection_pool=pool)
    try:
        report = await replayer.run(redis)
    finally:
        await redis.aclose()
    log_report(report, prefix)
    return report


if __name__ == "__main__":
    asyncio.run(main(load_app_config(), parse_args()))
