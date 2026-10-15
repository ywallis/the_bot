"""Helpers for producing, enumerating and locating Redis Streams.

``StreamPublisher`` is the one place that knows how an event becomes an
``XADD``: stream routing, optional namespace prefix, approximate trimming and
per-stream sequence numbers. Feed handlers use it; consumers read with
``XREAD`` directly.

The rest of the module owns the on-disk layout of recordings, including
``sealed_files``, the invariant every tool that touches a recording
directory depends on. See ``docs/design/event-driven-framework.md`` section 7.
"""

import json
from collections import defaultdict
from compression import zstd
from datetime import datetime, timezone
from io import BufferedIOBase
from pathlib import Path
from typing import Any

from apps.shared.src.config import (
    BOOK_FEED,
    DELTA_FEED,
    FUNDING_FEED,
    POSITION_FEED,
    TRADE_FEED,
    AppConfig,
)
from apps.shared.src.events import (
    ALERTS_STREAM,
    INTENTS_STREAM,
    LATENCY_STREAM,
    ORDER_EVENTS_STREAM,
    AnyEvent,
    balance_stream,
    book_stream,
    delta_stream,
    fees_stream,
    funding_stream,
    position_stream,
    prefixed,
    stream_for,
    to_stream_fields,
    trade_stream,
)

# On-disk recording names. Recordings are uncompressed while the recorder
# may still append to them and compressed once sealed and shipped.
FILE_SUFFIX = ".jsonl"
COMPRESSED_SUFFIX = ".jsonl.zst"
# One file per UTC hour. Small enough that data does not sit long on the
# trading host and that a backtest range is a filename filter, large enough
# that compression is unaffected: the zstd ratio on book data is flat above
# roughly 300 KB and an hour of any active feed is far past that.
BUCKET_FORMAT = "%Y-%m-%dT%H"
BUCKET_SECONDS = 3600


class StreamPublisher:
    """
    Publish events to their streams with bounded length and sequencing.

    Attributes
    ----------
    maxlen : int
        Approximate maximum stream length passed to ``XADD``.
    prefix : str
        Namespace prefix, empty in live trading.
    """

    def __init__(self, maxlen: int, prefix: str = "") -> None:
        """
        Initialize the publisher.

        Parameters
        ----------
        maxlen : int
            Approximate maximum stream length passed to ``XADD``.
        prefix : str
            Namespace prefix, e.g. ``bt:run1``. Empty for live streams.
        """
        self.maxlen = maxlen
        self.prefix = prefix
        self._seq: defaultdict[str, int] = defaultdict(int)

    def next_seq(self, stream: str) -> int:
        """
        Return the next sequence number for a stream.

        Sequence numbers start at 1 per process lifetime, so a consumer sees
        a reset to 1 when the producer restarts and a gap when it missed
        entries.

        Parameters
        ----------
        stream : str
            Unprefixed stream name.

        Returns
        -------
        int
            The sequence number to put on the next event for this stream.
        """
        self._seq[stream] += 1
        return self._seq[stream]

    def xadd(self, target: Any, event: AnyEvent) -> str:
        """
        Queue or send an ``XADD`` for an event.

        Parameters
        ----------
        target : Any
            A ``redis.asyncio.Redis`` client or a pipeline. On a client the
            returned awaitable must be awaited; on a pipeline the command is
            queued until ``execute``.
        event : AnyEvent
            The event to publish.

        Returns
        -------
        str
            The prefixed stream name the event was routed to.
        """
        stream = prefixed(self.prefix, stream_for(event))
        target.xadd(
            stream,
            to_stream_fields(event),
            maxlen=self.maxlen,
            approximate=True,
        )
        return stream

    async def publish(self, redis: Any, event: AnyEvent) -> str:
        """
        Send a single event immediately.

        Parameters
        ----------
        redis : Any
            A ``redis.asyncio.Redis`` client.
        event : AnyEvent
            The event to publish.

        Returns
        -------
        str
            The prefixed stream name the event was routed to.
        """
        stream = prefixed(self.prefix, stream_for(event))
        await redis.xadd(
            stream,
            to_stream_fields(event),
            maxlen=self.maxlen,
            approximate=True,
        )
        return stream


def entry_id_str(entry_id: Any) -> str:
    """
    Return a Redis stream entry id as the string a command will accept.

    Whether ids arrive as ``bytes`` or ``str`` is decided by the connection
    pool, not by the client: ``decode_responses`` passed to ``Redis`` is
    ignored when an existing ``ConnectionPool`` is handed in, and the pools
    in this app are built without it. So an id read from a stream is bytes
    in production and ``str`` in a test that builds a client directly, and
    ``str(b"1-0")`` is ``"b'1-0'"``, which Redis rejects as an invalid
    stream id. Every id that crosses from a reply back into a command goes
    through here.

    Parameters
    ----------
    entry_id : Any
        Entry id from an ``XREAD``, ``XREADGROUP`` or range reply.

    Returns
    -------
    str
        The id.
    """
    return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)


async def stream_tail(redis: Any, stream: str) -> str:
    """
    Return the id to start from to receive only entries added after now.

    ``XREAD`` accepts ``$`` for this, but ``$`` is resolved afresh on every
    call, so an entry published while a consumer sits between two reads is
    skipped without a trace. Resolving the tail once and then advancing
    through concrete ids closes that window: anything added after this call
    carries a higher id and is delivered.

    Parameters
    ----------
    redis : Any
        A ``redis.asyncio.Redis`` client.
    stream : str
        Stream name, already prefixed if the caller uses a prefix.

    Returns
    -------
    str
        The last entry id, or ``0-0`` for a stream that has none.
    """
    entries = await redis.xrevrange(stream, count=1)
    if not entries:
        return "0-0"
    return entry_id_str(entries[0][0])


def feed_stream(feed: str, venue: str, symbol: str) -> str:
    """
    Return the stream a feed of one subscription is published on.

    Parameters
    ----------
    feed : str
        Feed name, one of ``config.KNOWN_FEEDS``.
    venue : str
        Venue id of the subscription.
    symbol : str
        CCXT symbol of the subscription.

    Returns
    -------
    str
        Unprefixed stream name. A position is per venue, a delta per base
        asset; every other feed is per venue and symbol.

    Raises
    ------
    ValueError
        If the feed is unknown.
    """
    match feed:
        case "book":
            return book_stream(venue, symbol)
        case "trade":
            return trade_stream(venue, symbol)
        case "position":
            return position_stream(venue)
        case "funding":
            return funding_stream(venue, symbol)
        case "delta":
            return delta_stream(symbol.partition("/")[0])
    raise ValueError(f"Unknown feed {feed!r}")


def configured_streams(config: AppConfig, production: bool | None) -> list[str]:
    """
    Enumerate every stream the running system can produce.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    production : bool | None
        Production filter for subscriptions, see ``AppConfig.subscriptions``.

    Returns
    -------
    list[str]
        Sorted, unprefixed stream names: one book and trade stream per
        subscribed feed, the position, funding and delta streams the
        derivatives feeds publish, one balance and fee schedule stream per
        venue, the order management streams and the alerts.
    """
    streams: set[str] = set()
    for feed in (BOOK_FEED, TRADE_FEED, POSITION_FEED, FUNDING_FEED, DELTA_FEED):
        for venue, symbol in config.feed_pairs(feed, production):
            streams.add(feed_stream(feed, venue, symbol))
    for venue_config in config.venues:
        streams.add(balance_stream(venue_config.id))
        streams.add(fees_stream(venue_config.id))
    streams.update({INTENTS_STREAM, ORDER_EVENTS_STREAM, LATENCY_STREAM, ALERTS_STREAM})
    return sorted(streams)


def bucket_of_file(path: Path) -> str | None:
    """
    Return the bucket a recording file holds, or None if it is not one.

    Parameters
    ----------
    path : Path
        Any path inside a stream directory.

    Returns
    -------
    str | None
        ``YYYY-MM-DDTHH``, or None for a file the recorder did not write.
    """
    for suffix in (COMPRESSED_SUFFIX, FILE_SUFFIX):
        if path.name.endswith(suffix):
            return path.name[: -len(suffix)]
    return None


def recording_files(directory: Path) -> list[Path]:
    """
    Return every recording in a stream directory, oldest bucket first.

    Matches on the full name rather than ``Path.suffix``, which reports
    ``.zst`` for a compressed recording and would silently skip it.

    Parameters
    ----------
    directory : Path
        Directory returned by ``stream_path``.

    Returns
    -------
    list[Path]
        Recording files, sorted by bucket then name. Anything else in the
        directory is ignored.
    """
    if not directory.is_dir():
        return []
    found = [(bucket, p) for p in directory.iterdir() if (bucket := bucket_of_file(p))]
    return [path for _, path in sorted(found, key=lambda item: (item[0], item[1].name))]


def sealed_files(directory: Path) -> list[Path]:
    """
    Return the recordings the recorder is guaranteed to have closed.

    The newest bucket is excluded because rotation is driven by entry
    arrival: a quiet stream can hold its file open long after the bucket
    elapsed, and only the existence of a later bucket proves the earlier one
    was closed. Compaction, the shipper and any retention job must select
    files through this function. Touching the live file would unlink the
    recorder's open append fd, losing every subsequent write without an
    error, and would leave ``last_recorded_id`` with no tail to resume from.

    Parameters
    ----------
    directory : Path
        Directory returned by ``stream_path``.

    Returns
    -------
    list[Path]
        Sealed recordings, oldest first. Both compressed and uncompressed
        files of a sealed bucket are returned, which happens when a previous
        compaction was interrupted between rename and unlink.
    """
    files = recording_files(directory)
    if not files:
        return []
    live = bucket_of_file(files[-1])
    return [path for path in files if bucket_of_file(path) != live]


def recording_directories(root: Path) -> list[Path]:
    """
    Return every stream directory under a recording root.

    Parameters
    ----------
    root : Path
        Recording root directory.

    Returns
    -------
    list[Path]
        Directories that hold at least one recording, sorted.
    """
    if not root.is_dir():
        return []
    found = {path.parent for path in root.rglob("*") if bucket_of_file(path)}
    return sorted(found)


def open_recording(path: Path) -> BufferedIOBase:
    """
    Open a recording for binary reading, compressed or not.

    Every reader of a recording goes through this function, so a tier can
    compress its files without breaking a tool that reads them.

    Parameters
    ----------
    path : Path
        A ``.jsonl`` or ``.jsonl.zst`` recording.

    Returns
    -------
    BufferedIOBase
        A file object that yields the uncompressed lines.
    """
    if path.name.endswith(COMPRESSED_SUFFIX):
        return zstd.open(path, "rb")
    return open(path, "rb")


def stream_path(root: Path, stream: str) -> Path:
    """
    Map a stream name to the directory its recordings live in.

    Each colon-separated component becomes a directory and slashes inside a
    component are replaced by dashes, so ``md:book:venue_a:BASE/QUOTE`` becomes
    ``<root>/md/book/venue_a/BASE-QUOTE``.

    Parameters
    ----------
    root : Path
        Recording root directory.
    stream : str
        Unprefixed stream name.

    Returns
    -------
    Path
        Directory for the stream.

    Raises
    ------
    ValueError
        If a component is empty or a relative path marker.
    """
    parts = [part.replace("/", "-") for part in stream.split(":")]
    for part in parts:
        if part in ("", ".", ".."):
            raise ValueError(f"Stream name {stream!r} is not a valid path")
    return root.joinpath(*parts)


def bucket_of_entry(entry_id: str) -> str:
    """
    Return the UTC hour bucket a stream entry was added in.

    Parameters
    ----------
    entry_id : str
        Redis stream id, ``<ms>-<seq>``.

    Returns
    -------
    str
        ``YYYY-MM-DDTHH``.
    """
    ms = int(entry_id.split("-", 1)[0])
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(BUCKET_FORMAT)


def bucket_end(bucket: str) -> float:
    """
    Return the epoch second at which a bucket stops accepting entries.

    Parameters
    ----------
    bucket : str
        ``YYYY-MM-DDTHH``.

    Returns
    -------
    float
        Seconds since the epoch.
    """
    start = datetime.strptime(bucket, BUCKET_FORMAT).replace(tzinfo=timezone.utc)
    return start.timestamp() + BUCKET_SECONDS


def format_line(entry_id: str, event_type: str, data: bytes) -> bytes:
    """
    Build one JSON Lines record without re-encoding the payload.

    Parameters
    ----------
    entry_id : str
        Redis stream id.
    event_type : str
        Value of the entry's ``type`` field.
    data : bytes
        Raw JSON payload from the entry's ``data`` field.

    Returns
    -------
    bytes
        The line, newline terminated.
    """
    head = json.dumps({"id": entry_id, "type": event_type}, separators=(",", ":"))
    return b"".join((head[:-1].encode(), b',"data":', data, b"}\n"))


class StreamWriter:
    """
    Append-only writer for one stream, rotating files by UTC hour.

    Attributes
    ----------
    directory : Path
        Directory the bucket files live in.
    """

    def __init__(self, directory: Path) -> None:
        """
        Initialize the writer.

        Parameters
        ----------
        directory : Path
            Directory returned by ``stream_path``. Created on first write.
        """
        self.directory = directory
        self._bucket: str | None = None
        self._bucket_end: float = 0.0
        self._file: Any = None

    def write(self, entry_id: str, event_type: str, data: bytes) -> None:
        """
        Append one entry, opening a new file when the bucket changes.

        Files are opened for append, so an entry belonging to a bucket this
        writer already sealed reopens it rather than truncating it. That
        happens when the recorder is lagging far enough behind Redis to
        cross a bucket boundary.

        Parameters
        ----------
        entry_id : str
            Redis stream id.
        event_type : str
            Value of the entry's ``type`` field.
        data : bytes
            Raw JSON payload.
        """
        bucket = bucket_of_entry(entry_id)
        if bucket != self._bucket:
            self.close()
            self.directory.mkdir(parents=True, exist_ok=True)
            self._file = open(self.directory / f"{bucket}{FILE_SUFFIX}", "ab")
            self._bucket = bucket
            self._bucket_end = bucket_end(bucket)
        self._file.write(format_line(entry_id, event_type, data))

    def seal_if_elapsed(self, now: float, grace_s: float) -> bool:
        """
        Close the open file if its bucket ended more than ``grace_s`` ago.

        Rotation is otherwise driven by entry arrival, so a stream that goes
        quiet would hold its file open indefinitely and pin the data on the
        trading host. Sealing on a timer bounds that wait for every stream
        regardless of how often it produces.

        Parameters
        ----------
        now : float
            Current epoch second.
        grace_s : float
            How long past the end of a bucket to keep accepting entries.

        Returns
        -------
        bool
            True if a file was sealed.
        """
        if self._file is None or now < self._bucket_end + grace_s:
            return False
        self.close()
        return True

    def flush(self) -> None:
        """Flush buffered lines to the operating system."""
        if self._file is not None:
            self._file.flush()

    def close(self) -> None:
        """Flush and close the current file, if any."""
        if self._file is not None:
            self._file.close()
            self._file = None
            self._bucket = None
            self._bucket_end = 0.0
