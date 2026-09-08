"""Helpers for producing, enumerating and locating Redis Streams.

``StreamPublisher`` is the one place that knows how an event becomes an
``XADD``: stream routing, optional namespace prefix, approximate trimming and
per-stream sequence numbers. Feed handlers use it; consumers read with
``XREAD`` directly.

The rest of the module owns the on-disk layout of recordings, including
``sealed_files``, the invariant every tool that touches a recording
directory depends on. See ``docs/design/event-driven-framework.md`` section 7.
"""

from collections import defaultdict
from pathlib import Path
from typing import Any

from apps.shared.src.config import AppConfig, BOOK_FEED, TRADE_FEED
from apps.shared.src.events import (
    INTENTS_STREAM,
    LATENCY_STREAM,
    ORDER_EVENTS_STREAM,
    AnyEvent,
    balance_stream,
    book_stream,
    prefixed,
    stream_for,
    to_stream_fields,
    trade_stream,
)

# On-disk recording names. Recordings are uncompressed while the recorder
# may still append to them and compressed once sealed and shipped.
FILE_SUFFIX = ".jsonl"
COMPRESSED_SUFFIX = ".jsonl.zst"
# The id before any entry: an ``XREAD`` from it returns a stream from its start.
STREAM_START = "0-0"


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
        return STREAM_START
    return entry_id_str(entries[0][0])


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
        subscribed feed, one balance stream per venue and the order
        management streams.
    """
    streams: set[str] = set()
    for venue, symbol in config.feed_pairs(BOOK_FEED, production):
        streams.add(book_stream(venue, symbol))
    for venue, symbol in config.feed_pairs(TRADE_FEED, production):
        streams.add(trade_stream(venue, symbol))
    for venue_config in config.venues:
        streams.add(balance_stream(venue_config.id))
    streams.update({INTENTS_STREAM, ORDER_EVENTS_STREAM, LATENCY_STREAM})
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


def stream_path(root: Path, stream: str) -> Path:
    """
    Map a stream name to the directory its recordings live in.

    Each colon-separated component becomes a directory and slashes inside a
    component are replaced by dashes, so ``md:book:gate:ALPH/USDT`` becomes
    ``<root>/md/book/gate/ALPH-USDT``.

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
