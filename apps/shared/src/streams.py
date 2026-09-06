"""Helpers for producing and enumerating Redis Streams.

``StreamPublisher`` is the one place that knows how an event becomes an
``XADD``: stream routing, optional namespace prefix, approximate trimming and
per-stream sequence numbers. Feed handlers use it; consumers read with
``XREAD`` directly.
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
