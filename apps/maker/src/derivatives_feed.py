"""Feed handler for derivatives positions and funding rates.

Publishes a ``PositionEvent`` per subscribed contract symbol on
``acct:position:{venue}`` and a ``FundingEvent`` per subscribed perpetual on
``md:funding:{venue}:{symbol}``. Which symbols are covered is read from the
subscriptions that list the ``position`` and ``funding`` feeds.

A position changes only when one of our own orders fills, so the position
feed polls ``fetch_positions`` on a slow backstop interval and fetches again
as soon as ``oms:events`` reports a fill on the venue, which is the moment a
consumer needs the fresh figure. A position is published when it changed and
otherwise once per heartbeat, so a consumer can tell a quiet position from a
dead feed. A symbol the venue reports nothing for is published as flat.

No configured venue pushes funding over a websocket and the rate changes once
per funding interval, so the funding feed polls ``fetch_funding_rate``.

A fetch that fails is logged and retried on the next round rather than
ending the process: the reconciler treats a stale position as unhealthy and
stops building, which is the safe reaction, while a dead feed process would
wind the whole system down. See ``docs/design/inventory-hedging.md``
section 5.
"""

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, cast

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.ccxt_events import position_event_from_ccxt
from apps.shared.src.config import (
    FUNDING_FEED,
    POSITION_FEED,
    AppConfig,
    load_app_config,
)
from apps.shared.src.errors import NetworkError
from apps.shared.src.events import (
    ORDER_EVENTS_STREAM,
    FundingEvent,
    OrderEvent,
    OrderState,
    PositionEvent,
    from_stream_fields,
    funding_stream,
    now_ns,
    position_stream,
)
from apps.shared.src.funding import parse_funding_rate
from apps.shared.src.streams import StreamPublisher, entry_id_str, stream_tail
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# An unchanged position is republished this often, so its consumers can
# tell a quiet position from a feed that stopped.
HEARTBEAT_S = 60.0

# Order states whose report can mean the position moved.
FILL_STATES: frozenset[OrderState] = frozenset(
    {
        OrderState.PARTIALLY_FILLED,
        OrderState.FILLED,
        OrderState.CANCELLED,
        OrderState.EXPIRED,
    }
)

# Pause after a failed round before the next, so a venue refusing every call
# is not hammered at the poll rate.
ERROR_BACKOFF_S = 5.0


def feed_targets(
    config: AppConfig, feed: str, production_mode: bool | None
) -> dict[str, set[str]]:
    """
    Return the symbols each venue publishes a derivatives feed for.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    feed : str
        ``position`` or ``funding``.
    production_mode : bool | None
        Which strategies' subscriptions to serve.

    Returns
    -------
    dict[str, set[str]]
        Contract symbols per venue id.
    """
    targets: dict[str, set[str]] = {}
    for venue, symbol in config.feed_pairs(feed, production_mode):
        targets.setdefault(venue, set()).add(symbol)
    return targets


def _fingerprint(event: PositionEvent) -> tuple[Any, ...]:
    """
    Return what makes two position snapshots different.

    Parameters
    ----------
    event : PositionEvent
        The snapshot.

    Returns
    -------
    tuple[Any, ...]
        Side, size and entry price. Mark price and unrealized profit move
        with the market and do not count as a change.
    """
    return (event.side, event.contracts, event.entry_price)


class PositionFeed:
    """
    Publish the positions of one venue's subscribed contract symbols.

    Attributes
    ----------
    client : Any
        The venue's authenticated CCXT client.
    venue : str
        Venue id.
    symbols : list[str]
        Contract symbols published, sorted.
    wake : asyncio.Event
        Set to fetch now rather than at the next poll.
    """

    def __init__(
        self,
        client: Any,
        venue: str,
        symbols: set[str],
        redis: Any,
        publisher: StreamPublisher,
        poll_s: float,
        heartbeat_s: float = HEARTBEAT_S,
    ) -> None:
        """
        Initialize the feed.

        Parameters
        ----------
        client : Any
            The venue's authenticated CCXT client, markets loaded.
        venue : str
            Venue id.
        symbols : set[str]
            Contract symbols to publish.
        redis : Any
            The Redis client.
        publisher : StreamPublisher
            Publisher holding sequence numbers and trimming settings.
        poll_s : float
            Seconds between fetches when no fill wakes the feed.
        heartbeat_s : float
            Seconds after which an unchanged position is republished.
        """
        self.client = client
        self.venue = venue
        self.symbols = sorted(symbols)
        self.redis = redis
        self.publisher = publisher
        self.poll_s = poll_s
        self.heartbeat_s = heartbeat_s
        self.wake = asyncio.Event()
        self._last: dict[str, tuple[Any, ...]] = {}
        self._published_at: dict[str, float] = {}

    def _contract_size(self, symbol: str) -> Decimal:
        market = self.client.markets.get(symbol, {}) if self.client.markets else {}
        size = market.get("contractSize")
        return Decimal(str(size)) if size is not None else Decimal(1)

    async def fetch_once(self) -> list[PositionEvent]:
        """
        Fetch every symbol's position and publish what changed or is due.

        Returns
        -------
        list[PositionEvent]
            The events published this round.
        """
        positions = await self.client.fetch_positions(self.symbols)
        ts_recv = now_ns()
        by_symbol: dict[str, dict[str, Any]] = {}
        for position in positions or []:
            symbol = position.get("symbol")
            if symbol not in self.symbols:
                continue
            # One-way mode holds one position per symbol; a venue that still
            # lists an empty side next to the real one must not hide it.
            contracts = position.get("contracts") or 0
            if symbol not in by_symbol or contracts:
                by_symbol[symbol] = position
        published: list[PositionEvent] = []
        now = time.monotonic()
        for symbol in self.symbols:
            stream = position_stream(self.venue)
            event = position_event_from_ccxt(
                self.venue,
                symbol,
                by_symbol.get(symbol),
                contract_size=self._contract_size(symbol),
                seq=0,
                ts_recv=ts_recv,
            )
            fingerprint = _fingerprint(event)
            changed = self._last.get(symbol) != fingerprint
            due = now - self._published_at.get(symbol, -1e18) >= self.heartbeat_s
            if not (changed or due):
                continue
            event.seq = self.publisher.next_seq(stream)
            await self.publisher.publish(self.redis, event)
            self._last[symbol] = fingerprint
            self._published_at[symbol] = now
            published.append(event)
            if changed:
                logger.info(
                    f"{self.venue} {symbol} position {event.side.value} "
                    f"{event.contracts} contracts ({event.base_amount} base)"
                )
        return published

    async def run(self) -> None:
        """Fetch on every poll and every wake-up until cancelled."""
        while True:
            try:
                await self.fetch_once()
            except NetworkError as error:
                logger.error(f"{self.venue} position fetch failed: {error}")
                await asyncio.sleep(ERROR_BACKOFF_S)
            except Exception as error:  # noqa: BLE001, a stale feed is handled downstream
                logger.error(f"{self.venue} position fetch failed: {error!r}")
                await asyncio.sleep(ERROR_BACKOFF_S)
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=self.poll_s)
            except TimeoutError:
                pass
            self.wake.clear()


def wakes_position(event: OrderEvent) -> bool:
    """
    Return whether an order event can mean a position moved.

    Parameters
    ----------
    event : OrderEvent
        The event.

    Returns
    -------
    bool
        True for a report of filled size.
    """
    return event.state in FILL_STATES and event.filled > 0


async def watch_fills(
    redis: Any, feeds: dict[str, PositionFeed], block_ms: int, cursor: str
) -> None:
    """
    Wake a venue's position feed whenever ``oms:events`` reports a fill on it.

    Parameters
    ----------
    redis : Any
        The Redis client.
    feeds : dict[str, PositionFeed]
        Position feeds by venue id.
    block_ms : int
        How long a read blocks when nothing arrives.
    cursor : str
        Entry id to read after, resolved once by the caller.
    """
    position = cursor
    while True:
        response = await redis.xread(
            {ORDER_EVENTS_STREAM: position}, count=100, block=block_ms
        )
        for _stream, entries in cast(list[Any], response or []):
            for entry_id, fields in entries:
                position = entry_id_str(entry_id)
                try:
                    event = from_stream_fields(fields)
                except Exception:  # noqa: BLE001, not ours to judge
                    continue
                if not isinstance(event, OrderEvent):
                    continue
                feed = feeds.get(event.venue)
                if feed is not None and wakes_position(event):
                    feed.wake.set()


async def publish_funding_once(
    client: Any,
    venue: str,
    symbols: list[str],
    redis: Any,
    publisher: StreamPublisher,
) -> list[FundingEvent]:
    """
    Fetch and publish the funding rate of every symbol once.

    Parameters
    ----------
    client : Any
        The venue's CCXT client.
    venue : str
        Venue id.
    symbols : list[str]
        Perpetual symbols.
    redis : Any
        The Redis client.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.

    Returns
    -------
    list[FundingEvent]
        What was published. A symbol whose fetch fails or carries no rate
        is logged and skipped.
    """
    published: list[FundingEvent] = []
    for symbol in symbols:
        try:
            structure = await client.fetch_funding_rate(symbol)
        except Exception as error:  # noqa: BLE001, next round retries
            logger.error(f"{venue} {symbol} funding fetch failed: {error!r}")
            continue
        rate = parse_funding_rate(structure)
        if rate is None:
            logger.warning(f"{venue} {symbol} reported no funding rate")
            continue
        event = FundingEvent(
            ts_recv=now_ns(),
            venue=venue,
            symbol=symbol,
            seq=publisher.next_seq(funding_stream(venue, symbol)),
            funding_rate=rate.rate,
            next_funding_ts=rate.next_funding_ms,
            interval_s=rate.interval_s,
            mark_price=rate.mark_price,
            index_price=rate.index_price,
        )
        await publisher.publish(redis, event)
        published.append(event)
    return published


async def watch_funding(
    client: Any,
    venue: str,
    symbols: set[str],
    redis: Any,
    publisher: StreamPublisher,
    poll_s: float,
) -> None:
    """
    Publish every symbol's funding rate on an interval until cancelled.

    Parameters
    ----------
    client : Any
        The venue's CCXT client.
    venue : str
        Venue id.
    symbols : set[str]
        Perpetual symbols.
    redis : Any
        The Redis client.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    poll_s : float
        Seconds between rounds.
    """
    ordered = sorted(symbols)
    while True:
        await publish_funding_once(client, venue, ordered, redis, publisher)
        await asyncio.sleep(poll_s)


async def main(config: AppConfig, clients: dict[str, Any]) -> None:
    """
    Run the position and funding feeds of every subscribed venue.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    clients : dict[str, Any]
        Authenticated CCXT clients keyed by venue id.
    """
    positions = feed_targets(config, POSITION_FEED, production)
    funding = feed_targets(config, FUNDING_FEED, production)
    venues = sorted(set(positions) | set(funding))
    if not venues:
        logger.info("No position or funding subscriptions; nothing to do")
        return
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=10
    )
    redis = Redis(connection_pool=pool)
    publisher = StreamPublisher(maxlen=config.market_data.stream_maxlen)
    cursor = await stream_tail(redis, ORDER_EVENTS_STREAM)
    try:
        for venue in venues:
            await clients[venue].load_markets()
        feeds = {
            venue: PositionFeed(
                clients[venue],
                venue,
                symbols,
                redis,
                publisher,
                config.derivatives.position_poll_s,
            )
            for venue, symbols in positions.items()
        }
        tasks: list[Any] = [feed.run() for feed in feeds.values()]
        tasks += [
            watch_funding(
                clients[venue],
                venue,
                symbols,
                redis,
                publisher,
                config.derivatives.funding_poll_s,
            )
            for venue, symbols in funding.items()
        ]
        if feeds:
            tasks.append(watch_fills(redis, feeds, config.oms.block_ms, cursor))
        logger.info(f"Positions on {sorted(positions)}, funding on {sorted(funding)}")
        await asyncio.gather(*tasks)
    finally:
        for venue in venues:
            await clients[venue].close()
        await redis.aclose()


if __name__ == "__main__":
    from apps.shared.src.exchange_clients import authenticated_clients

    asyncio.run(main(load_app_config(), authenticated_clients))
