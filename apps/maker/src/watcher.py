"""Feed handlers for public market data.

One process runs a ``watch_order_book`` loop per book subscription and a
``watch_trades`` loop per trade subscription. Each update is published as a
typed event on its Redis Stream. Order books are also written to the legacy
``{symbol}-{venue}`` snapshot key so polling strategies keep working during
the migration.
"""

import asyncio
import json
import logging
from typing import Any

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.ccxt_events import book_event_from_ccxt, trade_event_from_ccxt
from apps.shared.src.config import BOOK_FEED, TRADE_FEED, AppConfig, load_app_config
from apps.maker.src.structs import LimitedSet
from apps.shared.src.errors import (
    CancelledError,
    ChecksumError,
    ExchangeClosedByUser,
    NetworkError,
    UnsubscribeError,
)
from apps.shared.src.events import book_stream, now_ns, snapshot_key, trade_stream
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.streams import StreamPublisher
from apps.shared.src.structs import CustomExchange
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# Errors after which the next ``watch_*`` call simply resubscribes. CCXT
# raises these itself when it drops a subscription (for example on a book
# checksum failure) or when a sibling loop closed the shared client. The
# client must not be closed here: the book and trade loops of one venue share
# it, and closing it from one loop cancels the other, which would loop forever.
RESUBSCRIBE_ERRORS: tuple[type[BaseException], ...] = (
    UnsubscribeError,
    ChecksumError,
    ExchangeClosedByUser,
    CancelledError,
)
# Errors that mean the connection is dead and the client should be recycled.
RECONNECT_ERRORS: tuple[type[BaseException], ...] = (NetworkError,)
RETRY_DELAY_S = 0.5
# Trades remembered per stream to drop the cache CCXT replays on resubscribe.
TRADE_DEDUPE_SIZE = 2000


async def publish_book(
    redis: Redis,
    publisher: StreamPublisher,
    venue: str,
    symbol: str,
    order_book: dict[str, Any],
    ts_recv: int,
    depth: int,
) -> None:
    """
    Publish one order book update as a ``BookEvent`` and a legacy snapshot.

    Both writes go in one pipeline so they cost a single round trip.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    order_book : dict[str, Any]
        CCXT unified order book.
    ts_recv : int
        Local receive time in nanoseconds.
    depth : int
        Levels per side to publish.
    """
    event = book_event_from_ccxt(
        venue,
        symbol,
        order_book,
        seq=publisher.next_seq(book_stream(venue, symbol)),
        ts_recv=ts_recv,
        depth=depth,
    )
    async with redis.pipeline(transaction=False) as pipe:
        pipe.set(snapshot_key(venue, symbol), json.dumps(order_book))
        publisher.xadd(pipe, event)
        await pipe.execute()


def trade_key(trade: dict[str, Any]) -> tuple[Any, ...]:
    """
    Return the identity of a CCXT trade for deduplication.

    Parameters
    ----------
    trade : dict[str, Any]
        CCXT unified trade.

    Returns
    -------
    tuple[Any, ...]
        The venue trade id if present, otherwise timestamp, price and
        amount. Venues that send no ids (MEXC) fall back to the latter.
    """
    trade_id = trade.get("id")
    if trade_id is not None:
        return ("id", str(trade_id))
    return ("fields", trade.get("timestamp"), trade.get("price"), trade.get("amount"))


async def publish_trades(
    redis: Redis,
    publisher: StreamPublisher,
    venue: str,
    symbol: str,
    trades: list[dict[str, Any]],
    ts_recv: int,
    seen: LimitedSet | None = None,
) -> int:
    """
    Publish a batch of trades as one ``TradeEvent`` each.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    trades : list[dict[str, Any]]
        CCXT unified trades, oldest first.
    ts_recv : int
        Local receive time in nanoseconds, shared by the whole batch.
    seen : LimitedSet | None
        Recently published trade keys. Trades already in it are skipped and
        new ones are added. None disables deduplication.

    Returns
    -------
    int
        Number of trades published.
    """
    fresh: list[dict[str, Any]] = []
    for trade in trades:
        if seen is not None:
            key = trade_key(trade)
            if key in seen:
                continue
            seen.add(key)
        fresh.append(trade)
    if not fresh:
        return 0
    stream = trade_stream(venue, symbol)
    async with redis.pipeline(transaction=False) as pipe:
        for trade in fresh:
            event = trade_event_from_ccxt(
                venue, symbol, trade, seq=publisher.next_seq(stream), ts_recv=ts_recv
            )
            publisher.xadd(pipe, event)
        await pipe.execute()
    return len(fresh)


async def handle_feed_error(
    error: BaseException, client: CustomExchange, feed: str, ticker: str
) -> None:
    """
    Log a recoverable feed error and prepare the client for the next call.

    Parameters
    ----------
    error : BaseException
        The caught error.
    client : CustomExchange
        The exchange client.
    feed : str
        Feed name for the log line.
    ticker : str
        Symbol for the log line.

    Raises
    ------
    BaseException
        The same error, if it is not recoverable.
    """
    name = type(error).__name__
    if isinstance(error, RESUBSCRIBE_ERRORS):
        logger.warning(f"{name} in {feed} for {client.id} {ticker}, resubscribing: {error}")
    elif isinstance(error, RECONNECT_ERRORS):
        logger.error(f"{name} in {feed} for {client.id} {ticker}, reconnecting: {error}")
        await client.close()
    else:
        logger.error(f"Error in {feed} for {client.id} {ticker}: {error}")
        raise error
    await asyncio.sleep(RETRY_DELAY_S)


async def watch_ob(
    client: CustomExchange,
    ticker: str,
    redis: Redis,
    publisher: StreamPublisher,
    depth: int,
) -> None:
    """
    Watch the order book for a given ticker and publish every update.

    Parameters
    ----------
    client : CustomExchange
        The exchange client.
    ticker : str
        The trading pair symbol.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    depth : int
        Levels per side to publish.
    """
    while True:
        try:
            order_book = await client.watch_order_book(ticker)
            ts_recv = now_ns()
            logger.debug(
                f"Bid {order_book['bids'][0][0]} and ask {order_book['asks'][0][0]} "
                f"on {client.name}"
            )
            await publish_book(
                redis, publisher, client.id, ticker, order_book, ts_recv, depth
            )
        except BaseException as e:  # noqa: B036, narrowed in handle_feed_error
            await handle_feed_error(e, client, "watch_ob", ticker)


async def watch_trades(
    client: CustomExchange,
    ticker: str,
    redis: Redis,
    publisher: StreamPublisher,
) -> None:
    """
    Watch public trades for a given ticker and publish every new trade.

    Relies on CCXT's default ``newUpdates`` behaviour, where each call
    resolves with only the trades received since the previous call. After a
    resubscription some venues replay their cached trades, so recently
    published trades are remembered and dropped.

    Parameters
    ----------
    client : CustomExchange
        The exchange client.
    ticker : str
        The trading pair symbol.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    """
    seen = LimitedSet(TRADE_DEDUPE_SIZE)
    while True:
        try:
            trades = await client.watch_trades(ticker)
            ts_recv = now_ns()
            published = await publish_trades(
                redis, publisher, client.id, ticker, trades, ts_recv, seen
            )
            logger.debug(
                f"{published}/{len(trades)} new trades on {client.name} {ticker}"
            )
        except BaseException as e:  # noqa: B036, narrowed in handle_feed_error
            await handle_feed_error(e, client, "watch_trades", ticker)


def build_tasks(
    config: AppConfig,
    clients: dict[str, CustomExchange],
    redis: Redis,
    publisher: StreamPublisher,
    production_mode: bool,
) -> list[Any]:
    """
    Create one watch coroutine per subscribed feed.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    clients : dict[str, CustomExchange]
        Exchange clients keyed by venue id.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Shared publisher.
    production_mode : bool
        Which strategies' subscriptions to serve.

    Returns
    -------
    list[Any]
        Coroutines ready to be gathered.
    """
    depth = config.market_data.book_depth
    tasks: list[Any] = []
    for venue, symbol in sorted(config.feed_pairs(BOOK_FEED, production_mode)):
        tasks.append(watch_ob(clients[venue], symbol, redis, publisher, depth))
    for venue, symbol in sorted(config.feed_pairs(TRADE_FEED, production_mode)):
        tasks.append(watch_trades(clients[venue], symbol, redis, publisher))
    return tasks


async def main(config: AppConfig, clients: dict[str, CustomExchange]) -> None:
    """
    Run all configured feed handlers until one fails.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    clients : dict[str, CustomExchange]
        Exchange clients keyed by venue id.
    """
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    publisher = StreamPublisher(maxlen=config.market_data.stream_maxlen)

    tasks = build_tasks(config, clients, redis, publisher, production)
    logger.info(f"Starting {len(tasks)} feed handlers")
    try:
        await asyncio.gather(*tasks)
    finally:
        for client in clients.values():
            await client.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main(load_app_config(), authenticated_clients))
