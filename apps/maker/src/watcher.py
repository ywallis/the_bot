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
from apps.shared.src.errors import (
    CancelledError,
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

# CCXT errors that mean "reconnect and carry on" rather than "crash".
RECOVERABLE_ERRORS: tuple[type[BaseException], ...] = (
    NetworkError,
    UnsubscribeError,
    ExchangeClosedByUser,
    CancelledError,
)


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


async def publish_trades(
    redis: Redis,
    publisher: StreamPublisher,
    venue: str,
    symbol: str,
    trades: list[dict[str, Any]],
    ts_recv: int,
) -> None:
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
    """
    if not trades:
        return
    stream = trade_stream(venue, symbol)
    async with redis.pipeline(transaction=False) as pipe:
        for trade in trades:
            event = trade_event_from_ccxt(
                venue, symbol, trade, seq=publisher.next_seq(stream), ts_recv=ts_recv
            )
            publisher.xadd(pipe, event)
        await pipe.execute()


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
        except RECOVERABLE_ERRORS as e:
            logger.error(
                f"Ignoring {type(e).__name__} in watch_ob for client {client.id}: {e}"
            )
            await client.close()
        except Exception as e:
            logger.error(f"Error in watch_ob for client {client.id}: {e}")
            raise


async def watch_trades(
    client: CustomExchange,
    ticker: str,
    redis: Redis,
    publisher: StreamPublisher,
) -> None:
    """
    Watch public trades for a given ticker and publish every new trade.

    Relies on CCXT's default ``newUpdates`` behaviour, where each call
    resolves with only the trades received since the previous call.

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
    while True:
        try:
            trades = await client.watch_trades(ticker)
            ts_recv = now_ns()
            logger.debug(f"{len(trades)} trades on {client.name} {ticker}")
            await publish_trades(redis, publisher, client.id, ticker, trades, ts_recv)
        except RECOVERABLE_ERRORS as e:
            logger.error(
                f"Ignoring {type(e).__name__} in watch_trades for client "
                f"{client.id}: {e}"
            )
            await client.close()
        except Exception as e:
            logger.error(f"Error in watch_trades for client {client.id}: {e}")
            raise


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
