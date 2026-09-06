"""Feed handler for account balances.

Fetches each venue's balance once, then follows ``watch_balance`` updates.
Every snapshot is published as a ``BalanceEvent`` on ``acct:balance:{venue}``
and written to the legacy ``balance-{venue}`` key for polling strategies.
"""

import asyncio
import json
import logging
from typing import Any

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.ccxt_events import balance_event_from_ccxt
from apps.shared.src.config import AppConfig, load_app_config
from apps.shared.src.errors import NetworkError
from apps.shared.src.events import balance_stream, now_ns
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.streams import StreamPublisher
from apps.shared.src.structs import CustomExchange

logging_config.setup_logging()
logger = logging.getLogger(__name__)


def legacy_balance_key(venue: str) -> str:
    """
    Return the legacy snapshot key for a venue's balance.

    Parameters
    ----------
    venue : str
        CCXT short id.

    Returns
    -------
    str
        Redis key.
    """
    return f"balance-{venue}"


async def publish_balance(
    redis: Redis,
    publisher: StreamPublisher,
    venue: str,
    balance: dict[str, Any],
    ts_recv: int,
) -> None:
    """
    Publish one balance snapshot as a ``BalanceEvent`` and a legacy key.

    The legacy snapshot gets a millisecond ``timestamp`` injected, as
    existing strategies expect.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    venue : str
        CCXT short id.
    balance : dict[str, Any]
        CCXT unified balance.
    ts_recv : int
        Local receive time in nanoseconds.
    """
    balance["timestamp"] = ts_recv // 1_000_000
    event = balance_event_from_ccxt(
        venue, balance, seq=publisher.next_seq(balance_stream(venue)), ts_recv=ts_recv
    )
    async with redis.pipeline(transaction=False) as pipe:
        pipe.set(legacy_balance_key(venue), json.dumps(balance))
        publisher.xadd(pipe, event)
        await pipe.execute()


async def fetch_balance(
    client: CustomExchange, redis: Redis, publisher: StreamPublisher
) -> None:
    """
    Fetch a balance once and publish it.

    Parameters
    ----------
    client : CustomExchange
        A CCXT exchange instance.
    redis : Redis
        A redis client instance.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    """
    try:
        balance = await client.fetch_balance()
        ts_recv = now_ns()
        logger.debug(f"Balances on {client.name} are {balance}")
        await publish_balance(redis, publisher, client.id, balance, ts_recv)
    except NetworkError as e:
        logger.error(
            f"Ignoring NetworkError in fetch_balance for client {client.id}: {e}"
        )
    except Exception as e:
        logger.error(f"Error in fetch_balance for client {client.id}: {e}")
        raise


async def watch_balance(
    client: CustomExchange, redis: Redis, publisher: StreamPublisher
) -> None:
    """
    Follow balance updates over websocket and publish each one.

    Parameters
    ----------
    client : CustomExchange
        A CCXT exchange instance.
    redis : Redis
        A redis client instance.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    """
    while True:
        try:
            balance = await client.watch_balance()
            ts_recv = now_ns()
            logger.debug(f"Balances on {client.name} are {balance}")
            await publish_balance(redis, publisher, client.id, balance, ts_recv)
        except NetworkError as e:
            logger.error(
                f"Ignoring NetworkError in watch_balance for client {client.id}: {e}"
            )
        except Exception as e:
            logger.error(f"Error in watch_balance for client {client.id}: {e}")
            raise


async def main(config: AppConfig, clients: dict[str, CustomExchange]) -> None:
    """
    Fetch initial balances then follow updates for every client.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    clients : dict[str, CustomExchange]
        A dict mapping CCXT short id to a CCXT exchange client.
    """
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    publisher = StreamPublisher(maxlen=config.market_data.stream_maxlen)

    # watch_balance only delivers changes, so seed each venue with a fetch.
    await asyncio.gather(
        *[fetch_balance(client, redis, publisher) for client in clients.values()],
    )
    try:
        await asyncio.gather(
            *[watch_balance(client, redis, publisher) for client in clients.values()],
        )
    finally:
        for client in clients.values():
            await client.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main(load_app_config(), authenticated_clients))
