"""
This module is responsible for fetching and watching account balances from exchanges.
It updates the balances in Redis for other components to access.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.errors import NetworkError
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.structs import CustomExchange

# TODO:

logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def fetch_balance(client: CustomExchange, redis: Redis):
    """
    Fetch the initial balance from the exchange and update Redis.

    Parameters
    ----------
    client : CustomExchange
        The exchange client instance.
    redis : Redis
        The Redis client instance.

    Raises
    ------
    Exception
        If an unexpected error occurs during balance fetching.
    """
    try:
        balance = await client.fetch_balance()
        balance["timestamp"] = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        logger.debug(f"Balances on {client.name} are {balance}")
        await redis.set(f"balance-{client.id}", json.dumps(balance))

    except NetworkError as e:
        logger.error(
            f" Ignoring NetworkError in watch_balance for client {client.id}: {e}"
        )
    except Exception as e:
        logger.error(f"Error in watch_balance for client {client.id}: {e}")
        raise


async def watch_balance(client: CustomExchange, redis: Redis):
    """
    Continuously watch for balance updates from the exchange and update Redis.

    Parameters
    ----------
    client : CustomExchange
        The exchange client instance.
    redis : Redis
        The Redis client instance.

    Raises
    ------
    Exception
        If an unexpected error occurs during balance watching.
    """
    while True:
        try:
            balance = await client.watch_balance()
            logger.debug(f"Balances on {client.name} are {balance}")
            # Injecting timestamp
            balance["timestamp"] = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            await redis.set(f"balance-{client.id}", json.dumps(balance))

        except NetworkError as e:
            logger.error(
                f" Ignoring NetworkError in watch_balance for client {client.id}: {e}"
            )
        except Exception as e:
            logger.error(f"Error in watch_balance for client {client.id}: {e}")
            raise


async def main(clients: dict[str, CustomExchange]):
    """
    Main entry point for the balance watcher service.
    Initializes Redis connection and starts balance fetching/watching tasks for all clients.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        A dictionary of authenticated exchange clients.
    """
    pool = ConnectionPool(host="localhost", port=6379, db=0, max_connections=20)
    redis = Redis(decode_responses=True, connection_pool=pool)

    # Since watch only shows updates, I first pull data with fetch
    await asyncio.gather(
        *[fetch_balance(client, redis) for client in clients.values()],
    )
    try:
        await asyncio.gather(
            *[watch_balance(client, redis) for client in clients.values()],
        )
    finally:
        for client in clients.values():
            await client.close()


if __name__ == "__main__":
    asyncio.run(main(authenticated_clients))
