"""Module for tracking exchange balances."""

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
    """Fetch a balance from an exchange using a CCXT client.

    Parameters
    ----------
    client : CustomExchange
        A CCXT exchange instance
    redis : Redis
        A redis client instance

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
    """Watch changes to the balance on an exchange using a WS CCXT client.

    Parameters
    ----------
    client : CustomExchange
        A CCXT exchange instance
    redis : Redis
        A redis client instance

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
    """Fetch initial balance then watch changes for multiple clients.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        A dict mapping CCXT short id to a CCXT exchange client

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
