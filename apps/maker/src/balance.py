import asyncio
import json
import logging

from redis.asyncio import ConnectionPool, Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.exchange_clients import authenticated_clients
from apps.maker.src.structs import CustomExchange
from apps.maker.src.errors import NetworkError

# TODO:

logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def fetch_balance(client: CustomExchange, redis: Redis):
    try:
        balance = await client.fetch_balance()
        logger.debug(f"Balances on {client.name} are {balance}")
        await redis.set(f"balance-{client.id}", json.dumps(balance))

    except NetworkError as e:
        logger.error(f" Ignoring NetworkError in watch_balance for client {client.id}: {e}")
    except Exception as e:
        logger.error(f"Error in watch_balance for client {client.id}: {e}")
        raise

async def watch_balance(client: CustomExchange, redis: Redis):
    while True:
        try:
            balance = await client.watch_balance()
            logger.debug(f"Balances on {client.name} are {balance}")
            await redis.set(f"balance-{client.id}", json.dumps(balance))

        except NetworkError as e:
            logger.error(f" Ignoring NetworkError in watch_balance for client {client.id}: {e}")
        except Exception as e:
            logger.error(f"Error in watch_balance for client {client.id}: {e}")
            raise


async def main(clients: dict[str, CustomExchange]):
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
