import asyncio
import json
import logging

from redis.asyncio import ConnectionPool, Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.exchange_clients import authenticated_clients
from apps.maker.src.structs import CustomExchange

# TODO:

logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def watch_balance(client: CustomExchange, redis: Redis):
    while True:
        balance = await client.watch_balance()
        logger.debug(balance)
        await redis.set(f"balance-{client.name}", json.dumps(balance))


async def main(clients: dict[str, CustomExchange]):
    print("Hi from main!")
    pool = ConnectionPool(host="localhost", port=6379, db=0, max_connections=20)
    redis = Redis(decode_responses=True, connection_pool=pool)

    await asyncio.gather(
        *[watch_balance(client, redis) for client in clients.values()],
    )


if __name__ == "__main__":
    asyncio.run(main(authenticated_clients))
