import asyncio
import json
import logging
from datetime import datetime

from redis.asyncio import ConnectionPool, Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.exchange_clients import authenticated_clients
from apps.maker.src.structs import CustomExchange

# TODO: 
# - Check if ccxt pro includes all async functions
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def watch_ob(client: CustomExchange, ticker: str, redis: Redis):
    while True:
        order_book = await client.watch_order_book(ticker)
        logger.debug(
            f"{datetime.now()} Bid {order_book['bids'][0][0]} and ask {order_book['asks'][0][0]} on {client.name}"
        )
        await redis.set(f"{ticker}-{client.name}", json.dumps(order_book))


async def main(ticker: str, clients: dict[str, CustomExchange]):
    print("Hi from main!")
    pool = ConnectionPool(host="localhost", port=6379, db=0, max_connections=20)
    redis = Redis(decode_responses=True, connection_pool=pool)

    asyncio.gather(*[watch_ob(client, ticker, redis) for client in clients.values()])

if __name__ == "__main__":
    asyncio.run(main("ALPH/USDT", authenticated_clients))
