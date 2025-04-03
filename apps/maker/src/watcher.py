import asyncio
import json
import logging
from datetime import datetime

from redis.asyncio import ConnectionPool, Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.exchange_clients import authenticated_clients
from apps.maker.src.structs import CustomExchange
from apps.maker.src.utils import load_config

# TODO:

logging_config.setup_logging()
logger = logging.getLogger(__name__)

config = load_config()
pairs = set()

strategies = config.get("strategies")
if strategies is None:
    raise Exception("No strategy found")
for strategy in strategies:

    pairs.add(strategy["symbol"])


async def watch_ob(client: CustomExchange, ticker: str, redis: Redis):
    while True:
        order_book = await client.watch_order_book(ticker)
        logger.debug(
            f"{datetime.now()} Bid {order_book['bids'][0][0]} and ask {order_book['asks'][0][0]} on {client.name}"
        )
        await redis.set(f"{ticker}-{client.id}", json.dumps(order_book))


async def main(tickers: set[str], clients: dict[str, CustomExchange]):
    pool = ConnectionPool(host="localhost", port=6379, db=0, max_connections=20)
    redis = Redis(decode_responses=True, connection_pool=pool)

    await asyncio.gather(
        *[watch_ob(client, ticker, redis) for client in clients.values() for ticker in tickers],
    )


if __name__ == "__main__":
    asyncio.run(main(pairs, authenticated_clients))
