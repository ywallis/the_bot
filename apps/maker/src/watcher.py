"""
This module monitors the order book of exchanges and updates Redis with the latest state.
It connects to exchanges via CCXT and streams order book updates.
"""

import asyncio
import json
import logging
from datetime import datetime

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.errors import (
    CancelledError,
    ExchangeClosedByUser,
    NetworkError,
    UnsubscribeError,
)
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.structs import CustomExchange
from apps.shared.src.utils import exchange_and_pair

# TODO:

logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def watch_ob(client: CustomExchange, ticker: str, redis: Redis):
    """
    Watch the order book for a specific symbol on an exchange and update Redis.

    Parameters
    ----------
    client : CustomExchange
        The exchange client instance.
    ticker : str
        The trading pair symbol (e.g., 'BTC/USDT').
    redis : Redis
        The Redis client instance.

    Raises
    ------
    Exception
        If an unexpected error occurs during watching.
    """
    while True:
        try:
            order_book = await client.watch_order_book(ticker)
            logger.debug(
                f"{datetime.now()} Bid {order_book['bids'][0][0]} and ask {order_book['asks'][0][0]} on {client.name}"
            )
            await redis.set(f"{ticker}-{client.id}", json.dumps(order_book))
        except NetworkError as e:
            logger.error(
                f" Ignoring NetworkError in watch_ob for client {client.id}: {e}"
            )
            await client.close()

        except UnsubscribeError as e:
            logger.error(
                f" Ignoring UnsubscribeError in watch_ob for client {client.id}: {e}"
            )
            await client.close()

        except ExchangeClosedByUser as e:
            logger.error(
                f" Ignoring ExchangeClosedByUser in watch_ob for client {client.id}: {e}"
            )
            await client.close()

        except CancelledError as e:
            logger.error(
                f" Ignoring CancelledError in watch_ob for client {client.id}: {e}"
            )
            await client.close()

        except Exception as e:
            logger.error(f"Error in watch_ob for client {client.id}: {e}")
            raise


async def main(config_tuples: set[tuple[str, str]], clients: dict[str, CustomExchange]):
    """
    Main entry point for the order book watcher service.

    Parameters
    ----------
    config_tuples : set[tuple[str, str]]
        A set of tuples containing (exchange_id, symbol) to watch.
    clients : dict[str, CustomExchange]
        A dictionary of authenticated exchange clients.
    """
    pool = ConnectionPool(host="localhost", port=6379, db=0, max_connections=20)
    redis = Redis(decode_responses=True, connection_pool=pool)

    try:
        await asyncio.gather(
            *[watch_ob(clients[tup[0]], tup[1], redis) for tup in config_tuples]
        )

    finally:
        for client in clients.values():
            await client.close()


if __name__ == "__main__":
    asyncio.run(main(exchange_and_pair, authenticated_clients))
