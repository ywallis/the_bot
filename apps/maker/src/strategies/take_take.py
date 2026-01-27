"""
This module implements the 'take_take' strategy.
It scans two exchanges for arbitrage opportunities where the bid on one exchange
overlaps with the ask on another, and executes a taker order on both sides simultaneously.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta, timezone

from redis.asyncio import Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.strategies.utils import (
    generate_take_take_order,
    min_max_usd_converter,
    ob_matcher,
    retrieve_ob_redis,
)

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# TODO:


async def take_take(redis: Redis, strategy: dict[str, str]):
    """
    Execute the take-take strategy.

    Continuously monitors order books on two exchanges. If a spread arbitrage opportunity is found,
    it calculates the optimal order size and executes a batch of taker orders on both exchanges.

    Parameters
    ----------
    redis : Redis
        The Redis client instance.
    strategy : dict[str, str]
        The strategy configuration dictionary.
    """
    # Defining state

    watching: bool = True

    refresh_speed: float = float(strategy["refresh_speed"])
    symbol: str = strategy["symbol"]
    exchange_1: str = strategy["exchange_1"]
    exchange_2: str = strategy["exchange_2"]
    spread: float = float(strategy["spread"])
    sizing: float = float(strategy["sizing"])
    min_size_usdt: float = float(strategy["min_size_usdt"])
    max_size_usdt: float = float(strategy["max_size_usdt"])
    joint_nonce: str = ""
    last_order_timestamp: int = 0
    min_size: float
    max_size: float

    while watching:
        # Simple throttle

        await asyncio.sleep(refresh_speed)

        batch = asyncio.gather(
            retrieve_ob_redis(redis, f"{symbol}-{exchange_1}"),
            retrieve_ob_redis(redis, f"{symbol}-{exchange_2}"),
        )
        e1_order_book, e2_order_book = await batch
        if e1_order_book is None:
            logger.debug(f"Could not fetch order book for {exchange_1}")
            await asyncio.sleep(1)
            continue

        if e2_order_book is None:
            logger.debug(f"Could not fetch order book for {exchange_2}")
            await asyncio.sleep(1)
            continue

        current_time = datetime.now(UTC)

        e1_order_book_time = datetime.fromtimestamp(
            e1_order_book["timestamp"] / 1000, UTC
        )
        e2_order_book_time = datetime.fromtimestamp(
            e2_order_book["timestamp"] / 1000, UTC
        )
        logger.debug(f"{exchange_1} order book is \n {e1_order_book}")
        logger.debug(f"{exchange_2} order book is \n {e2_order_book}")

        if current_time - e1_order_book_time > timedelta(seconds=15):
            logger.debug("Taker order book is stale, waiting for update")
            continue

        if current_time - e2_order_book_time > timedelta(seconds=15):
            logger.debug("Maker order book is stale, waiting for update")
            continue

        # Double order prevention
        if e1_order_book["timestamp"] + e2_order_book["timestamp"] == joint_nonce:
            logger.debug("Order book combination has not changed since last cycle.")
            continue
        else:
            joint_nonce = str(e1_order_book["timestamp"]) + str(
                e2_order_book["timestamp"]
            )

        if int(e1_order_book["timestamp"]) < last_order_timestamp:
            logger.debug(
                f"{exchange_1} order book has not been refreshed since last order"
            )
            continue
        if int(e2_order_book["timestamp"]) < last_order_timestamp:
            logger.debug(
                f"{exchange_2} order book has not been refreshed since last order"
            )
            continue

        e1_bids: list[list] = e1_order_book["bids"]
        e1_asks: list[list] = e1_order_book["asks"]
        e2_bids: list[list] = e2_order_book["bids"]
        e2_asks: list[list] = e2_order_book["asks"]

        e1_best_bid: float = e1_bids[0][0]
        e1_best_ask: float = e1_asks[0][0]
        e2_best_bid: float = e2_bids[0][0]
        e2_best_ask: float = e2_asks[0][0]

        min_size, max_size = min_max_usd_converter(
            e1_best_bid, min_size_usdt, max_size_usdt
        )

        if e1_best_bid >= e2_best_ask * spread:
            sell_exchange = exchange_1
            buy_exchange = exchange_2
            match = ob_matcher(e1_bids, e2_asks, spread, sizing, max_size, min_size)
            if match is None:
                continue
            target_ask, target_bid, target_order_size = match

        elif e2_best_bid >= e1_best_ask * spread:
            sell_exchange = exchange_2
            buy_exchange = exchange_1
            match = ob_matcher(e2_bids, e1_asks, spread, sizing, max_size, min_size)
            if match is None:
                continue
            target_ask, target_bid, target_order_size = match

        else:
            continue

        await generate_take_take_order(
            redis,
            buy_exchange,
            sell_exchange,
            target_ask,
            target_bid,
            target_order_size,
            symbol,
            strategy,
            "tt",
            last_order_timestamp,
        )
        last_order_timestamp = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
