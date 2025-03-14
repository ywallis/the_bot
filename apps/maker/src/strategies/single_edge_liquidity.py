import asyncio
import logging
from datetime import UTC, datetime, timedelta

from redis.asyncio import ConnectionPool, Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.enums import OrderSide
from apps.maker.src.strategies.utils import maker_order_sizer, retrieve_ob_redis, min_max_usd_converter
from apps.maker.src.utils import load_config

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# TODO:
# - Look into how connection pools should be re-use between modules
# - Some kind of parser / strategy selector
# - A separation between strategy launcher and strategy itself?
# - Consider which throttling systems still make sense
# - Init triggers cancellation messages

async def maker(redis: Redis, strategy: dict[str, str]):
    # Defining state

    watching: bool = True
    buy_exists: bool = False
    sell_exists: bool = False
    buy_arbitrage: bool = True
    sell_arbitrage: bool = True

    symbol: str = strategy["symbol"]
    maker: str = strategy["maker_exchange"]
    taker: str = strategy["taker_exchange"]
    spread: float = float(strategy["spread"])
    sizing: float = float(strategy["sizing"])
    min_size_usdt: float = float(strategy["min_size_usdt"])
    max_size_usdt: float = float(strategy["max_size_usdt"])
    min_size: float
    max_size: float

    while watching:
        batch = asyncio.gather(
            retrieve_ob_redis(redis, f"{symbol}-{maker}"),
            retrieve_ob_redis(redis, f"{symbol}-{taker}"),
        )
        maker_order_book, taker_order_book = await batch
        if maker_order_book is None:
            logger.debug(f"Could not fetch order book for {maker}")
            await asyncio.sleep(1)
            continue

        if taker_order_book is None:
            logger.debug(f"Could not fetch order book for {taker}")
            await asyncio.sleep(1)
            continue

        current_time = datetime.now(UTC)
        taker_order_book_time = datetime.fromtimestamp(
            taker_order_book["timestamp"] / 1000, UTC
        )
        maker_order_book_time = datetime.fromtimestamp(
            maker_order_book["timestamp"] / 1000, UTC
        )
        logger.info(f"Taker ({taker}) order book is \n {taker_order_book}")
        logger.info(f"Maker ({maker}) order book is \n {maker_order_book}")

        # Do I really need this if take-take is not involved?

        if current_time - taker_order_book_time > timedelta(seconds=5):
            logger.info("Taker order book is stale, waiting for update")
            continue
        
        if current_time - maker_order_book_time > timedelta(seconds=5):
            logger.info("Maker order book is stale, waiting for update")
            continue

        taker_client_bids: list[list] = taker_order_book["bids"]
        taker_client_asks: list[list] = taker_order_book["asks"]
        maker_client_bids: list[list] = maker_order_book["bids"]
        maker_client_asks: list[list] = maker_order_book["asks"]

        best_bid_taker: float = taker_client_bids[0][0]
        best_ask_taker: float = taker_client_asks[0][0]
        best_bid_maker: float = maker_client_bids[0][0]
        best_ask_maker: float = maker_client_asks[0][0]

        min_size, max_size = min_max_usd_converter(best_bid_taker, min_size_usdt, max_size_usdt)
        # Sell side arbitrage

        if best_ask_maker >= best_ask_taker * spread:
            # Introducing parameter for speed control

            sell_arbitrage = True

            # Calculate the current optimal order size

            optimal_sell_size = maker_order_sizer(
                best_ask_maker,
                taker_client_asks,
                OrderSide.SELL,
                spread,
                max_size_usdt,
            )

            # logger.info(f"Optimal order size currently {optimal_sell_size}")

            # If the flag for an existing sell doesn't exist yet, create a sell order at the bottom ask.
            # Includes a custom clientOrderId to differentiate these orders from hanging taker order.

            if not sell_exists:
                logger.info("Sell does not exist yet.")

                # if await check_if_solvent(
                #     taker_client,
                #     maker_client,
                #     best_ask_maker,
                #     optimal_sell_size,
                #     pair=pair,
                # ):
                #     returned_sell_order = await create_and_return_order_abstraction(
                #         maker_client, best_ask_maker, optimal_sell_size, pair, "sell"
                #     )
                # sell_exists = True


async def main():
    print("Hi from main.")

    pool = ConnectionPool(host="localhost", port=6379, db=0, max_connections=20)
    redis = Redis(decode_responses=True, connection_pool=pool)
    config = load_config()
    strategies = config.get("strategies")

    if not strategies:
        raise Exception("Could not find any valid strategy")

    await maker(redis, strategies[0])


if __name__ == "__main__":
    asyncio.run(main())
