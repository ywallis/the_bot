import asyncio
import logging

# from datetime import UTC, datetime, timedelta
from redis.asyncio import Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.enums import OrderSide
from apps.maker.src.strategies.utils import (
    generate_order_replace,
    maker_order_sizer,
    min_max_usd_converter,
    retrieve_ob_redis,
    send_processor_cancellation,
    within_percentage_range,
)
from apps.maker.src.structs import OrderMessage

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# TODO:
# - Consider which throttling systems still make sense
# - Make sure all values put into redis follow the format I want


async def single_edge_liquidity(redis: Redis, strategy: dict[str, str]):
    # Defining state

    watching: bool = True

    symbol: str = strategy["symbol"]
    maker: str = strategy["maker_exchange"]
    taker: str = strategy["taker_exchange"]
    spread: float = float(strategy["spread"])
    min_size_usdt: float = float(strategy["min_size_usdt"])
    max_size_usdt: float = float(strategy["max_size_usdt"])
    min_size: float
    max_size: float
    order_side: OrderSide | None = None
    sell_order: OrderMessage | None = None
    buy_order: OrderMessage | None = None
    sell_order_identifier: str = "es"
    buy_order_identifier: str = "eb"

    # Cancels any open orders in case process has to restart

    await send_processor_cancellation(redis, strategy, sell_order_identifier)
    await send_processor_cancellation(redis, strategy, buy_order_identifier)

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

        # current_time = datetime.now(UTC)
        # taker_order_book_time = datetime.fromtimestamp(
        #     taker_order_book["timestamp"] / 1000, UTC
        # )
        # maker_order_book_time = datetime.fromtimestamp(
        #     maker_order_book["timestamp"] / 1000, UTC
        # )
        logger.debug(f"Taker ({taker}) order book is \n {taker_order_book}")
        logger.debug(f"Maker ({maker}) order book is \n {maker_order_book}")

        # Do I really need this if take-take is not involved?

        # if current_time - taker_order_book_time > timedelta(seconds=5):
        #     logger.info("Taker order book is stale, waiting for update")
        #     continue
        #
        # if current_time - maker_order_book_time > timedelta(seconds=5):
        #     logger.info("Maker order book is stale, waiting for update")
        #     continue

        taker_client_bids: list[list] = taker_order_book["bids"]
        taker_client_asks: list[list] = taker_order_book["asks"]
        maker_client_bids: list[list] = maker_order_book["bids"]
        maker_client_asks: list[list] = maker_order_book["asks"]

        best_bid_taker: float = taker_client_bids[0][0]
        best_ask_taker: float = taker_client_asks[0][0]
        best_bid_maker: float = maker_client_bids[0][0]
        best_ask_maker: float = maker_client_asks[0][0]

        min_size, max_size = min_max_usd_converter(
            best_bid_taker, min_size_usdt, max_size_usdt
        )

        # Sell side arbitrage

        if best_ask_maker >= best_ask_taker * spread:
            order_side = OrderSide.SELL
            # Calculate the current optimal order size

            optimal_sell_size = maker_order_sizer(
                best_ask_maker,
                taker_client_asks,
                OrderSide.SELL,
                spread,
                max_size,
                min_size,
            )

            logger.debug(f"Optimal sell size currently {optimal_sell_size}")

            # If the flag for an existing sell is false, create a sell order at the bottom ask.

            if sell_order is None:
                logger.debug("Sell does not exist yet.")

                sell_order = await generate_order_replace(
                    redis,
                    maker,
                    taker,
                    best_ask_maker,
                    optimal_sell_size,
                    symbol,
                    order_side,
                    strategy,
                    sell_order_identifier,
                )

            elif sell_order["price"] != best_ask_maker:
                logger.debug("Order no longer at bottom of asks, replacing.")

                sell_order = await generate_order_replace(
                    redis,
                    maker,
                    taker,
                    best_ask_maker,
                    optimal_sell_size,
                    symbol,
                    order_side,
                    strategy,
                    sell_order_identifier,
                )

            elif not within_percentage_range(
                sell_order["amount"], optimal_sell_size, 20
            ):
                logger.debug(
                    "Order no longer within acceptable size range, cancelling."
                )

                sell_order = await send_processor_cancellation(
                    redis, strategy, sell_order_identifier
                )

        else:
            # Arb conditions are gone
            if sell_order:
                sell_order = await send_processor_cancellation(
                    redis, strategy, sell_order_identifier
                )
                logger.debug("No more arb, cancellation was generated")

        # Buy side arbitrage

        if best_bid_taker >= best_bid_maker * spread:
            order_side = OrderSide.BUY
            # Calculate the current optimal order size

            optimal_buy_size = maker_order_sizer(
                best_bid_maker,
                taker_client_bids,
                OrderSide.BUY,
                spread,
                max_size,
                min_size,
            )

            logger.debug(f"Optimal buy size currently {optimal_buy_size}")

            # If the flag for an existing sell is false, create a sell order at the bottom ask.

            if buy_order is None:
                logger.debug("Buy does not exist yet.")

                buy_order = await generate_order_replace(
                    redis,
                    maker,
                    taker,
                    best_bid_maker,
                    optimal_buy_size,
                    symbol,
                    order_side,
                    strategy,
                    buy_order_identifier,
                )

            elif buy_order["price"] != best_bid_maker:
                logger.debug("Order no longer at top of bids, replacing.")

                buy_order = await generate_order_replace(
                    redis,
                    maker,
                    taker,
                    best_bid_maker,
                    optimal_buy_size,
                    symbol,
                    order_side,
                    strategy,
                    buy_order_identifier,
                )

            elif not within_percentage_range(buy_order["amount"], optimal_buy_size, 20):
                logger.debug(
                    "Order no longer within acceptable size range, cancelling."
                )

                buy_order = await send_processor_cancellation(
                    redis, strategy, buy_order_identifier
                )
        else:
            # Arb conditions are gone
            if buy_order:
                buy_order = await send_processor_cancellation(
                    redis, strategy, buy_order_identifier
                )
                logger.debug("No more arb, cancellation was generated")
