import asyncio
import logging
import time

from redis.asyncio import Redis

import apps.shared.src.logging_config as logging_config
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
# Could also check if liquidity is higher on maker exchange


async def fake_maker(redis: Redis, strategy: dict[str, str]):
    # Defining state

    watching: bool = True

    symbol: str = strategy["symbol"]
    maker: str = strategy["maker_exchange"]
    taker: str = strategy["taker_exchange"]
    spread: float = float(strategy["spread"])
    min_spread: float = float(strategy["min_spread"])
    min_size_usdt: float = float(strategy["min_size_usdt"])
    max_size_usdt: float = float(strategy["max_size_usdt"])
    liquidity_utilization: float = float(strategy["liquidity_utilization"])
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
        # Simple throttle

        time.sleep(0.01)

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

        logger.debug(f"Taker ({taker}) order book is \n {taker_order_book}")
        logger.debug(f"Maker ({maker}) order book is \n {maker_order_book}")

        taker_client_bids: list[list[float]] = taker_order_book["bids"]
        taker_client_asks: list[list[float]] = taker_order_book["asks"]
        maker_client_bids: list[list[float]] = maker_order_book["bids"]
        maker_client_asks: list[list[float]] = maker_order_book["asks"]

        best_bid_taker: float = taker_client_bids[0][0]
        best_ask_taker: float = taker_client_asks[0][0]
        best_bid_maker: float = maker_client_bids[0][0]
        best_ask_maker: float = maker_client_asks[0][0]

        min_size, max_size = min_max_usd_converter(
            best_bid_taker, min_size_usdt, max_size_usdt
        )

        target_sell_price: float = best_ask_taker * spread
        target_buy_price: float = best_bid_taker * spread

        # Sell order generation

        available_liquidity_taker = maker_order_sizer(
            target_sell_price,
            taker_client_asks,
            OrderSide.SELL,
            min_spread,
            max_size,
        )

        sell_size = available_liquidity_taker * liquidity_utilization

        if sell_size > min_size:
            logger.debug(f"Optimal sell size currently {sell_size}")

            order_side = OrderSide.BUY
            if sell_order is None:
                logger.debug("Sell does not exist yet.")
                sell_order = await generate_order_replace(
                    redis,
                    maker,
                    taker,
                    target_sell_price,
                    sell_size,
                    symbol,
                    order_side,
                    strategy,
                    sell_order_identifier,
                    bool(sell_order),
                )
            elif not within_percentage_range(sell_order["price"], sell_size, 0.01):
                logger.debug(
                    f"Order price of {sell_order.get('price')} too far from target {target_sell_price}, replacing."
                )
                sell_order = await generate_order_replace(
                    redis,
                    maker,
                    taker,
                    target_sell_price,
                    sell_size,
                    symbol,
                    order_side,
                    strategy,
                    sell_order_identifier,
                    bool(sell_order),
                )

                continue

            elif not within_percentage_range(sell_order["amount"], sell_size, 5):
                logger.debug(
                    f"Order amount of {sell_order.get('amount')} too far from target {sell_size}, replacing."
                )
                sell_order = await generate_order_replace(
                    redis,
                    maker,
                    taker,
                    target_sell_price,
                    sell_size,
                    symbol,
                    order_side,
                    strategy,
                    sell_order_identifier,
                    bool(sell_order),
                )
                continue

        else:
            if sell_order:
                await send_processor_cancellation(
                    redis, strategy, sell_order_identifier
                )
