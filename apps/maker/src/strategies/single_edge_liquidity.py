import asyncio
import logging
from decimal import Decimal

# from datetime import UTC, datetime, timedelta
from redis.asyncio import ConnectionPool, Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.constants import REDIS_HOSTNAME, REDIS_PORT
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.strategies.utils import (
    check_if_solvent,
    maker_order_sizer,
    min_max_usd_converter,
    order_time,
    retrieve_ob_redis,
    send_processor_cancellation,
    send_processor_order,
)
from apps.maker.src.structs import CancellationMessage, OrderMessage
from apps.maker.src.utils import load_config

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# TODO:
# - Some kind of parser / strategy selector
# - A separation between strategy launcher and strategy itself?
# - Consider which throttling systems still make sense


async def single_edge_liquidity(redis: Redis, strategy: dict[str, str]):
    # Defining state

    watching: bool = True
    buy_exists: bool = False
    sell_exists: bool = False

    symbol: str = strategy["symbol"]
    strategy_identifier: str = strategy["identifier"]
    maker: str = strategy["maker_exchange"]
    taker: str = strategy["taker_exchange"]
    spread: float = float(strategy["spread"])
    min_size_usdt: float = float(strategy["min_size_usdt"])
    max_size_usdt: float = float(strategy["max_size_usdt"])
    min_size: float
    max_size: float

    # Cancels any open orders in case process has to restart

    await send_processor_cancellation(redis, strategy)

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

            if not sell_exists:
                logger.debug("Sell does not exist yet.")

                if await check_if_solvent(
                    redis,
                    taker,
                    maker,
                    best_ask_maker,
                    optimal_sell_size,
                    pair=symbol,
                ):
                    sell_order = OrderMessage(
                        kind=MessageType.ORDER,
                        strategy=f"{strategy_identifier}es",
                        exchange=maker,
                        id=f"t-{order_time()}_{strategy_identifier}es",
                        exchange_id="_",
                        pair=symbol,
                        side=OrderSide.SELL,
                        order_type=OrderType.REPLACE,
                        price=Decimal(best_ask_maker),
                        amount=Decimal(optimal_sell_size),
                    )

                    await send_processor_order(redis, sell_order)
                    sell_exists = True
                    logger.debug(f"Sell order was generated: {sell_order}")

            elif sell_order["price"] != best_ask_maker:
                logger.debug("Order no longer at bottom of asks, replacing.")

                if await check_if_solvent(
                    redis,
                    taker,
                    maker,
                    best_ask_maker,
                    optimal_sell_size,
                    symbol,
                ):
                    sell_order = OrderMessage(
                        kind=MessageType.ORDER,
                        strategy=f"{strategy_identifier}es",
                        exchange=maker,
                        id=f"t-{order_time()}_{strategy_identifier}es",
                        exchange_id="_",
                        pair=symbol,
                        side=OrderSide.SELL,
                        order_type=OrderType.REPLACE,
                        price=Decimal(best_ask_maker),
                        amount=Decimal(optimal_sell_size),
                    )

                    await send_processor_order(redis, sell_order)
                    sell_exists = True
                    logger.debug(f"Sell order was generated: {sell_order}")
                
                else:
                    logger.debug("Client may not be solvent, cancelling.")

                    await send_processor_cancellation(redis, strategy)
                    sell_exists = False

        else:
            if sell_exists:
                await send_processor_cancellation(redis, strategy)
                logger.debug(
                    "No more arb, cancellation was generated"
                )
                sell_exists = False

        # Buy side arbitrage

        if best_bid_taker >= best_bid_maker * spread:
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

            if not buy_exists:
                logger.debug("Buy does not exist yet.")

                if await check_if_solvent(
                    redis,
                    maker,
                    taker,
                    best_bid_maker,
                    optimal_buy_size,
                    pair=symbol,
                ):
                    buy_order = OrderMessage(
                        kind=MessageType.ORDER,
                        strategy=f"{strategy_identifier}eb",
                        exchange=maker,
                        id=f"t-{order_time()}_{strategy_identifier}eb",
                        exchange_id="_",
                        pair=symbol,
                        side=OrderSide.BUY,
                        order_type=OrderType.REPLACE,
                        price=Decimal(best_bid_maker),
                        amount=Decimal(optimal_buy_size),
                    )

                    await send_processor_order(redis, buy_order)
                    sell_exists = True
                    logger.debug(f"Sell order was generated: {buy_order}")

        else:
            if sell_exists:
                buy_cancellation: CancellationMessage = CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy=f"{strategy_identifier}es",
                    exchange=maker,
                    id="",
                    pair=symbol,
                )
                logger.debug(
                    f"No more arb, cancellation was generated: {buy_cancellation}"
                )
                sell_exists = False


async def main():
    pool = ConnectionPool(
        host=REDIS_HOSTNAME, port=REDIS_PORT, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    config = load_config()
    strategies = config.get("strategies")

    if not strategies:
        raise Exception("Could not find any valid strategy")

    await single_edge_liquidity(redis, strategies[0])


if __name__ == "__main__":
    asyncio.run(main())
