import asyncio
import copy
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

from redis.asyncio import ConnectionPool, Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.constants import (
    MESSAGE_PROCESSOR_CHANNEL,
    REDIS_HOSTNAME,
    REDIS_PORT,
)
from apps.maker.src.enums import MessageType, OidComponent, OrderSide, OrderType
from apps.maker.src.errors import NetworkError
from apps.maker.src.exchange_clients import authenticated_clients
from apps.maker.src.structs import CustomExchange, LimitedSet, OrderMessage
from apps.maker.src.utils import info_from_oid, load_config

logging_config.setup_logging()
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)

# TODO:
# - Make fee fetching dynamic


async def process_order_update(
    redis: Redis, matching_client_id: str, order: dict[str, str]
):
    gate_fee = 0.001
    quantity = float(order["filled"])
    price = float(order["price"])
    side = order["side"]

    if price * quantity <= 3:
        quantity = 3.1 / float(order["price"])

    # if side == OrderSide.SELL:
    if side == "sell":
        if matching_client_id == "gate":
            fee_ratio = 1 / (1 - gate_fee)

            quantity = round(quantity * fee_ratio, 3)
            print("QQQQ", quantity)

    await send_match_order(redis, matching_client_id, order, quantity)


async def send_match_order(
    redis: Redis,
    matching_client_id: str,
    order: dict[str, str],
    adjusted_quantity: float,
):
    if order["side"] == "sell":
        side = OrderSide.BUY
    else:
        side = OrderSide.SELL

    matching_order = OrderMessage(
        kind=MessageType.ORDER,
        strategy="matching",
        exchange=matching_client_id,
        id=order["clientOrderId"],
        exchange_id="_",
        pair=order["symbol"],
        side=side,
        order_type=OrderType.MARKET,
        price=Decimal(order["price"]),
        amount=Decimal(adjusted_quantity).quantize(Decimal("0.0000")),
    )

    flattened = json.dumps(dict(matching_order), default=str)
    await redis.publish(MESSAGE_PROCESSOR_CHANNEL, flattened)
    logger.info(f"Matching order was sent: {matching_order}")


async def watch_orders(
    redis: Redis, client: CustomExchange, ticker: str, should_match: dict[str, str]
):
    since = datetime.now(timezone.utc)
    timestamp = int(since.timestamp() * 1000)
    recently_processed_orders = LimitedSet(100)
    while True:
        try:
            orders: list[dict[str, str]] = await client.watch_orders(
                ticker, since=timestamp
            )
            orders_copy = copy.deepcopy(orders)

        except NetworkError as e:
            logger.error(
                f" Ignoring NetworkError in watch_balance for client {client.id}: {e}"
            )
        except Exception as e:
            logger.error(f"Error in watch_balance for client {client.id}: {e}")
            raise

        else:
            for order in orders_copy:
                logger.debug(f"Processing orders from {client.name}")
                logger.debug(order)

                order_copy = copy.deepcopy(order)

                if order_copy["status"] == "open" or order_copy["filled"] == 0:
                    logger.debug(f"Order did not meet fill conditions: {order_copy}")
                    continue

                strategy_identifier = info_from_oid(
                    order_copy["clientOrderId"], OidComponent.STRATEGY
                )
                if strategy_identifier not in should_match:
                    logger.debug(f"Order did not meet match conditions: {order_copy}")
                    continue
                if should_match[strategy_identifier] == client.id:
                    logger.debug("Cannot match self.")
                    continue
                if order_copy.get("clientOrderId") not in recently_processed_orders:
                    recently_processed_orders.add(order_copy.get("clientOrderId"))
                    asyncio.create_task(
                        process_order_update(
                            redis, should_match[strategy_identifier], order_copy
                        )
                    )
                else:
                    logger.warning(
                        f"The order no {order_copy['clientOrderId']} tried getting matched multiple times."
                    )


async def main(clients: dict[str, CustomExchange]):
    config = load_config()
    exchange_and_pair: set[tuple[str, str]] = set()

    should_match = {}

    strategies = config.get("strategies")
    if strategies is None:
        raise Exception("No strategy found")
    for strategy in strategies:
        # pairs.add(strategy["symbol"])
        if strategy.get("should_match"):
            should_match[strategy["identifier"]] = strategy["taker_exchange"]
            exchange_and_pair.add((strategy["maker_exchange"], strategy["symbol"]))
    logger.debug(f"Should match: {should_match}")
    logger.debug(f"Exchange and symbol: {exchange_and_pair}")
    # Init Redis
    pool = ConnectionPool(
        host=REDIS_HOSTNAME, port=REDIS_PORT, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    while True:
        try:
            await asyncio.gather(
                *[
                    watch_orders(redis, clients[tup[0]], tup[1], should_match)
                    for tup in exchange_and_pair
                ]
            )
        finally:
            for client in clients.values():
                await client.close()


if __name__ == "__main__":
    asyncio.run(main(authenticated_clients))
