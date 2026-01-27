"""
This module monitors orders on one exchange and mirrors them (hedges) on another exchange.
It watches for order updates and sends matching orders to the message processor.
"""

import asyncio
import copy
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.constants import (
    MESSAGE_PROCESSOR_CHANNEL,
    REDIS_HOSTNAME,
    REDIS_PORT,
)
from apps.maker.src.enums import MessageType, OidComponent, OrderSide, OrderType
from apps.maker.src.structs import LimitedSet, OrderMessage
from apps.maker.src.utils import info_from_oid
from apps.shared.src.errors import NetworkError
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.structs import CustomExchange
from apps.shared.src.utils import load_config

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# TODO:
# - Make fee fetching dynamic


async def process_order_update(
    redis: Redis, origin_client_id: str, matching_client_id: str, order: dict[str, str]
):
    """
    Calculate quantity and price for the matching order and send it.

    Parameters
    ----------
    redis : Redis
        The Redis client instance.
    origin_client_id : str
        The ID of the exchange where the original order was filled.
    matching_client_id : str
        The ID of the exchange where the matching order should be placed.
    order : dict[str, str]
        The order details from the origin exchange.
    """
    native_asset_fee = {"bitget": 0.001, "gate": 0.001}
    quantity = float(order["filled"])
    price = float(order["price"])
    side = order["side"]

    if side == "buy":
        if origin_client_id in native_asset_fee:
            quantity = quantity * (1 - native_asset_fee[origin_client_id])

    if price * quantity <= 3:
        quantity = 3.1 / float(order["price"])

    if side == "sell":
        if matching_client_id in native_asset_fee:
            fee_ratio = 1 / (1 - native_asset_fee[matching_client_id])

            quantity = quantity * fee_ratio

    await send_match_order(redis, matching_client_id, order, quantity)


async def send_match_order(
    redis: Redis,
    matching_client_id: str,
    order: dict[str, str],
    adjusted_quantity: float,
):
    """
    Construct and publish the matching order message to Redis.

    Parameters
    ----------
    redis : Redis
        The Redis client instance.
    matching_client_id : str
        The ID of the exchange where the order will be placed.
    order : dict[str, str]
        The original order details.
    adjusted_quantity : float
        The calculated quantity for the matching order.
    """
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
    """
    Continuously watch for order updates on a specific exchange and ticker.

    Parameters
    ----------
    redis : Redis
        The Redis client instance.
    client : CustomExchange
        The exchange client to watch.
    ticker : str
        The trading pair symbol.
    should_match : dict[str, str]
        A mapping of strategy identifiers to the target matching exchange ID.

    Raises
    ------
    Exception
        If an unexpected error occurs during watching.
    """
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
                            redis=redis,
                            origin_client_id=client.id,
                            matching_client_id=should_match[strategy_identifier],
                            order=order_copy,
                        )
                    )
                else:
                    logger.warning(
                        f"The order no {order_copy['clientOrderId']} tried getting matched multiple times."
                    )


async def main(clients: dict[str, CustomExchange]):
    """
    Main entry point for the matcher service.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        A dictionary of authenticated exchange clients.
    """
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
