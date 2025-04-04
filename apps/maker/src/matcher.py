import asyncio

import copy
from datetime import datetime, timezone
import logging
from apps.maker.src.enums import OidComponent
import apps.maker.src.logging_config as logging_config
from apps.maker.src.structs import CustomExchange, LimitedSet
from apps.maker.src.exchange_clients import authenticated_clients
from apps.maker.src.utils import info_from_oid, load_config

logging_config.setup_logging()
logger = logging.getLogger(__name__)

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

# TODO:
# - Limit scope to clients that are an active part of the strategy?

async def process_order_update(matching_client_id, order: dict[str, str]):
    pass

async def watch_orders(client: CustomExchange, ticker: str, should_match: dict[str, str]):
    since = datetime.now(timezone.utc)
    timestamp = int(since.timestamp() * 1000)
    recently_processed_orders = LimitedSet(100)
    while True:
        orders: list[dict[str, str]] = await client.watch_orders(ticker, since=timestamp)
        orders_copy = copy.deepcopy(orders)

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
            if order_copy.get("id") not in recently_processed_orders:
                recently_processed_orders.add(order_copy.get("id"))
                asyncio.create_task(
                    process_order_update(should_match[strategy_identifier], order_copy)
                )
            else:
                logger.warning(
                    f"The order no {order_copy['id']} tried getting matched multiple times."
                )


async def main(config_tuples: set[tuple[str, str]], clients: dict[str, CustomExchange]):
    while True:
        await asyncio.gather(
            *[watch_orders(clients[tup[0]], tup[1], should_match) for tup in config_tuples]
        )


if __name__ == "__main__":
    asyncio.run(main(exchange_and_pair, authenticated_clients))
