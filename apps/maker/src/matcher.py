import asyncio

import copy
from datetime import datetime, timezone
import logging
import apps.maker.src.logging_config as logging_config
from apps.maker.src.structs import CustomExchange, LimitedSet
from apps.maker.src.errors import NetworkError
from apps.maker.src.exchange_clients import authenticated_clients
from apps.maker.src.utils import load_config

logging_config.setup_logging()
logger = logging.getLogger(__name__)

config = load_config()
pairs = set()

strategies = config.get("strategies")
if strategies is None:
    raise Exception("No strategy found")
for strategy in strategies:
    pairs.add(strategy["symbol"])


# TODO:
# Watch all orders.
# Parse OID
# If strat should be matched
# and coming from maker exchange
# match
async def watch_orders(client: CustomExchange, ticker: str):
    since = datetime.now(timezone.utc)
    timestamp = int(since.timestamp() * 1000)
    recently_processed_orders = LimitedSet(100)
    while True:
        try:
            orders = await client.watch_orders(ticker, since=timestamp)
            # orders_copy = copy.deepcopy(orders)

            # for order in orders_copy:
            for order in orders:
                logger.debug(f"Processing orders from {client.name}")
                logger.debug(order)

                order_copy = copy.deepcopy(order)

                if order_copy["status"] != "open":
                    if order_copy["filled"] != 0:
                        if order_copy.get("id") not in recently_processed_orders:
                            recently_processed_orders.add(order_copy.get("id"))
                            asyncio.create_task(
                                process_order_update(taker_client, order_copy)
                            )
                        else:
                            logger.warning(
                                f"The order no {order_copy['id']} tried getting matched multiple times."
                            )


        except Exception as e:
            logger.error(f"Error in client loop {e}")
            await client.close()


async def main(tickers: set[str], clients: dict[str, CustomExchange]):
    while True:
        await asyncio.gather(
            *[watch_orders(client) for client in clients.values() for ticker in tickers]
        )


if __name__ == "__main__":
    asyncio.run(main(pairs, authenticated_clients))
