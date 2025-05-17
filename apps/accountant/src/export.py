import asyncio
import logging

import apps.shared.src.logging_config as logging_config
from apps.accountant.src.utils import (
    export_to_sql,
    load_pg_config,
    retrieve_and_prepare_orders,
    retrieve_and_prepare_trades,
)
from apps.shared.src.exchange_clients import authenticated_clients, load_clients
from apps.shared.src.utils import exchange_and_pair

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)




async def loop(pg_config: dict[str, str]):
    while True:
        for exchange_name, pair in exchange_and_pair:
            client = authenticated_clients[exchange_name]
            # logger.info()
            orders = await retrieve_and_prepare_orders(client, pair)
            # print(orders)
            if len(orders) != 0:
                export_to_sql(orders, pg_config, "orders")

            trades = await retrieve_and_prepare_trades(client, pair)
            # print(trades)
            if len(trades) != 0:
                export_to_sql(trades, pg_config, "trades")

        await asyncio.sleep(10)
        break


if __name__ == "__main__":
    pg_config = load_pg_config()
    asyncio.run(loop(pg_config))
