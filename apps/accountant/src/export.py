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
from apps.shared.src.errors import ExchangeError, RequestTimeout, NetworkError

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def loop(pg_config: dict[str, str]):
    while True:
        for exchange_name, pair in exchange_and_pair:
            client = authenticated_clients[exchange_name]
            try:
                orders = await retrieve_and_prepare_orders(client, pair)
                if len(orders) != 0:
                    export_to_sql(orders, pg_config, "orders", client.name)

                trades = await retrieve_and_prepare_trades(client, pair)
                if len(trades) != 0:
                    export_to_sql(trades, pg_config, "trades", client.name)

            except ExchangeError as e:
                logger.error(f"Exchange error: {e}")
            except RequestTimeout as e:
                logger.error(f"RequestTimeout error: {e}")
            except NetworkError as e:
                logger.error(f"Network error: {e}")

        await asyncio.sleep(10)


async def main(pg_config: dict[str, str]):
    await load_clients()
    await loop(pg_config)


if __name__ == "__main__":
    pg_config = load_pg_config()
    asyncio.run(main(pg_config))
