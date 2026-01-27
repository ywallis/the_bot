"""
This module identifies and processes "orphan" orders.
Orphans are orders that exist in the database or strategy logs but might not be fully reconciled.
It queries the database for potential orphans and attempts to fetch their details from the exchange
to ensure data consistency.
"""

import asyncio
import logging

from pandas import DataFrame

import apps.shared.src.logging_config as logging_config
from apps.accountant.src.sql_connector import QueryLoader, send_sql_query
from apps.accountant.src.utils import (
    export_to_sql,
    load_pg_config,
    prepare_items_for_pg,
)
from apps.shared.src.errors import ExchangeError
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.structs import CustomExchange

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def get_orphans(clients: dict[str, CustomExchange]):
    """
    Find and process orphan orders.

    Queries the database for orders flagged as orphans (e.g., missing details),
    fetches their status from the respective exchange, and updates the database.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        A dictionary of authenticated exchange clients.

    Raises
    ------
    Exception
        If there are errors loading queries, database data, or parsing results.
    """
    query_loader = QueryLoader()
    query_loader.load_queries()
    orphans = query_loader.get_query("find_orphans")
    if orphans is None:
        raise Exception("Error loading orphans query")

    pg_config = load_pg_config()

    data = send_sql_query(pg_config, orphans, raw=False)

    if data is None:
        logger.info("No orphans found")
        return

    if not isinstance(data, DataFrame):
        raise Exception("Error loading dataframe")

    for index, row in data.iterrows():
        for client in clients.values():
            if row["exchange"] == client.name:
                logger.info(
                    f"Identified order matching client {client.name}, {row['order_id']}"
                )
                symbol = row["symbol"]
                if not isinstance(symbol, str):
                    raise Exception("Error in returned orphan data")

                id = row["order_id"]
                if not isinstance(id, str):
                    raise Exception("Error in returned orphan data")
                try:
                    order = await client.fetch_order(symbol=symbol, id=id)

                    logger.debug(f"Order was fetched for orphan: {order}")
                    prepared_order = prepare_items_for_pg(client, order)
                    export_to_sql(prepared_order, pg_config, "orders", client.name)
                except ExchangeError as e:
                    logger.error(f"Could not fetch order: {e}")

                # Break to stop looking if client found
                break


async def main():
    """
    Main entry point for the orphanage service.
    """
    await get_orphans(authenticated_clients)
    for client in authenticated_clients.values():
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
