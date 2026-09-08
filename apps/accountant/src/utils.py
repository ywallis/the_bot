"""Utility functions for the accountant application."""

import asyncio
import logging
import os
from copy import deepcopy

from pandas import DataFrame
import psycopg
from dotenv import load_dotenv
from psycopg import sql

import apps.shared.src.logging_config as logging_config
from apps.accountant.src.structs import UnaddressedImbalance
from apps.shared.src.structs import CustomExchange, ccxtItem

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


def load_pg_config() -> dict[str, str]:
    """
    Load PostgreSQL configuration from environment variables.

    Returns
    -------
    dict[str, str]
        The configuration dictionary.
    """
    config: dict[str, str] = {}

    _ = load_dotenv()
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    if POSTGRES_DB is None:
        raise Exception("DB Credentials not found")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    if POSTGRES_USER is None:
        raise Exception("DB Credentials not found")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    if POSTGRES_PASSWORD is None:
        raise Exception("DB Credentials not found")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    if POSTGRES_PORT is None:
        raise Exception("DB Credentials not found")
    config["POSTGRES_DB"] = POSTGRES_DB
    config["POSTGRES_USER"] = POSTGRES_USER
    config["POSTGRES_PASSWORD"] = POSTGRES_PASSWORD
    config["POSTGRES_PORT"] = POSTGRES_PORT
    return config


def prepare_items_for_pg(
    client: CustomExchange,
    raw_items: list[ccxtItem] | ccxtItem,
) -> list[dict[str, str]]:
    """
    Prepare CCXT items for export to PostgreSQL.

    Flattens structures and normalizes fields.

    Parameters
    ----------
    client : CustomExchange
        The exchange client.
    raw_items : list[ccxtItem] | ccxtItem
        The items to prepare.

    Returns
    -------
    list[dict[str, str]]
        List of prepared items as dictionaries.
    """
    imported_items = deepcopy(raw_items)

    if not isinstance(imported_items, list):
        items: list[ccxtItem] = [imported_items]
    else:
        items = imported_items

    prepared_items: list[dict[str, str]] = []

    for item in items:
        # Renaming order to order_id because of conflict in SQL
        if "order" in item:
            item["order_id"] = item.pop("order")

        # Integrating empty statement in case of nonexistent values

        item["fee_cost"] = ""
        item["fee_currency"] = ""
        item["usdt_value"] = ""
        item["asset_net_q"] = ""

        fee = item.get("fee")
        if isinstance(fee, dict):
            item["fee_cost"] = fee.get("cost", "")
            item["fee_currency"] = fee.get("currency", "")

        for fee in item.get("fees", []):
            if float(fee.get("cost", "0")) != 0.0:
                item["fee_cost"] = fee.get("cost", "")
                item["fee_currency"] = fee.get("currency", "")
        item["exchange"] = client.name

        # Generate usdt_value column
        cost_str = str(item.get("cost", "0"))
        fee_cost_str = str(item.get("fee_cost", "0"))

        assert isinstance(cost_str, str)
        try:
            cost = float(cost_str)
        except ValueError:
            cost = 0.0

        assert isinstance(fee_cost_str, str)
        try:
            fee_cost = float(fee_cost_str)
        except ValueError:
            fee_cost = 0.0

        if item["fee_currency"] != "USDT":
            item["usdt_value"] = cost_str
        elif item.get("side") == "buy":
            item["usdt_value"] = str(cost + fee_cost)
        else:
            item["usdt_value"] = str(cost - fee_cost)

        # Generate asset_net_q column
        amount_str = str(item.get("amount", "0"))
        assert isinstance(amount_str, str)
        try:
            amount = float(amount_str)
        except ValueError:
            amount = 0.0

        if item["fee_currency"] != "USDT":
            item["asset_net_q"] = str(amount - fee_cost)
        else:
            item["asset_net_q"] = amount_str
        # Flatten dicts and lists in order to export them to columns.
        flattened_item = dict_to_text(item)
        prepared_items.append(flattened_item)

    return prepared_items


def dict_to_text(d: ccxtItem) -> dict[str, str]:
    """
    Convert dictionary values to strings for SQL storage.

    Parameters
    ----------
    d : ccxtItem
        The item dictionary.

    Returns
    -------
    dict[str, str]
        The dictionary with string values.
    """

    def convert(i_value: object) -> str:
        if isinstance(i_value, str):
            return i_value
        if isinstance(i_value, dict):
            return ", ".join(
                f"{k}:{v}"
                for k, v in i_value.items()  # pyright: ignore[reportUnknownVariableType]
            )
        if isinstance(i_value, list):
            return ", ".join(
                f"{fee['currency']}:{fee['cost']}"
                for fee in i_value  # pyright: ignore[reportUnknownVariableType]
            )
        return str(i_value)

    return {key: convert(value) for key, value in d.items()}


async def retrieve_and_prepare_orders(
    client: CustomExchange,
    ticker: str,
    start: int | None = None,
    end: int | None = None,
):
    """
    Fetch and prepare orders from a client.

    Parameters
    ----------
    client : CustomExchange
        The exchange client.
    ticker : str
        The trading pair symbol.
    start : int | None, optional
        Start timestamp.
    end : int | None, optional
        End timestamp.

    Returns
    -------
    list[dict[str, str]]
        List of prepared orders.
    """
    if client.name == "Bitget":
        orders = await client.fetch_canceled_and_closed_orders(
            symbol=ticker, limit=100, since=start, params={"until": end}
        )
    else:
        orders = await client.fetch_closed_orders(
            symbol=ticker, limit=100, since=start, params={"until": end}
        )

    return prepare_items_for_pg(client, orders)


async def retrieve_and_prepare_trades(
    client: CustomExchange, pair: str, start: int | None = None, end: int | None = None
):
    """
    Fetch and prepare trades from a client.

    Parameters
    ----------
    client : CustomExchange
        The exchange client.
    pair : str
        The trading pair symbol.
    start : int | None, optional
        Start timestamp.
    end : int | None, optional
        End timestamp.

    Returns
    -------
    list[dict[str, str]]
        List of prepared trades.
    """
    trades = await client.fetch_my_trades(
        symbol=pair, limit=100, since=start, params={"until": end}
    )

    return prepare_items_for_pg(client, trades)


def export_to_sql(
    data: list[dict[str, str]],
    credentials: dict[str, str],
    table: str,
    client_name: str,
):
    """
    Export data to PostgreSQL.

    Parameters
    ----------
    data : list[dict[str, str]]
        The data to export.
    credentials : dict[str, str]
        PostgreSQL credentials.
    table : str
        The table name.
    client_name : str
        The name of the client (for logging).
    """
    dbname = credentials["POSTGRES_DB"]
    user = credentials["POSTGRES_USER"]
    password = credentials["POSTGRES_PASSWORD"]
    port = credentials["POSTGRES_PORT"]

    with psycopg.connect(
        f"dbname={dbname} user={user} password={password} host=localhost port={port}"
    ) as conn:
        with conn.cursor() as cur:
            # Insert data
            columns = data[0].keys()  # Get the column names from the dictionary
            columns_identifiers = [sql.Identifier(col.lower()) for col in columns]

            # Insert query
            insert_query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (exchange, id, side) DO NOTHING"
            ).format(
                sql.Identifier(table),
                sql.SQL(", ").join(columns_identifiers),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            )

            def sanitize(value: str):
                if value == "" or value == "None":
                    return None
                return value

            # Convert dictionaries to tuple format for psycopg3
            values = [tuple(sanitize(v) for v in d.values()) for d in data]

            # Execute the insert for all rows
            cur.executemany(insert_query, values)

        logger.info(f"Data for {client_name} inserted successfully in {table} table!")


def unaddressed_imbalances(pair: str, imbalances: DataFrame, orders: list[str]):
    """
    Identify and report unaddressed imbalances.

    Parameters
    ----------
    pair : str
        The trading pair.
    imbalances : DataFrame
        The imbalances dataframe.
    orders : list[str]
        List of open client order IDs.
    """
    ticker = pair.split("/")[0]

    # Drops the index from the returned pandas df
    try:
        imbalances.set_index("clientorderid", inplace=True)
    except AttributeError:
        raise AttributeError(
            "Nothing returned from database, there are likely no imbalances."
        )
    # Fetches all open orders on all active clients

    # Initialize and create a dict of all imbalances (oid and amount)

    imbalance_dict = {}

    for index, row in imbalances.iterrows():
        if index is not None:
            if row["symbol"] == pair:
                amount = round(float(row["delta"]), 2)
                imbalance_dict[index] = amount

    all_unaddressed_imbalances: list[UnaddressedImbalance] = []

    # Core loop, iterates over all imbalances and checks for a pending order. If none exists, they will be counted.

    for order_no in imbalance_dict.keys():
        if order_no not in orders:
            if imbalance_dict[order_no] < 0:
                side = "buy"

            else:
                side = "sell"

            order_data: UnaddressedImbalance = {
                "id": order_no,
                "amount": abs(imbalance_dict[order_no]),
                "side": side,
            }

            all_unaddressed_imbalances.append(order_data)

    buy_counter = 0
    buy_total = 0.0
    sell_counter = 0
    sell_total = 0.0

    for imbalance in all_unaddressed_imbalances:
        if imbalance["side"] == "buy":
            buy_counter += 1
            buy_total += imbalance["amount"]
        else:
            sell_counter += 1
            sell_total += imbalance["amount"]

    if buy_counter != 0:
        print(
            f"There are {buy_counter} unaddressed buy-side imbalances for a total of {buy_total} {ticker}."
        )

    if sell_counter != 0:
        print(
            f"There are {sell_counter} unaddressed sell-side imbalances for a total of {sell_total} {ticker}."
        )

    if buy_counter == 0 and sell_counter == 0:
        print("There are no unaddressed imbalances.")


async def fetch_all_open_orders_client_order_id(
    tickers: set[str], clients: dict[str, CustomExchange]
) -> list[str]:
    """
    Fetch client order IDs for all open orders.

    Parameters
    ----------
    tickers : set[str]
        Set of trading pairs.
    clients : dict[str, CustomExchange]
        Dictionary of exchange clients.

    Returns
    -------
    list[str]
        List of client order IDs.
    """
    tasks = []
    for client in clients.values():
        for ticker in tickers:
            tasks.append(client.fetch_open_orders(ticker))

    results = await asyncio.gather(*tasks)

    all_orders = []
    for orders in results:
        for order in orders:
            all_orders.append((order["clientOrderId"]))

    return all_orders
