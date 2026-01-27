"""
This module provides a real-time status dashboard for the trading bot.
It displays open orders, account balances, and daily performance metrics in the terminal.
"""

import asyncio
from datetime import datetime
import os

from pandas import DataFrame
from psycopg import sql

from apps.shared.src.errors import RequestTimeout, NetworkError, ExchangeError
from apps.accountant.src.sql_connector import QueryLoader, send_sql_query
from apps.accountant.src.utils import (
    fetch_all_open_orders_client_order_id,
    load_pg_config,
    unaddressed_imbalances,
)
from apps.shared.src.exchange_clients import (
    authenticated_clients,
    load_clients,
    symbols,
)
from apps.shared.src.structs import CustomExchange


async def get_order_status(
    clients: dict[str, CustomExchange], ticker: str, details=True
):
    """
    List all open orders for a client in a terminal format.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        Dictionary of exchange clients.
    ticker : str
        The trading pair symbol.
    details : bool, optional
        Whether to print detailed information for each order. Default is True.
    """

    for client in clients.values():
        all_open_orders = await client.fetch_open_orders(ticker)
        open_buy_orders_total = 0
        open_sell_orders_total = 0

        for order in all_open_orders:
            if order["side"] == "buy":
                open_buy_orders_total += float(order["remaining"])
            elif order["side"] == "sell":
                open_sell_orders_total += float(order["remaining"])
            if details:
                print(
                    f"Open {order['side']} {ticker.split("/")[0]} order on {client.name} at {order['price']}, "
                    f"{round(float(order['remaining']), 2)} of {round(float(order['amount']), 2)} remaining."
                )
        if open_buy_orders_total != 0:
            print(f"Total of {round(open_buy_orders_total, 2)} {ticker.split("/")[0]} buys open on {client.name}.")
        if open_sell_orders_total != 0:
            print(
                f"Total of {round(open_sell_orders_total, 2)} {ticker.split("/")[0]} sells open on {client.name}."
            )


async def fetch_balances(clients: dict[str, CustomExchange]):
    """
    Fetch and print account balances for all clients.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        Dictionary of exchange clients.
    """
    for client in clients.values():
        balances = await client.fetch_balance()
        assert isinstance(balances["free"], dict)
        for symbol, amount in balances["free"].items():
            print(f"{round(amount, 6)} {symbol} available on {client.name}")


def get_daily_performance(query: sql.Composed, symbol: str):
    """
    Fetch and print daily performance metrics.

    Parameters
    ----------
    query : sql.Composed
        The SQL query for daily performance.
    symbol : str
        The trading pair symbol.
    """
    pg_config = load_pg_config()
    daily_performance = send_sql_query(
        pg_config, query, False, {"symbol": symbol, "range": 1}
    )
    if daily_performance is not None:
        print(daily_performance)


async def main(clients: dict[str, CustomExchange], pairs: set):
    """
    Main entry point for the status dashboard.
    Continuously updates the display with order status, balances, and performance.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        Dictionary of exchange clients.
    pairs : set
        Set of trading pairs to monitor.
    """
    pg_config = load_pg_config()
    # Initialize QueryLoader

    query_loader = QueryLoader()
    query_loader.load_queries()
    fetch_imbalances_query = query_loader.get_query("fetch_imbalances")
    daily_string = query_loader.get_query("daily_overview")
    if daily_string is None:
        raise Exception("Query could not be loaded")
    daily_overview = sql.SQL(daily_string).format(
        symbol=sql.Placeholder("symbol"), range=sql.Placeholder("range")
    )
    if fetch_imbalances_query is None:
        raise Exception("Error loading query")
    await load_clients()

    while True:
        try:
            os.system("clear")
            print(f"Status at {datetime.now()}")
            for symbol in pairs:
                await get_order_status(clients, symbol, True)
            await fetch_balances(clients)
            all_open_orders = await fetch_all_open_orders_client_order_id(
                pairs, authenticated_clients
            )
            for symbol in pairs:
                imbalances = send_sql_query(
                    pg_config, fetch_imbalances_query, False, {"symbol": symbol}
                )
                if imbalances is None:
                    print(f"No imbalances for {symbol}")
                if isinstance(imbalances, DataFrame):
                    # raise Exception("Error returning imbalances from DB")
                    try:
                        unaddressed_imbalances(symbol, imbalances, all_open_orders)
                    except KeyError:
                        print(f"No imbalances for {symbol}")

                get_daily_performance(daily_overview, symbol)

            await asyncio.sleep(15)
        except ExchangeError as e:
            print("Exchange error, retrying.")
            print(e)
        except RequestTimeout as e:
            print("Request timeout, retrying.")
            print(e)
        except NetworkError as e:
            print("Network error, retrying.")
            print(e)


if __name__ == "__main__":
    asyncio.run(main(authenticated_clients, symbols))
