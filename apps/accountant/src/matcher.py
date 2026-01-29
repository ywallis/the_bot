"""Module for matching imbalances by placing orders."""

import asyncio
from decimal import Decimal

from pandas import DataFrame
from psycopg import sql

from apps.accountant.src.sql_connector import QueryLoader, send_sql_query
from apps.accountant.src.utils import (
    fetch_all_open_orders_client_order_id,
    load_pg_config,
)
from apps.shared.src.exchange_clients import authenticated_clients, symbols
from apps.shared.src.structs import CustomExchange


def currency_picker() -> str:
    """
    Prompt the user to select a currency pair.

    Returns
    -------
    str
        The selected currency pair.
    """
    while True:
        currency = input("Enter the token pair to be matched: ")

        break
    return currency


def date_picker() -> str:
    """
    Prompt the user to select a date for matching orders.

    Returns
    -------
    str
        The selected date string modified for matching.
    """
    loop = True
    date_mod = ""
    date = ""
    while loop:
        date = input("Enter the date (DD/MM/YY) you want to match orders on: ")
        date_mod = "".join(date.split("/")[::-1])
        print(date_mod)
        if date_mod != "":
            loop = False
        else:
            print("This can't be empty")

    if date == "00":
        date_mod = ""
        print(date_mod)

    return date_mod


def client_picker(clients: dict[str, CustomExchange]):
    """
    Prompt the user to select an exchange client.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        Available exchange clients.

    Returns
    -------
    CustomExchange
        The selected client.
    """
    print(f"Available exchanges are: {[name for name in clients]}")
    while True:
        choice = input("Enter name of client you want to match on: ")

        if choice in clients:
            break

    return clients[choice]


async def client_closer(clients: dict[str, CustomExchange]):
    """
    Close all exchange client connections.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        The exchange clients to close.
    """
    for client in clients.values():
        await client.close()


async def main():
    """
    Execute the matcher module.

    Loads configuration, identifies imbalances, constructs remedial orders,
    and executes them after user confirmation.
    """
    pg_config = load_pg_config()

    # Initialize QueryLoader

    query_loader = QueryLoader()
    query_loader.load_queries()
    fetch_imbalances = query_loader.get_query("fetch_imbalances")
    if fetch_imbalances is None:
        raise Exception("Error loading query")

    print("Uses a static spread for the time being, please modify.")
    spread = 1.003

    taker_client = client_picker(authenticated_clients)
    date = date_picker()
    currency = currency_picker()

    imbalances = send_sql_query(
        pg_config, fetch_imbalances, False, {"symbol": currency}
    )
    if not isinstance(imbalances, DataFrame):
        raise Exception("No imbalances returned")

    # Drops the index from the returned pandas df
    imbalances.set_index("clientorderid", inplace=True)
    orders = await fetch_all_open_orders_client_order_id(symbols, authenticated_clients)
    imbalance_dict = {}

    print(imbalances)

    for index, row in imbalances.iterrows():
        if index is not None:
            if index.__str__().startswith(f"t-{date}") and row["symbol"] in symbols:
                amount = round(float(row["delta"]), 2)
                imbalance_dict[index] = amount

    all_orders_to_place = []

    # Core loop, iterates over all imbalances and checks for a pending order. If none exists, will create one.

    for order_no in imbalance_dict.keys():
        side = ""
        print(order_no)
        if order_no in orders:
            print("This is pending")

        else:
            if imbalance_dict[order_no] < 0:
                print(f"{imbalance_dict[order_no]} imbalance, should buy!")
                side = "buy"

                # Search through the database for the price the unmatched orders were executed at.

                # First looks at partial fills for price. If they exist, will take the most reachable price
                query = sql.SQL(
                    """
                SELECT 
        trades.*,
        orders.clientorderid
    FROM 
        trades
    LEFT JOIN 
        orders ON trades.order_id = orders.id
    WHERE
            orders.clientorderid = %s AND trades.side = 'buy';

                """
                )
                partial_fills = send_sql_query(pg_config, query, False, (order_no,))

                if partial_fills is not None:
                    if not isinstance(partial_fills, DataFrame):
                        raise Exception("Error loading from DB")
                    if partial_fills.size > 0:
                        partial_fills = max(partial_fills["price"].values)

                # If partial fills don't exist, looks at the price most reachable price on the unmatched order.

                query = sql.SQL(
                    """
                            SELECT 
                    trades.*,
                    orders.clientorderid
                FROM 
                    trades
                LEFT JOIN 
                    orders ON trades.order_id = orders.id
                WHERE
                        orders.clientorderid = %s AND trades.side = 'sell';

                            """
                )
                try:
                    imbalance = send_sql_query(pg_config, query, False, (order_no,))
                    if imbalance is not None:
                        if not isinstance(imbalance, DataFrame):
                            raise Exception("Error loading from DB")
                        imbalanced_price = imbalance["price"].values
                    else:
                        raise Exception("Imbalance cannot be loaded")

                    imbalanced_price = min(imbalanced_price)

                    if partial_fills is not None:
                        price = partial_fills
                        print("There are partial fills, will use their price.")

                    else:
                        price = round(float(imbalanced_price) / spread, 3)

                    print(
                        f"Creating a matching {imbalance['symbol']} buy order for order {order_no} with a quantity of {abs(imbalance_dict[order_no])} and a price of {price}."
                    )

                except TypeError as e:
                    print(e)
                    print("Order likely missing, should recollect data.")
                    raise

            else:
                print(f"{imbalance_dict[order_no]} imbalance, should sell!")
                side = "sell"

                # Search through the database for the price the unmatched orders were executed at.

                # First looks at partial fills for price. If they exist, will take the most reachable price
                query = sql.SQL(
                    """
                SELECT 
        trades.*,
        orders.clientorderid
    FROM 
        trades
    LEFT JOIN 
        orders ON trades.order_id = orders.id
    WHERE
            orders.clientorderid = %s AND trades.side = 'sell';

                """
                )
                partial_fills = send_sql_query(pg_config, query, False, (order_no,))

                if partial_fills is not None:
                    if not isinstance(partial_fills, DataFrame):
                        raise Exception("Error loading from DB")
                    if partial_fills.size > 0:
                        partial_fills = max(partial_fills["price"].values)

                # If partial fills don't exist, looks at the price most reachable price on the unmatched order.

                query = sql.SQL(
                    """
                            SELECT 
                    trades.*,
                    orders.clientorderid
                FROM 
                    trades
                LEFT JOIN 
                    orders ON trades.order_id = orders.id
                WHERE
                        orders.clientorderid = %s AND trades.side = 'buy';

                            """
                )
                try:
                    imbalance = send_sql_query(pg_config, query, False, (order_no,))
                    if imbalance is not None:
                        if not isinstance(imbalance, DataFrame):
                            raise Exception("Error loading imbalance from db")
                        imbalanced_price = imbalance["price"].values

                    else:
                        raise Exception("Imbalance not found")
                    imbalanced_price = min(imbalanced_price)

                    if partial_fills is not None:
                        price = partial_fills
                        print("There are partial fills, will use their price.")

                    else:
                        price = round(float(imbalanced_price) / spread, 3)

                    print(
                        f"Creating a matching {imbalance['symbol']} sell order for order {order_no} with a quantity of {abs(imbalance_dict[order_no])} and a price of {price}."
                    )

                except TypeError as e:
                    print(e)
                    print("Order likely missing, should recollect data.")
                    raise

            # Separating order creation and data collection to introduce a stopper

            if not isinstance(price, (float, Decimal)):
                print(type(price))
                raise Exception("Issue with price")
            order_data = {
                "id": order_no,
                "amount": abs(imbalance_dict[order_no]),
                "price": round(float(price), 3),
                "side": side,
                "symbol": imbalance["symbol"][0],
            }

            all_orders_to_place.append(order_data)

    confirming = True
    while confirming:
        confirmation = input("Are you sure you want to place these order? y/n")
        if confirmation == "y":
            confirming = False

            for order in all_orders_to_place:
                print("Placing")
                order = await taker_client.create_order(
                    symbol=order["symbol"],
                    type="limit",
                    side=order["side"],
                    amount=order["amount"],
                    price=order["price"],
                    params={"clientOrderId": order["id"]},
                )
                print(order)
        if confirmation == "n":
            confirming = False

    await client_closer(authenticated_clients)


if __name__ == "__main__":
    asyncio.run(main())
