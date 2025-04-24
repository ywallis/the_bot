from pandas import DataFrame
from psycopg import sql
from apps.accountant.src.sql_connector import send_sql_query, QueryLoader
from apps.accountant.src.utils import (
    fetch_all_open_orders_client_order_id,
    load_pg_config,
)
from apps.shared.src.exchange_clients import authenticated_clients, symbols

pg_config = load_pg_config()

# Initialize QueryLoader

query_loader = QueryLoader()
query_loader.load_queries()
fetch_imbalances = query_loader.get_query("fetch_imbalances")
if fetch_imbalances is None:
    raise Exception("Error loading query")

print("Uses a static spread for the time being, please modify.")
spread = 1.003

# Create a simple loop to choose a single date to match on. Can be escaped with '00' for a full history match

loop = True
date_mod = ""
date = ""
while loop:
    date = input("Enter the date (DD/MM/YY) you want to match orders on:")
    date_mod = "".join(date.split("/")[::-1])
    print(date_mod)
    if date_mod != "":
        loop = False
    else:
        print("This can't be empty")

if date == "00":
    date_mod = ""
    print(date_mod)

# Fetches all imbalances from database

imbalances = send_sql_query(pg_config, fetch_imbalances)
if not isinstance(imbalances, DataFrame):
    raise Exception("No imbalances returned")

# Drops the index from the returned pandas df
try:
    imbalances.set_index("clientorderid", inplace=True)
except AttributeError:
    print("Nothing returned from database, there are likely no imbalances.")
    raise AttributeError(
        "Nothing returned from database, there are likely no imbalances."
    )
# Fetches all open orders on all active clients

orders = fetch_all_open_orders_client_order_id(symbols, authenticated_clients)

# Initialize and create a dict of all imbalances (oid and amount)

imbalance_dict = {}

print(imbalances)

for index, row in imbalances.iterrows():
    if index is not None:
        if index.__str__().startswith(f"t-{date_mod}") and row["symbol"] in symbols:
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
            query = sql.SQL("""
            SELECT 
    trades.*,
    orders.clientorderid
FROM 
    trades
LEFT JOIN 
    orders ON trades.order_id = orders.id
WHERE
		orders.clientorderid = %s AND trades.side = 'buy';

            """)
            partial_fills = send_sql_query(pg_config, query, False, (order_no,))

            if partial_fills is not None:
                if partial_fills.size > 0:
                    partial_fills = max(partial_fills["price"].values)

            # If partial fills don't exist, looks at the price most reachable price on the unmatched order.

            query = sql.SQL("""
                        SELECT 
                trades.*,
                orders.clientorderid
            FROM 
                trades
            LEFT JOIN 
                orders ON trades.order_id = orders.id
            WHERE
            		orders.clientorderid = %s AND trades.side = 'sell';

                        """)
            try:
                imbalance = send_sql_query(pg_config, query, False, (order_no,))
                if imbalance is not None:
                    imbalanced_price = imbalance["price"].values

                imbalanced_price = min(imbalanced_price)

                if partial_fills is not None:
                    price = partial_fills
                    print("There are partial fills, will use their price.")

                else:
                    price = round(float(imbalanced_price) / spread, 3)

                print(
                    f"Creating a matching buy order for order {order_no} with a quantity of {abs(imbalance_dict[order_no])} and a price of {price}."
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
            query = sql.SQL("""
            SELECT 
    trades.*,
    orders.clientorderid
FROM 
    trades
LEFT JOIN 
    orders ON trades.order_id = orders.id
WHERE
		orders.clientorderid = %s AND trades.side = 'sell';

            """)
            partial_fills = send_sql_query(pg_config, query, False, (order_no,))

            if partial_fills is not None:
                if partial_fills.size > 0:
                    partial_fills = max(partial_fills["price"].values)

            # If partial fills don't exist, looks at the price most reachable price on the unmatched order.

            query = sql.SQL("""
                        SELECT 
                trades.*,
                orders.clientorderid
            FROM 
                trades
            LEFT JOIN 
                orders ON trades.order_id = orders.id
            WHERE
            		orders.clientorderid = %s AND trades.side = 'buy';

                        """)
            try:
                imbalance = send_sql_query(pg_config, query, False, (order_no,))
                if imbalance is not None:
                    imbalanced_price = imbalance["price"].values

                imbalanced_price = min(imbalanced_price)

                if partial_fills is not None:
                    price = partial_fills
                    print("There are partial fills, will use their price.")

                else:
                    price = round(float(imbalanced_price) / spread, 3)

                print(
                    f"Creating a matching sell order for order {order_no} with a quantity of {abs(imbalance_dict[order_no])} and a price of {price}."
                )

            except TypeError as e:
                print(e)
                print("Order likely missing, should recollect data.")
                raise

        # Separating order creation and data collection to introduce a stopper

        order_data = {
            "id": order_no,
            "amount": abs(imbalance_dict[order_no]),
            "price": round(float(price), 3),
            "side": side,
        }

        all_orders_to_place.append(order_data)

confirming = True
while confirming:
    confirmation = input("Are you sure you want to place these order? y/n")
    if confirmation == "y":
        confirming = False

        for order in all_orders_to_place:
            print("Placing")
            print(
                taker_client.create_order(
                    symbol=pair,
                    type="limit",
                    side=order["side"],
                    amount=order["amount"],
                    price=order["price"],
                    params={"clientOrderId": order["id"]},
                )
            )
    if confirmation == "n":
        confirming = False
