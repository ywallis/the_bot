import sys
import os
# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from services.status_clients import taker_client, pair, all_clients
from services.sql_connector import send_sql_query
from services.sql_queries import *
from status_boiler import fetch_all_open_orders_client_order_id

from dotenv import dotenv_values

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/docker/database/', '.env'))
pg_config = dotenv_values(f'..{config_path}')

print('Uses a static spread for the time being, please modify.')
spread = 1.003

# Create a simple loop to choose a single date to match on. Can be escaped with '00' for a full history match

loop = True
date_mod = ""
date = ""
while loop:
    date = input('Enter the date (DD/MM/YY) you want to match orders on:')
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

imbalances = send_sql_query(pg_config, unmatched)

# Drops the index from the returned pandas df

imbalances.set_index('clientorderid', inplace=True)

# Fetches all open orders on all active clients

orders = fetch_all_open_orders_client_order_id(pair, *all_clients)

# Initialize and create a dict of all imbalances (oid and amount)

imbalance_dict = {}

print(imbalances)

for index, row in imbalances.iterrows():
    if index is not None:
        if index.startswith(f't-{date_mod}'):
            amount = round(float(row['sell_minus_buy']), 2)
            imbalance_dict[index] = amount

all_orders_to_place = []

# Core loop, iterates over all imbalances and checks for a pending order. If none exists, will create one.

for order_no in imbalance_dict.keys():
    side = ""
    print(order_no)
    if order_no in orders:
        print('This is pending')

    else:
        if imbalance_dict[order_no] > 0:
            print(f'{imbalance_dict[order_no]} imbalance, should buy!')
            side = "buy"

            # Search through the database for the price the unmatched orders were executed at.

            # First looks at partial fills for price. If they exist, will take the most reachable price
            query = f"""
            SELECT 
    trades.*,
    orders.clientorderid
FROM 
    trades
LEFT JOIN 
    orders ON trades.order_id = orders.id
WHERE
		orders.clientorderid = '{order_no}' AND trades.side = 'buy';

            """
            partial_fills = send_sql_query(pg_config, query)

            if partial_fills is not None:
                if partial_fills.size > 0:
                    partial_fills = max(partial_fills['price'].values)

            # If partial fills don't exist, looks at the price most reachable price on the unmatched order.

            query = f"""
                        SELECT 
                trades.*,
                orders.clientorderid
            FROM 
                trades
            LEFT JOIN 
                orders ON trades.order_id = orders.id
            WHERE
            		orders.clientorderid = '{order_no}' AND trades.side = 'sell';

                        """
            imbalanced_price = send_sql_query(pg_config, query)['price'].values

            imbalanced_price = min(imbalanced_price)

            if partial_fills is not None:
                if partial_fills.size > 0:
                    price = partial_fills
                    print('There are partial fills, will use their price.')
                else:
                    price = round(float(imbalanced_price) / spread, 3)

            else:
                price = round(float(imbalanced_price) / spread, 3)

            print(f'Creating a matching buy order for order {order_no} with a quantity of {imbalance_dict[order_no]} and a price of {price}.')

        else:
            print(f'{imbalance_dict[order_no]} imbalance, should sell!')
            side = "sell"

            # Search through the database for the price the unmatched orders were executed at.

            # First looks at partial fills for price. If they exist, will take the most reachable price
            query = f"""
            SELECT 
    trades.*,
    orders.clientorderid
FROM 
    trades
LEFT JOIN 
    orders ON trades.order_id = orders.id
WHERE
		orders.clientorderid = '{order_no}' AND trades.side = 'sell';

            """
            partial_fills = send_sql_query(pg_config, query)

            if partial_fills is not None:
                if partial_fills.size > 0:
                    partial_fills = max(partial_fills['price'].values)

            # If partial fills don't exist, looks at the price most reachable price on the unmatched order.

            query = f"""
                        SELECT 
                trades.*,
                orders.clientorderid
            FROM 
                trades
            LEFT JOIN 
                orders ON trades.order_id = orders.id
            WHERE
            		orders.clientorderid = '{order_no}' AND trades.side = 'buy';

                        """
            imbalanced_price = send_sql_query(pg_config, query)['price'].values

            imbalanced_price = min(imbalanced_price)

            if partial_fills is not None:
                if partial_fills.size > 0:
                    price = partial_fills
                    print('There are partial fills, will use their price.')
                else:
                    price = round(float(imbalanced_price) / spread, 3)

            else:
                price = round(float(imbalanced_price) / spread, 3)

            print(
                f'Creating a matching sell order for order {order_no} with a quantity of {imbalance_dict[order_no]} and a price of {price}.')

        # Separating order creation and data collection to introduce a stopper

        order_data = {'id': order_no,
                      'amount': abs(imbalance_dict[order_no]),
                      'price': price,
                      'side': side, }

        all_orders_to_place.append(order_data)

print(all_orders_to_place)

confirming = True
while confirming:
    confirmation = input('Are you sure you want to place these order? y/n')
    if confirmation == "y":
        confirming = False

        for order in all_orders_to_place:
            print('Placing')
            print(taker_client.create_order(symbol=pair,
                                            type='limit',
                                            side=order['side'],
                                            amount=order['amount'],
                                            price=order['price'],
                                            params={f'clientOrderId': order['id']}))
    if confirmation == 'n':
        confirming = False
