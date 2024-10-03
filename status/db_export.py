import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

import psycopg

from status.status_clients import taker_client, pair, all_clients

def prepare_items_for_pg(items):
    """This function prepares CCXT order/trade items for an export to a PG database"""

    prepared_items = []

    for item in items:

        # Renaming order to order_id because of conflict in SQL
        if 'order' in item:
            item['order_id'] = item.pop('order')

        # Integrating empty statement in case of nonexistent values

        item['fee_cost'] = None
        item['fee_currency'] = None
        item['usdt_value'] = None
        item['asset_net_q'] = None

        if item['fee'] is not None:
            item['fee_cost'] = item['fee']['cost']
            item['fee_currency'] = item['fee']['currency']
        for fee in item['fees']:
            if float(fee['cost']) != 0:
                item['fee_cost'] = fee['cost']
                item['fee_currency'] = fee['currency']
        item['exchange'] = client.name

        # Generate usdt_value column

        if item    ['fee_currency'] != 'USDT':
            item['usdt_value'] = item['cost']
        elif item['side'] == 'buy':
            item['usdt_value'] = item['cost'] + item['fee_cost']
        else:
            item['usdt_value'] = item['cost'] - item['fee_cost']

        # Generate asset_net_q columns

        if item['fee_currency'] != 'USDT':
            if item['fee_cost'] is not None:
                item['asset_net_q'] = item['amount'] - item['fee_cost']
            else:
                item['asset_net_q'] = None
        else:
            item['asset_net_q'] = item['amount']

        # Flatten dicts and lists in order to export them to columns.
        flattened_item = dict_to_text(item)
        prepared_items.append(flattened_item)

    return prepared_items

def dict_to_text(d):

    def convert(value):
        if isinstance(value, dict) or isinstance(value, list):
            return str(value)  # Convert sub-dict to string
        return value

    for key, value in d.items():
        d[key] = convert(value)
    return d


def retrieve_and_prepare_orders(client, ticker, start=None, end=None):

    """This function downloads all latest trades from a client to a csv file on the set path to NAS.
    Includes a production flag, to instead export to test folder if set to False."""

    if client.name == 'Bitget':
        orders = client.fetch_canceled_and_closed_orders(symbol=ticker, limit=100, since=start, params={'until': end})
    else:
        orders = client.fetch_closed_orders(symbol=ticker, limit=500, since=start, params={'until': end})

    return  prepare_items_for_pg(orders)


def retrieve_and_prepare_trades(client, pair, start=None, end=None):

    """This function downloads trades from a CCXT client and prepares them to export to a Postgres server."""

    trades_with_fee = []

    trades = client.fetch_my_trades(symbol=pair, limit=100, since=start, params={'until': end})

    return prepare_items_for_pg(trades)

def export_to_sql(data, credentials, table):

    """Takes in a list of orders or trades in CCXT format, and a dict of PG credentials, and outputs the data to the attached DB."""

    dbname = credentials['dbname']
    user = credentials['user']
    password = credentials['password']

    with psycopg.connect(f"dbname={dbname} user={user} password={password} host=localhost port=5432") as conn:
        with conn.cursor() as cur:

            # Insert data
            columns = data[0].keys()  # Get the column names from the dictionary
            columns_str = ', '.join(columns)  # Comma-separated column names
            placeholders = ', '.join(['%s'] * len(columns))  # Generate placeholders for each column

            # Insert query
            insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) ON CONFLICT (datetime, id) DO NOTHING"

            # Convert dictionaries to tuple format for psycopg3
            values = [tuple(d.values()) for d in data]

            # Execute the insert for all rows
            cur.executemany(insert_query, values)

        print("Data inserted successfully!")


pg_credentials = {'dbname': 'arb_bot',
                  'user': 'postgres',
                  'password': 'password'}

for client in all_clients:
    trades = retrieve_and_prepare_trades(client, pair)
    export_to_sql(trades, pg_credentials, 'trades')
    orders = retrieve_and_prepare_orders(client, pair)
    export_to_sql(orders, pg_credentials, 'orders')




