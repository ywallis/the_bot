import psycopg
import os
from dotenv import dotenv_values

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/docker/database/', '.env'))
pg_config = dotenv_values(f'..{config_path}')

def prepare_items_for_pg(client, imported_items):
    """This function prepares CCXT order/trade items for an export to a PG database"""

    if type(imported_items) != list:
        items = [imported_items]
    else:
        items = imported_items

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

        if item['fee_currency'] != 'USDT':
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

    def convert(i_value):
        if isinstance(i_value, dict) or isinstance(i_value, list):
            return str(i_value)  # Convert sub-dict to string
        return i_value

    for key, value in d.items():
        d[key] = convert(value)
    return d


def retrieve_and_prepare_orders(client, ticker, start=None, end=None):

    """This function downloads all latest orders from a client."""

    if client.name == 'Bitget':
        orders = client.fetch_canceled_and_closed_orders(symbol=ticker, limit=100, since=start, params={'until': end})
    else:
        orders = client.fetch_closed_orders(symbol=ticker, limit=100, since=start, params={'until': end})

    return prepare_items_for_pg(client, orders)


def retrieve_and_prepare_trades(client, pair, start=None, end=None):

    """This function downloads trades from a CCXT client and prepares them to export to a Postgres server."""

    trades = client.fetch_my_trades(symbol=pair, limit=100, since=start, params={'until': end})

    return prepare_items_for_pg(client, trades)

def export_to_sql(data, credentials, table):

    """Takes in a list of orders or trades in CCXT format, and a dict of PG credentials, and outputs the data to the attached DB."""

    dbname = credentials['POSTGRES_DB']
    user = credentials['POSTGRES_USER']
    password = credentials['POSTGRES_PASSWORD']

    with psycopg.connect(f"dbname={dbname} user={user} password={password} host=localhost port=5432") as conn:
        with conn.cursor() as cur:

            # Insert data
            columns = data[0].keys()  # Get the column names from the dictionary
            columns_str = ', '.join(columns)  # Comma-separated column names
            placeholders = ', '.join(['%s'] * len(columns))  # Generate placeholders for each column

            # Insert query
            insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) ON CONFLICT (exchange, id) DO NOTHING"

            # Convert dictionaries to tuple format for psycopg3
            values = [tuple(d.values()) for d in data]

            # Execute the insert for all rows
            cur.executemany(insert_query, values)

        print(f"Data inserted successfully in {table} table!")


def unaddressed_imbalances(pair, imbalances, orders):
    """This function tries to notify of imbalances in an arbitrage setup, similar to the matcher, but designed as a background service.
    Imbalances are meant to be fed as a pandas DF. Orders are CCXT objects."""

    ticker = pair.split('/')[0]

    # Drops the index from the returned pandas df
    try:
        imbalances.set_index('clientorderid', inplace=True)
    except AttributeError:
        print('Nothing returned from database, there are likely no imbalances.')
        raise AttributeError('Nothing returned from database, there are likely no imbalances.')
    # Fetches all open orders on all active clients

    # Initialize and create a dict of all imbalances (oid and amount)

    imbalance_dict = {}

    for index, row in imbalances.iterrows():
        if index is not None:
            if row['symbol'] == pair:
                amount = round(float(row['delta']), 2)
                imbalance_dict[index] = amount

    all_unaddressed_imbalances = []

    # Core loop, iterates over all imbalances and checks for a pending order. If none exists, they will be counted.

    for order_no in imbalance_dict.keys():

        if order_no not in orders:

            if imbalance_dict[order_no] < 0:
                side = 'buy'

            else:
                side = 'sell'

            order_data = {'id': order_no,
                          'amount': abs(imbalance_dict[order_no]),
                          'side': side, }

            all_unaddressed_imbalances.append(order_data)

    buy_counter = 0
    buy_total = 0
    sell_counter = 0
    sell_total = 0

    for imbalance in all_unaddressed_imbalances:
        if imbalance['side'] == 'buy':
            buy_counter += 1
            buy_total += imbalance['amount']
        else:
            sell_counter += 1
            sell_total += imbalance['amount']

    if buy_counter != 0:
        print(f'There are {buy_counter} unaddressed buy-side imbalances for a total of {buy_total} {ticker}.')

    if sell_counter != 0:
        print(f'There are {sell_counter} unaddressed sell-side imbalances for a total of {sell_total} {ticker}.')

    if buy_counter == 0 and sell_counter == 0:
        print(f'There are no unaddressed imbalances.')