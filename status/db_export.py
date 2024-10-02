import psycopg

from status.status_clients import taker_client, pair, all_clients

def dict_to_text(d):
    def convert(value):
        if isinstance(value, dict) or isinstance(value, list):
            return str(value)  # Convert sub-dict to string
        return value

    for key, value in d.items():
        d[key] = convert(value)
    return d


def download_orders(client, ticker, production=True):

    """This function downloads all latest trades from a client to a csv file on the set path to NAS.
    Includes a production flag, to instead export to test folder if set to False."""


    # Bitmart doesn't support queries for over 200 last trades.

    if client.name == 'BitMart':
        orders = client.fetch_closed_orders(symbol=ticker, limit=200)
    elif client.name == 'Bitget':
        orders = client.fetch_canceled_and_closed_orders(symbol=ticker, limit=100)
    else:
        orders = client.fetch_closed_orders(symbol=ticker, limit=500)

def download_trades_to_sql(client, pair, start=None, end=None):


    """This function downloads all latest trades from a client to a Postgres server."""

    trades_with_fee = []

    trades = client.fetch_my_trades(symbol=pair, limit=100, since=start, params={'until': end})

    for trade in trades:

        # Renaming order to order_id because of conflict in SQL

        trade['order_id'] = trade.pop('order')

        # Integrating empty statement in case of nonexistent values

        trade['fee_cost'] = None
        trade['fee_currency'] = None
        trade['usdt_value'] = None
        trade['asset_net_q'] = None

        if trade['fee'] is not None:
            trade['fee_cost'] = trade['fee']['cost']
            trade['fee_currency'] = trade['fee']['currency']
        for fee in trade['fees']:
            if float(fee['cost']) != 0:
                trade['fee_cost'] = fee['cost']
                trade['fee_currency'] = fee['currency']
        trade['exchange'] = client.name

        # Generate usdt_value column

        if trade    ['fee_currency'] != 'USDT':
            trade['usdt_value'] = trade['cost']
        elif trade['side'] == 'buy':
            trade['usdt_value'] = trade['cost'] + trade['fee_cost']
        else:
            trade['usdt_value'] = trade['cost'] - trade['fee_cost']

        # Generate asset_net_q columns

        if trade['fee_currency'] != 'USDT':
            trade['asset_net_q'] = trade['amount'] - trade['fee_cost']
        else:
            trade['asset_net_q'] = trade['amount']

        # Flatten dicts and lists in order to export them to columns.
        flattened_trade = dict_to_text(trade)
        trades_with_fee.append(flattened_trade)

    print(trades_with_fee[0]['datetime'])
    print(trades_with_fee[-1]['datetime'])

    with psycopg.connect("dbname=arb_bot user=postgres password=password host=localhost port=5432") as conn:
        with conn.cursor() as cur:
            # Define your table structure
            table = 'trades'
            columns = trades_with_fee[0].keys()  # Get the column names from the dictionary
            columns_str = ', '.join(columns)  # Comma-separated column names
            placeholders = ', '.join(['%s'] * len(columns))  # Generate placeholders for each column

            # Insert query
            insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) ON CONFLICT (datetime, id) DO NOTHING"

            # Convert dictionaries to tuple format for psycopg3
            values = [tuple(d.values()) for d in trades_with_fee]

            # Execute the insert for all rows
            cur.executemany(insert_query, values)

        print("Data inserted successfully!")


for client in all_clients:
    download_trades_to_sql(client, pair, start=1727654400000, end=1727697600000)

