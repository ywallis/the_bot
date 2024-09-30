import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

import time
from datetime import datetime, timedelta, date
import pandas as pd
import os
import pytz
import ccxt

from config.config import pair, path_to_data
from status_clients import all_clients


def fetch_order_amount(client, start, end):

    """The purpose of this function is only to return the
    amount of orders in a timeframe to support history bisection search"""

    if client.name == 'BitMart':
        orders = client.fetch_closed_orders(symbol=pair, limit=100, params={'startTime': start, 'endTime': end})
    elif client.name == 'Bitget':
        orders = client.fetch_canceled_and_closed_orders(symbol=pair, limit=100, params={'startTime': start, 'endTime': end})
    elif client.name == 'MEXC Global':
        orders = client.fetch_closed_orders(symbol=pair, limit=100, params={'startTime': start, 'endTime': end})
    else:
        orders = client.fetch_closed_orders(symbol=pair, limit=100, since=start, params={'until': end})

    return len(orders)

def fetch_trade_amount(client, start, end):

    """The purpose of this function is only to return the
       amount of trades in a timeframe to support history bisection search"""

    # Bitmart doesn't support queries for over 200 last trades.

    if client.name == 'BitMart':
        trades = client.fetch_my_trades(symbol=pair, limit=100, params={'startTime': start, 'endTime': end})

    elif client.name == 'MEXC Global':
        trades = client.fetch_my_trades(symbol=pair, limit=100, params={'startTime': start, 'endTime': end})

    else:
        trades = client.fetch_my_trades(symbol=pair, limit=100, since=start, params={'until': end})

    return len(trades)


def download_trades(client, start, end):

    """This function downloads all latest trades from a client to a csv file on the set path to NAS.
    Includes a production flag, to instead export to test folder if set to False."""

    today = str(date.today())
    trades_with_fee = []

    output_path = f'{path_to_data}{pair.split("/")[0]}/Trades/{today}_{client.name}_history.csv'


    # Bitmart doesn't support queries for over 200 last trades.

    if client.name == 'BitMart':
        trades = client.fetch_my_trades(symbol=pair, limit=200, params={'startTime': start, 'endTime': end})

    elif client.name == 'MEXC Global':
        trades = client.fetch_my_trades(symbol=pair, limit=100, params={'startTime': start, 'endTime': end})

    else:
        trades = client.fetch_my_trades(symbol=pair, limit=100, since=start, params={'until': end})

    # The if statement below should prevent for a dataframe with the improper amount of columns

    if len(trades) == 0:
        return

    for trade in trades:

        # Integrating empty statement in case of nonexistent values

        trade['fee_cost'] = None
        trade['fee_currency'] = None

        if trade['fee'] is not None:
            trade['fee_cost'] = trade['fee']['cost']
            trade['fee_currency'] = trade['fee']['currency']
        for fee in trade['fees']:
            if float(fee['cost']) != 0:
                trade['fee_cost'] = fee['cost']
                trade['fee_currency'] = fee['currency']
        trade['exchange'] = client.name
        trades_with_fee.append(trade)

    df = pd.DataFrame(trades_with_fee)
    df.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
    clean = pd.read_csv(output_path, index_col=0)
    clean.drop_duplicates(subset='id', inplace=True)
    clean.to_csv(output_path, header=True)


def download_orders(client, start, end):

    today = str(date.today())
    orders_with_fee = []

    output_path = f'{path_to_data}{pair.split("/")[0]}/Orders/{today}_{client.name}_history.csv'

    if client.name == 'BitMart':
        orders = client.fetch_closed_orders(symbol=pair, limit=200, params={'startTime': start, 'endTime': end})
    elif client.name == 'Bitget':
        orders = client.fetch_canceled_and_closed_orders(symbol=pair, limit=100, params={'startTime': start, 'endTime': end})
    elif client.name == 'MEXC Global':
        orders = client.fetch_closed_orders(symbol=pair, limit=100, params={'startTime': start, 'endTime': end})
    else:
        orders = client.fetch_closed_orders(symbol=pair, limit=100, since=start, params={'until': end})

    # The if statement below should prevent for a dataframe with the improper amount of columns

    if len(orders) == 0:
        return

    for order in orders:

        # Integrating empty statement in case of nonexistent values

        order['fee_cost'] = None
        order['fee_currency'] = None

        if order['fee'] is not None:
            order['fee_cost'] = order['fee']['cost']
            order['fee_currency'] = order['fee']['currency']
        for fee in order['fees']:
            if float(fee['cost']) != 0:
                order['fee_cost'] = fee['cost']
                order['fee_currency'] = fee['currency']
        order['exchange'] = client.name
        orders_with_fee.append(order)

    df = pd.DataFrame(orders_with_fee)
    df.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
    clean = pd.read_csv(output_path, index_col=0)
    clean.drop_duplicates(subset='id', inplace=True)
    clean.to_csv(output_path, header=True)


# This all works but is filthy. Turn into bisection search and clean this up!

def get_history(client, start_date_str):


    start_date = datetime.strptime(start_date_str, '%d/%m/%y')
    start_date_utc = pytz.timezone('UTC').localize(start_date)
    loop_start = start_date
    original_loop_size = timedelta(minutes=30)
    loop_size = original_loop_size

    while loop_start < start_date + timedelta(days=1):
        loop_end = loop_start + loop_size
        loop_start_utc = pytz.timezone('UTC').localize(loop_start)
        loop_end_utc = pytz.timezone('UTC').localize(loop_end)
        loop_start_input = int(loop_start_utc.timestamp() * 1000)
        loop_end_input = int(loop_end_utc.timestamp() * 1000)

        print(loop_start_utc)
        orders_in_timeframe = fetch_order_amount(client, loop_start_input, loop_end_input)
        print(f"There are {orders_in_timeframe} orders between {loop_start_utc} and {loop_end_utc}.")
        trades_in_timeframe = fetch_trade_amount(client, loop_start_input, loop_end_input)
        print(f"There are {trades_in_timeframe} trades between {loop_start_utc} and {loop_end_utc}.")

        # This section effectively implements a dirty bisection style sizing of the timeframe

        if orders_in_timeframe > 99 or trades_in_timeframe > 99:

            loop_size = loop_size // 2
            print(f'Over limit, reducing to {loop_size}')
            continue

        print(loop_end_utc)
        download_orders(client, loop_start_input, loop_end_input)
        download_trades(client, loop_start_input, loop_end_input)

        time.sleep(1)

        loop_start = loop_end

        # Resetting loop size to higher value.

        loop_size = original_loop_size


start_date_input = input('Enter the date (DD/MM/YY) for historical downloads (7D history):')

for client in all_clients:
    try:
        get_history(client, start_date_input)
    except ccxt.ExchangeError as e:
        print('Exchange error, retrying.')
        print(e)
    except ccxt.RequestTimeout as e:
        print('Request timeout, retrying.')
        print(e)
    except ccxt.NetworkError as e:
        print('Network error, retrying.')
        print(e)