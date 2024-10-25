import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

import time
from datetime import datetime, timedelta
import os
import pytz
import ccxt

from status_clients import all_clients, pair
from accounting_boiler import retrieve_and_prepare_orders, retrieve_and_prepare_trades, export_to_sql

from dotenv import dotenv_values

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/docker/database/', '.env'))
pg_config = dotenv_values(f'..{config_path}')

def get_history(client, start_date_str):

    start_date = datetime.strptime(start_date_str, '%d/%m/%y')
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
        orders_in_timeframe = retrieve_and_prepare_orders(client, pair, loop_start_input, loop_end_input)
        print(f'There are {len(orders_in_timeframe)} orders between {loop_start_utc.strftime("%Y-%m-%d %H:%M:%S")} '
              f'and {loop_end_utc.strftime("%Y-%m-%d %H:%M:%S")} on {client}.')
        trades_in_timeframe = retrieve_and_prepare_trades(client, pair, loop_start_input, loop_end_input)
        print(f'There are {len(trades_in_timeframe)} trades between {loop_start_utc.strftime("%Y-%m-%d %H:%M:%S")} '
              f'and {loop_end_utc.strftime("%Y-%m-%d %H:%M:%S")} on {client}.')

        # This section effectively implements a dirty bisection style sizing of the timeframe

        if len(orders_in_timeframe) > 99 or len(trades_in_timeframe) > 99:

            loop_size = loop_size // 2
            print(f'Over limit, reducing to {loop_size}')
            continue

        print(loop_end_utc)
        if len(trades_in_timeframe) != 0:
            export_to_sql(trades_in_timeframe, pg_config, 'trades')
        if len(orders_in_timeframe) != 0:
            export_to_sql(orders_in_timeframe, pg_config, 'orders')

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