import sys
from datetime import datetime
import time
import ccxt
import os

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from dotenv import dotenv_values

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/docker/database/', '.env'))
pg_config = dotenv_values(f'..{config_path}')

from services.status_clients import pair, all_clients
from services.accounting_boiler import retrieve_and_prepare_trades, retrieve_and_prepare_orders, export_to_sql

looping = True

while looping:
    print(f'Status at {datetime.now()}')

    try:
        for client in all_clients:

            print(f'Now exporting {client.name} trades.')
            trades = retrieve_and_prepare_trades(client, pair)
            if len(trades) != 0:
                export_to_sql(trades, pg_config, 'trades')
            print(f'Now exporting {client.name} orders.')
            orders = retrieve_and_prepare_orders(client, pair)
            if len(orders) != 0:
                export_to_sql(orders, pg_config, 'orders')


    except ccxt.ExchangeError as e:
        print('Exchange error, retrying.')
        print(e)
    except ccxt.RequestTimeout as e:
        print('Request timeout, retrying.')
        print(e)
    except ccxt.NetworkError as e:
        print('Network error, retrying.')
        print(e)

    else:
        time.sleep(15)