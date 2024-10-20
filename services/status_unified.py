import sys
import os

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from dotenv import dotenv_values

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '/docker/database/', '.env'))
pg_config = dotenv_values(f'..{config_path}')

import ccxt
import time
from datetime import datetime
from notifications import send_email
from status_boiler import get_order_status, get_balance_status, fetch_all_open_orders_client_order_id
from status_clients import all_clients, low_balance_threshold, pair
from accounting_boiler import unaddressed_imbalances
from sql_connector import send_sql_query
from sql_queries import imbalances


email_sent = False
bot_activated = True

if __name__ == '__main__':

    while bot_activated:
        try:
            print(f'Status at {datetime.now()}')
            for client in all_clients:
                if get_balance_status(client, ticker=pair, threshold=low_balance_threshold):
                    print(f'Low balance on {client.name}')
                    if not email_sent:
                        send_email(subject='ALPH Bot URGENT', message=f'Low balance on {client.name}')
                        email_sent = True

            for client in all_clients:
                get_order_status(client, ticker=pair, details=False)

            # Adding imbalance monitoring

            imbalances = send_sql_query(pg_config, imbalances)
            all_open_orders = fetch_all_open_orders_client_order_id(pair, *all_clients)

            unaddressed_imbalances(pair, imbalances, all_open_orders)

            print('Cycle done')

            time.sleep(30)
            os.system('clear')
        except ccxt.ExchangeError as e:
            print('Exchange error, retrying.')
            print(e)
        except ccxt.RequestTimeout as e:
            print('Request timeout, retrying.')
            print(e)
        except ccxt.NetworkError as e:
            print('Network error, retrying.')
            print(e)
