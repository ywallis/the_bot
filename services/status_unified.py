import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

import ccxt
import time
from datetime import datetime
from notifications import send_email
from status_boiler import get_order_status, get_balance_status, download_orders, download_trades
from status_clients import all_clients, low_balance_threshold, pair


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
                get_order_status(client, ticker=pair)
                # download_trades(client, pair)
                # download_orders(client, pair)

            print('Cycle done')

            time.sleep(30)
        except ccxt.ExchangeError as e:
            print('Exchange error, retrying.')
            print(e)
        except ccxt.RequestTimeout as e:
            print('Request timeout, retrying.')
            print(e)
        except ccxt.NetworkError as e:
            print('Network error, retrying.')
            print(e)
