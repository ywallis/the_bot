import ccxt
import time
from datetime import datetime
from config import taker_client, maker_client, pair, low_balance_threshold
from notifications import send_email
from status_boiler import get_order_status, get_balance_status, download_orders, download_trades


email_sent = False
bot_activated = True

if __name__ == '__main__':

    while bot_activated:
        try:
            print(f'Status at {datetime.now()}')

            if get_balance_status(maker_client, ticker=pair, threshold=low_balance_threshold):
                print(f'Low balance on {maker_client.name}')
                if not email_sent:
                    send_email(subject='ALPH Bot URGENT', message=f'Low balance on {maker_client.name}')
                    email_sent = True
            if get_balance_status(taker_client, ticker=pair, threshold=low_balance_threshold):
                print(f'Low balance on {taker_client.name}')
                if not email_sent:
                    send_email(subject='ALPH Bot URGENT', message=f'Low balance on {taker_client.name}')
                    email_sent = True

            # Switch to direct exchange clients whenever possible.

            get_order_status(maker_client, ticker=pair)
            get_order_status(taker_client, ticker=pair)
            download_trades(maker_client, pair, False)
            download_trades(taker_client, pair, False)
            download_orders(maker_client, pair, False)
            download_orders(taker_client, pair, False)
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
