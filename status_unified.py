import ccxt
import time
from datetime import datetime
from config import pair, low_balance_threshold, gate_client, mexc_client, bitget_client
from notifications import send_email
from status_boiler import get_order_status, get_balance_status, download_orders, download_trades


email_sent = False
bot_activated = True

if __name__ == '__main__':

    while bot_activated:
        try:
            print(f'Status at {datetime.now()}')

            if get_balance_status(gate_client, ticker=pair, threshold=low_balance_threshold):
                print(f'Low balance on {gate_client.name}')
                if not email_sent:
                    send_email(subject='ALPH Bot URGENT', message=f'Low balance on {gate_client.name}')
                    email_sent = True
            if get_balance_status(mexc_client, ticker=pair, threshold=low_balance_threshold):
                print(f'Low balance on {mexc_client.name}')
                if not email_sent:
                    send_email(subject='ALPH Bot URGENT', message=f'Low balance on {mexc_client.name}')
                    email_sent = True
            if get_balance_status(bitget_client, ticker=pair, threshold=low_balance_threshold):
                print(f'Low balance on {bitget_client.name}')
                if not email_sent:
                    send_email(subject='ALPH Bot URGENT', message=f'Low balance on {bitget_client.name}')
                    email_sent = True

            # Switch to direct exchange clients whenever possible.

            get_order_status(gate_client, ticker=pair)
            get_order_status(mexc_client, ticker=pair)
            get_order_status(bitget_client, ticker=pair)
            download_trades(gate_client, pair, False)
            download_trades(mexc_client, pair, False)
            download_trades(bitget_client, pair, False)
            download_orders(gate_client, pair, False)
            download_orders(mexc_client, pair, False)
            download_orders(bitget_client, pair, False)

            print('Cycle done')

            time.sleep(30)
        except ccxt.ExchangeError as e:
            print('Exchange error, retrying.')
            print(e)
        except ccxt.RequestTimeout as e:
            print('Request timeout, retrying.')
            print(e)
