import ccxt
import pandas as pd
import os
import time
from datetime import datetime, date
from config import taker_client, maker_client, path_to_NAS, pair, low_balance_threshold
from notifications import send_email


def get_balance_status(client, ticker, threshold):

    """This function allows to display the base and quote asset balances in a convenient terminal format.
    Also includes a threshold for email notification which can be set in config."""

    low_balance = False
    all_balances = client.fetch_balance()
    threshold = threshold

    base_asset = ticker.split('/')[0]
    quote_asset = ticker.split('/')[1]

    # Includes minimum threshold for exchanges with leftover balance

    print(f"{base_asset} {round(all_balances['free'][base_asset], 2)} available on {client.name}")
    print(f"{quote_asset} {round(all_balances['free'][quote_asset], 2)} available on {client.name}")

    # Check if currently used balances are below set threshold for notification

    if (all_balances['free'][base_asset] < threshold
            or all_balances['free'][quote_asset] < threshold):
        low_balance = True

    return low_balance


def get_order_status(client, ticker):

    """This function lists all open orders for a client in a terminal format."""

    all_open_orders = client.fetch_open_orders(ticker)
    open_buy_orders_total = 0
    open_sell_orders_total = 0

    for order in all_open_orders:
        if order['side'] == 'buy':
            open_buy_orders_total += order['remaining']
        elif order['side'] == 'sell':
            open_sell_orders_total += order['remaining']

        print(f"Open {order['side']} order on {client.name} at {order['price']}, "
              f"{round(order['remaining'], 2)} of {round(order['amount'], 2)} remaining.")
    if open_buy_orders_total != 0:
        print(f"Total of {round(open_buy_orders_total, 2)} buys open on {client.name}.")
    if open_sell_orders_total != 0:
        print(f"Total of {round(open_sell_orders_total, 2)} sells open on {client.name}.")


def download_trades(client, ticker, production=True):

    """This function downloads all latest trades from a client to a csv file on the set path to NAS.
    Includes a production flag, to instead export to test folder if set to False."""

    today = str(date.today())
    trades_with_fee = []

    if production:
        output_path = f'{path_to_NAS}{ticker.split("/")[0]}/{today}_{client.name}.csv'
    else:
        output_path = f'{path_to_NAS}Test/{today}_{client.name}.csv'

    # Bitmart doesn't support queries for over 200 last trades.

    if client.name == 'BitMart':
        trades = client.fetch_my_trades(symbol=ticker, limit=200)
    else:
        trades = client.fetch_my_trades(symbol=ticker, limit=1000)

    for trade in trades:
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

            get_order_status(maker_client, ticker=pair)
            get_order_status(taker_client, ticker=pair)
            download_trades(maker_client, pair)
            download_trades(taker_client, pair)
            # get_trades(bitmart_client, 'ALPH/USDT')
            print('Cycle done')

            time.sleep(30)
        except ccxt.ExchangeError as e:
            print('Exchange error, retrying.')
            print(e)
        except ccxt.RequestTimeout as e:
            print('Request timeout, retrying.')
            print(e)
