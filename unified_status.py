import ccxt
import pandas as pd
import os
import time
from datetime import datetime, date
from config import taker_client, maker_client, path_to_NAS


def get_balance_status(client):

    all_balances = client.fetch_balance()
    for ticker in all_balances['free']:
        print(f"{ticker} {round(all_balances['free'][ticker], 2)} available on {client.name}")


def get_order_status(client, ticker):
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


def download_trades(client, ticker):
    today = str(date.today())
    trades_with_fee = []
    output_path = f'{path_to_NAS}{ticker.split("/")[0]}/{today}_{client.name}.csv'
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
    # clean = pd.read_csv(output_path, header=0, index_col=0)
    clean = pd.read_csv(output_path, index_col=0)
    clean.drop_duplicates(subset='id', inplace=True)
    clean.to_csv(output_path, header=True)


bot_activated = True

if __name__ == '__main__':

    while bot_activated:
        try:
            print(f'Status at {datetime.now()}')

            get_balance_status(maker_client)
            get_balance_status(taker_client)
            get_order_status(maker_client, ticker='ALPH/USDT')
            get_order_status(taker_client, ticker='ALPH/USDT')
            download_trades(maker_client, 'ALPH/USDT')
            download_trades(taker_client, 'ALPH/USDT')
            # get_trades(bitmart_client, 'ALPH/USDT')
            print('Cycle done')
            time.sleep(30)
        except ccxt.ExchangeError:
            print('Exchange error, retrying.')
        except ccxt.RequestTimeout:
            print('Request timeout, retrying.')
