from datetime import date
import os
import pandas as pd
from config import path_to_NAS
from make1 import gate_take, mexc_maker


def download_trades(client, ticker):
    today = str(date.today())
    trades_with_fee = []
    output_path = f'{path_to_NAS}Test/{today}_{client.name}.csv'
    trades = client.fetch_my_trades(symbol=ticker, limit=1000)

    for trade in trades:
        for fee in trade['fees']:
            if fee['cost'] != '0.0':
                trade['fee_cost'] = fee['cost']
                trade['fee_currency'] = fee['currency']
                trade['exchange'] = client.name
                trades_with_fee.append(trade)

    df = pd.DataFrame.from_dict(trades_with_fee)
    df.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
    clean = pd.read_csv(output_path, header=0, index_col=0)
    clean.drop_duplicates(subset='id', inplace=True)
    clean.to_csv(output_path, header=True)


download_trades(gate_take, 'ALPH/USDT')
download_trades(mexc_maker, 'ALPH/USDT')
