from datetime import date

import pandas as pd

from config.config import path_to_data, pair

pd.options.mode.copy_on_write = True

# Load Trades

def load_db(start=None):

    # Start is here to make sure previous values don't "leak" into the new data.
    if start is None:
        start = str(date.today())[:-3]

    trades = pd.read_csv(f'{path_to_data}{pair.split("/")[0]}/Trades/all_Trades.csv', index_col=0,
                         dtype={'info': 'object',
                                'id': 'object',
                                'timestamp': 'int64',
                                'symbol': 'object',
                                'order': 'object',
                                'type': 'object',
                                'side': 'object',
                                'takerOrMaker': 'object',
                                'fee': 'object',
                                })

    trades['datetime'] = pd.to_datetime(trades['timestamp'], unit='ms')
    trades.set_index('datetime', inplace=True)
    trades.sort_index(inplace=True, ascending=False)
    trades['date'] = trades.index.date
    data_clean = trades[['side', 'exchange', 'takerOrMaker', 'amount', 'price', 'cost', 'fee_currency', 'fee_cost', 'date', 'order']]

    # Load Orders

    orders = pd.read_csv(f'{path_to_data}{pair.split("/")[0]}/Orders/all_Orders.csv', index_col=0,
                         dtype={"id": "object",
                                "clientOrderId": "object",
                                "timestamp": "int64",
                                "lastTradeTimestamp": "float64",
                                "services": "object",
                                "symbol": "object",
                                "type": "object",
                                "timeInForce": "object",
                                "side": "object",
                                "price": "float64",
                                "stopPrice": "float64",
                                "triggerPrice": "float64",
                                "average": "float64",
                                "amount": "float64",
                                "cost": "float64",
                                "filled": "float64",
                                "remaining": "float64",
                                "fee": "object",
                                "trades": "object",
                                "info": "object",
                                "fees": "object",
                                "lastUpdateTimestamp": "float64",
                                "postOnly": "float64",
                                "reduceOnly": "float64",
                                "takeProfitPrice": "float64",
                                "stopLossPrice": "float64",
                                "exchange": "object",
                                "fee_cost": "object",
                                "fee_currency": "object",
                                "date": "object",
                                })

    orders['datetime'] = pd.to_datetime(orders['timestamp'], unit='ms')
    orders.set_index('datetime', inplace=True)
    orders.sort_index(inplace=True, ascending=False)
    orders['date'] = orders.index.date
    orders.rename(columns={'id': 'order'}, inplace=True)
    orders_clean = orders[['order', 'clientOrderId']]

    data_clean.sort_index(inplace=True)

    # Define functions to generate usdt value and net quantity after execution, and apply to data

    def usdt_value(row):
        if row['fee_currency'] != 'USDT':
            return row['cost']
        elif row['side'] == 'buy':
            return row['cost'] + row['fee_cost']
        else:
            return row['cost'] - row['fee_cost']

    data_clean['usdt_value'] = data_clean.apply(usdt_value, axis=1)

    def asset_net_q(row):
        if row['fee_currency'] != 'USDT':
            return row['amount'] - row['fee_cost']
        else:
            return row['amount']

    data_clean['asset_net_q'] = data_clean.apply(asset_net_q, axis=1)

    data_clean_prep = data_clean.reset_index()
    detailed = pd.merge(data_clean_prep, orders_clean, on='order', how='left')
    detailed.set_index('datetime', inplace=True)

    return detailed[start :]

def find_imbalance(single_date=None, df=load_db()):

    detailed = df

    all_buys_by_id = detailed[detailed['side'] ==  'buy']
    all_sells_by_id = detailed[detailed['side'] ==  'sell']

    all_sells_performance_id = all_sells_by_id.groupby('clientOrderId').sum('usdt_value')['usdt_value']
    all_buys_performance_id = all_buys_by_id.groupby('clientOrderId').sum('usdt_value')['usdt_value']
    performance_id = pd.concat([all_sells_performance_id, all_buys_performance_id], axis=1)
    performance_id.fillna(0, inplace=True)
    performance_id.columns = ['All sells', 'All buys']
    performance_id['Net Gain'] = performance_id['All sells'] - performance_id['All buys']

    all_sells_inventory_id = all_sells_by_id.groupby('clientOrderId').sum('asset_net_q')['asset_net_q']
    all_buys_inventory_id = all_buys_by_id.groupby('clientOrderId').sum('asset_net_q')['asset_net_q']
    inventory_id = pd.concat([all_sells_inventory_id, all_buys_inventory_id], axis=1)
    inventory_id.columns = ['All sells', 'All buys']
    inventory_id.fillna(0, inplace=True)
    inventory_id['Net Gain'] = inventory_id['All buys'] - inventory_id['All sells']

    # This isolates trade combinations which have a large (2? make user definable) imbalance,
    # and either should have an associated open order or the need for one.

    need_matching = inventory_id.loc[(inventory_id['Net Gain'] < -2 ) | (inventory_id['Net Gain'] > 2 )]

    # print(need_matching)

    output = {}

    for index, row in need_matching.iterrows():
        if index.startswith(f't-{single_date}'):
            amount = round(float(row['Net Gain']), 2)
            output[index] = amount

    # print(output)
    return output
