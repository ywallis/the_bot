import pandas as pd
import numpy as np
from config.config import path_to_data, pair

# Load Trades

trades = pd.read_csv(f'{path_to_data}/Trades/all_trades.csv', index_col=0,
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

orders = pd.read_csv(f'{path_to_data}/Orders/all_orders.csv', index_col=0,
                     dtype={"id": "object",
                            "clientOrderId": "object",
                            "timestamp": "int64",
                            "lastTradeTimestamp": "float64",
                            "status": "object",
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