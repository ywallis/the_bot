from config.config import gate_client, mexc_client, bitget_client, pair
import pandas as pd
from status_boiler import fetch_all_open_orders_client_order_id
from load_db import find_imbalance

### First version, fetches all open orders and exports them to a csv with matching-relevant information.

# orders = []
#
# clients = [gate_client, mexc_client, bitget_client]
#
# for client in clients:
#     client_orders = client.fetch_open_orders(pair)
#
#     for order in client_orders:
#         formatted = {'id': order['clientOrderId'], 'price': order['price'], 'amount': order['amount'],
#                      'side': order['side'],
#                      'remaining': order['remaining'], 'exchange': client.name}
#         orders.append(formatted)
#         print(formatted)
#
# df = pd.DataFrame(orders)
# df.set_index('id', inplace=True)
# print(df)

# Export if manual needed

#df.to_csv('matching.csv')

# for item in df.index:
#     print(item)

imbalances = find_imbalance()
orders = fetch_all_open_orders_client_order_id(pair, gate_client, mexc_client, bitget_client)

for _ in imbalances.keys():
    print(_)
    if _ in orders:
        print('This is pending')
    else:
        print('This is an issue')
        if imbalances[_] < 0:
            print(f'{imbalances[_]} imbalance, should buy!')
        else:
            print(f'{imbalances[_]} imbalance, should buy!')
