import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from config.config import gate_client, mexc_client, bitget_client, pair
from status_boiler import fetch_all_open_orders_client_order_id
from load_db import find_imbalance, load_db

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
print('Uses a static spread for the time being, please modify.')
spread = 1.003

loop = True
date_mod = ""
while loop:
    date = input('Enter the date (DD/MM/YY) you want to match orders on:')
    date_mod = "".join(date.split("/")[::-1])
    print(date_mod)
    if date_mod != "":
        loop = False
    else:
        print("This can't be empty")

db = load_db()
imbalances = find_imbalance(date_mod, db)
orders = fetch_all_open_orders_client_order_id(pair, gate_client, mexc_client, bitget_client)

for _ in imbalances.keys():
    print(_)
    if _ in orders:
        print('This is pending')
    else:
        if imbalances[_] < 0:
            print(f'{imbalances[_]} imbalance, should buy!')

            # Search through the database for the price the unmatched orders were executed at.

            # First looks at partial fills for price. If they exist, will take the most reachable price

            partial_fills = db.loc[(db['clientOrderId'] == _) & (db['side'] == 'buy')]['price'].values

            if partial_fills.size > 0:
                partial_fills = max(partial_fills)

            # If partial fills don't exist, looks at the price most reachable price on the unmatched order.

            imbalanced_price = db.loc[(db['clientOrderId'] == _) & (db['side'] == 'sell')]['price'].values

            imbalanced_price = min(imbalanced_price)

            if partial_fills.size > 0:
                price = partial_fills
                print('There are partial fills, will use their price.')

            else:
                price = round(imbalanced_price / spread, 3)


            print(f'Creating a matching buy order for order {_} with a quantity of {imbalances[_]} and a price of {price}.')

        else:
            print(f'{imbalances[_]} imbalance, should sell!')

            # Search through the database for the price the unmatched orders were executed at.

            # First looks at partial fills for price

            partial_fills = db.loc[(db['clientOrderId'] == _) & (db['side'] == 'sell')]['price'].values
            if partial_fills.size > 0:
                partial_fills = min(partial_fills)

            # If partial fills don't exist, looks at the price on the unmatched order.

            imbalanced_price = db.loc[(db['clientOrderId'] == _) & (db['side'] == 'buy')]['price'].values
            imbalanced_price = max(imbalanced_price)


            if partial_fills.size > 0:
                price = partial_fills
                print('There are partial fills, will use their price.')

            else:
                price = round(imbalanced_price * spread, 3)

            print(f'Creating a matching sell order for order {_} with a quantity of {imbalances[_]} and a price of {price}.')
