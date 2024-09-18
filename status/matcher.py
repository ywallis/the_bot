from config.config import gate_client, mexc_client, bitget_client, pair
import pandas as pd

### First version, fetches all open orders and exports them to a csv with matching-relevant information.

orders = []

clients = [gate_client, mexc_client, bitget_client]

for client in clients:
    client_orders = client.fetch_open_orders(pair)

    for order in client_orders:
        formatted = {'id': order['clientOrderId'], 'price': order['price'], 'amount': order['amount'],
                     'side': order['side'],
                     'remaining': order['remaining'], 'exchange': client.name}
        orders.append(formatted)
        print(formatted)

df = pd.DataFrame(orders)
df.set_index('id', inplace=True)
print(df)

# Export if manual needed

#df.to_csv('matching.csv')

# for item in df.index:
#     print(item)
