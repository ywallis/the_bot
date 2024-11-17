import ccxt.pro as ccxt
import asyncio
from datetime import datetime
import redis
import json
from config.env_var import mexc_secret_test, mexc_key_test, gateio_secret_test, gateio_key_test


gate = ccxt.gateio({'apiKey': gateio_key_test, 'secret': gateio_secret_test})
mexc = ccxt.mexc({'apiKey': mexc_key_test, 'secret': mexc_secret_test})
ticker = 'ALPH/USDT'

async def watch_ob(client, pair):
    while True:
        try:
            order_book = await client.watch_order_book(pair)
            print(f'{datetime.now()} Bid {order_book["bids"][0][0]} and ask {order_book["asks"][0][0]} on {client.name}')
            # print(order_book)
            # store_list(f'{client.name}-{pair}', order_book)
            store_list('test', order_book)


        except Exception as e:
            print(str(e))
            break
    await client.close()

def store_list(key, my_list):
    # Serialize the list into a JSON string
    serialized_list = json.dumps(my_list)
    r.set(key, serialized_list)

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

if __name__ == '__main__':
    asyncio.run(watch_ob(gate, ticker))