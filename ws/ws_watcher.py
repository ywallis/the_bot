import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

import asyncio
from datetime import datetime, date
import redis
import signal
import json
import logging

from ws.ws_clients import all_clients, pair, strategy

watching = True

def graceful_exit(signum, frame):
    global watching
    print("Signal received. Shutting down gracefully...")
    watching = False

# Register signal handlers
signal.signal(signal.SIGINT, graceful_exit)  # Handle Ctrl+C
signal.signal(signal.SIGTERM, graceful_exit) # Handle termination signals


async def watch_ob(client, ticker):
    while watching:
        try:
            order_book = await client.watch_order_book(ticker)
            message = f'{datetime.now()} Bid {order_book["bids"][0][0]} and ask {order_book["asks"][0][0]} on {client.name}'
            print(message)
            # print(order_book)
            store_ob(f'{pair}-{client.name}', order_book)
            # store_list(f'test-{client.name}', order_book)
            # store_list(f'test-{client.name}', message)

        except Exception as e:
            print(str(e))
            await client.close()


        # finally:
        #     await client.close()


async def main(ticker, *clients):
    # try:
    # await asyncio.gather(*[watch_ob(client, ticker) for client in clients], return_exceptions=True)
    await asyncio.gather(*[watch_ob(client, ticker) for client in clients])

    # # TODO Add exception logging
    # except Exception as e:
    #     print(e)


def store_ob(key, my_ob):
    # Serialize the list into a JSON string
    serialized_ob = json.dumps(my_ob)
    r.set(key, serialized_ob)

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

today = str(date.today())
now = datetime.now()

if strategy['production']:

    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.WARNING,
                        filename=f'../Logs/{today}_watcher_{strategy['name']}.txt')

else:
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG,
                        filename=f'../Logs/{today}_watcher_{strategy['name']}.txt')

logger = logging.getLogger(__name__)


if __name__ == '__main__':

    asyncio.run(main(pair, *all_clients))
