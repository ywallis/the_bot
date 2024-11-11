import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

import asyncio
import ccxt.pro
from boiler_ws import process_order_update
from services.heartbeat import heartbeat_sender
import threading
import copy
from config.option_picker import strategy_picker, maker_client_picker
from arb_client_maker import arb_client_maker
from config.env_var import mexc_key, mexc_secret, bitget_key, bitget_secret, bitget_password, gateio_secret, gateio_key

from datetime import datetime, timezone



async def loop(maker_client, taker_client, symbol):
    since = datetime.now(timezone.utc)
    timestamp = int(since.timestamp() * 1000)
    while True:
        orders = await maker_client.watch_orders(symbol, since=timestamp)
        print('--------------------------------------------------------------')
        print('Received', len(orders), 'after', maker_client.iso8601 (timestamp))
        print(orders)

        for order in orders:
            with open('all_orders.txt', 'a') as file:
                file.write(f'\n{str(order)}')

            order_copy = copy.deepcopy(order)
            asyncio.create_task(process_order_update(taker_client, order_copy))

        print('waiting for next update...')


##### THIS SECTION WILL BE REPLACED BY A PARSER
strategy = strategy_picker()
maker_client_id, maker_client_index = maker_client_picker(strategy)

# Using getattr to use id from strategy to generate client

taker_client, maker_client = arb_client_maker(strategy, maker_client_id, maker_client_index)

pair = strategy['pair']


# Get exchange-specific settings

instance_config = strategy['maker_exchanges'][maker_client_index]['settings']

async def main():
    try:
        await loop(maker_client, taker_client, pair)
    except ccxt.NetworkError as e:
        print('Network error, ghetto logging.')
        with open('disconnect.txt', 'a') as file:
            file.write(f'\n{e}')
    await maker_client.close()


gate_fee = 0.001


if __name__ == '__main__':

    # Create the heartbeat sender as a daemon thread, uses strategy name + exchange for heartbeat
    sender_thread = threading.Thread(target=heartbeat_sender, daemon=True, args=(strategy['name'] + strategy['maker_exchanges'][maker_client_index]['id'],))
    sender_thread.start()

    # Start main async loop in main thread
    asyncio.run(main())