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
import logging
from datetime import date
from limited_set import LimitedSet
from ws_clients import taker_client, maker_clients, strategy, pair


from datetime import datetime, timezone



# ##### THIS SECTION WILL BE REPLACED BY A PARSER
# strategy = strategy_picker()
# maker_client_id, maker_client_index = maker_client_picker(strategy)
#
# # Using getattr to use id from strategy to generate client
#
# taker_client, maker_client = arb_client_maker(strategy, maker_client_id, maker_client_index)
#
# pair = strategy['pair']


# Get exchange-specific settings

# instance_config = strategy['maker_exchanges'][maker_client_index]['settings']
for exchange in strategy['maker_exchanges']:
    if not exchange['settings']['ws_matcher_active']:
        print('Matcher should not be active! Check config. Stopping.')
        sys.exit()

today = str(date.today())
now = datetime.now()

if strategy['production']:

    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.WARNING,
                        filename=f'../Logs/{today}_matcher_{strategy['name']}.txt')

else:
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG,
                        filename=f'../Logs/{today}_matcher_{strategy['name']}.txt')

logger = logging.getLogger(__name__)

async def loop(client):
    since = datetime.now(timezone.utc)
    timestamp = int(since.timestamp() * 1000)
    recently_processed_orders = LimitedSet(100)
    while True:
        try:
            orders = await client.watch_orders(pair, since=timestamp)
            orders_copy = copy.deepcopy(orders)
            print('--------------------------------------------------------------')
            print(f'Received {len(orders_copy)} orders at {datetime.now(timezone.utc)} on {client.name}')
            print(orders_copy)

            for order in orders_copy:

                logger.info(f'Processing orders from {client.name}')
                logger.info(order)
                # print('TESTING, MATCHING TURNED OFF!')

                # Creating deep copy of order before processing to avoid mutating.

                order_copy = copy.deepcopy(order)

                # Checking if the order: Is not open, has been at least partially filled, and whether the id had been processed recently.

                if order_copy['status'] != 'open':
                    if order_copy['filled'] != 0:
                        if order_copy.get('id') not in recently_processed_orders:
                            recently_processed_orders.add(order_copy.get('id'))
                            asyncio.create_task(process_order_update(taker_client, order_copy))
                        else:
                            logger.warning(f'The order no {order_copy['id']} tried getting matched multiple times.')

            print('waiting for next update...')

        except Exception as e:
            logger.error(f'Error in client loop {e}')

async def main():

    while True:
        try:
            await asyncio.gather(*[loop(client) for client in maker_clients])
        except ccxt.NetworkError as e:
            print('Network error, logging.')
            logger.error('Network error')
            logger.error(e)

    # await maker_client.close()


if __name__ == '__main__':

    # Create the heartbeat sender as a daemon thread, uses strategy name + exchange for heartbeat
    sender_thread = threading.Thread(target=heartbeat_sender, daemon=True, args=(strategy['name'], strategy['zmq_port'],))
    sender_thread.start()

    # Start main async loop in main thread
    asyncio.run(main())