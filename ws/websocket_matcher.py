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
# from config.option_picker import strategy_picker, maker_client_picker
# from arb_client_maker import arb_client_maker
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
    while True:
        orders = await client.watch_orders(pair, since=timestamp)
        print('--------------------------------------------------------------')
        print(f'Received {len(orders)} orders at {datetime.now(timezone.utc)} on {client.name}')
        print(orders)

        for order in orders:

            logger.info(f'Processing orders from {client.name}')
            logger.info(order)

            # Creating deep copy of order before processing to avoid mutating.

            order_copy = copy.deepcopy(order)
            asyncio.create_task(process_order_update(taker_client, order_copy))

        print('waiting for next update...')

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