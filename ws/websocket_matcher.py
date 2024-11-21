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
from config.option_picker import strategy_picker, maker_client_picker
from arb_client_maker import arb_client_maker

from datetime import datetime, timezone



##### THIS SECTION WILL BE REPLACED BY A PARSER
strategy = strategy_picker()
maker_client_id, maker_client_index = maker_client_picker(strategy)

# Using getattr to use id from strategy to generate client

taker_client, maker_client = arb_client_maker(strategy, maker_client_id, maker_client_index)

pair = strategy['pair']


# Get exchange-specific settings

instance_config = strategy['maker_exchanges'][maker_client_index]['settings']

if not instance_config['ws_matcher_active']:
    print('Matcher should not be active! Check config. Stopping.')
    sys.exit()

today = str(date.today())
now = datetime.now()

if strategy['production']:

    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.WARNING,
                        filename=f'../Logs/{today}_matcher_{strategy['name']}_{maker_client.name}.txt')

else:
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG,
                        filename=f'../Logs/{today}_matcher_{strategy['name']}_{maker_client.name}.txt')

logger = logging.getLogger(__name__)

async def loop():
    since = datetime.now(timezone.utc)
    timestamp = int(since.timestamp() * 1000)
    while True:
        orders = await maker_client.watch_orders(pair, since=timestamp)
        print('--------------------------------------------------------------')
        print('Received', len(orders), 'after', maker_client.iso8601 (timestamp))
        print(orders)

        for order in orders:

            logger.info(order)

            order_copy = copy.deepcopy(order)
            asyncio.create_task(process_order_update(taker_client, order_copy))

        print('waiting for next update...')

async def main():

    while True:
        try:
            await loop()
        except ccxt.NetworkError as e:
            print('Network error, logging.')
            logger.error('Network error')
            logger.error(e)

    # await maker_client.close()


if __name__ == '__main__':

    # Create the heartbeat sender as a daemon thread, uses strategy name + exchange for heartbeat
    sender_thread = threading.Thread(target=heartbeat_sender, daemon=True, args=(strategy['name'] + strategy['maker_exchanges'][maker_client_index]['id'],))
    sender_thread.start()

    # Start main async loop in main thread
    asyncio.run(main())