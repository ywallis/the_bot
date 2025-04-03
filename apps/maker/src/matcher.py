import asyncio

from datetime import datetime, timezone
import logging
import apps.maker.src.logging_config as logging_config
from apps.maker.src.enums import OrderSide
from apps.maker.src.strategies.utils import (
    generate_order_replace,
    maker_order_sizer,
    min_max_usd_converter,
    retrieve_ob_redis,
    send_processor_cancellation,
    within_percentage_range,
)
from apps.maker.src.structs import OrderMessage, CustomExchange
from apps.maker.src.errors import NetworkError 

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# TODO: 
# Watch all orders.
# Parse OID
# If strat should be matched
# and coming from maker exchange
# match 
async def loop(client: CustomExchange):
    since = datetime.now(timezone.utc)
    timestamp = int(since.timestamp() * 1000)
    recently_processed_orders = LimitedSet(100)
    while True:
        try:
            orders = await client.watch_orders(pair, since=timestamp)
            orders_copy = copy.deepcopy(orders)
            # print('--------------------------------------------------------------')
            # print(f'Received {len(orders_copy)} orders at {datetime.now(timezone.utc)} on {client.name}')
            # print(orders_copy)

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
            await client.close()

async def main():
    while True:
        try:
            await asyncio.gather(*[loop(client) for client in maker_clients])
        except NetworkError as e:
            print('Network error, logging.')
            logger.error('Network error')
            logger.error(e)

if __name__ == '__main__':
    asyncio.run(main())
