import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from datetime import date, datetime
import logging

import ccxt.async_support as ccxt # type: ignore
import asyncio
import threading
import redis
from async_support.make1_async import make_and_take
from config.option_picker import strategy_picker, maker_client_picker
from config.min_max_usd_converter import min_max_usd_converter
from async_support.arb_client_maker import arb_client_maker
from services.heartbeat import heartbeat_receiver
import time

##### THIS SECTION WILL BE REPLACED BY A PARSER
strategy = strategy_picker()
maker_client_id, maker_client_index = maker_client_picker(strategy)

# Using getattr to use id from strategy to generate client

taker_client, maker_client = arb_client_maker(strategy, maker_client_id, maker_client_index)

pair = strategy['pair']


# Get exchange-specific settings

instance_config = strategy['maker_exchanges'][maker_client_index]['settings']



today = str(date.today())
now = datetime.now()

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG,
                    filename=f'../Logs/{today}_maker_{strategy['name']}_{maker_client.name}.txt')
logger = logging.getLogger(__name__)

# Initializing redis instance

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


async def main_loop(event):

    # Loop is driven by heartbeat

    receiver_thread = threading.Thread(target=heartbeat_receiver, daemon=True, args=(event, strategy['name'] + strategy['maker_exchanges'][maker_client_index]['id'],))
    receiver_thread.start()

    # Convert min/max settings from USD to base asset
    ticker_info = await taker_client.fetch_ticker(pair)
    current_usd_value = ticker_info['last']
    instance_config_usd = min_max_usd_converter(current_usd_value, instance_config)

    while not event.is_set():
        try:
            await make_and_take(taker_client, maker_client, instance_config_usd, pair, stop_event, r)

        except ccxt.NetworkError as e:
            print('Main loop level Network error')
            logger.info('Main loop level Network error')
            logger.info(e)

        except ccxt.ExchangeError as e:

            # Has happened because of too many requests.
            time.sleep(10)
            print('Main loop level Exchange error')
            logger.info('Main loop level Exchange error')
            logger.info(e)

        except TypeError as e:
            print('Main loop level type error, probably coroutine')
            logger.info('Main loop level type error, probably coroutine')
            logger.info(e)
        except RuntimeError as e:
            print('Main loop level Runtime error')
            logger.info('Main loop level Runtime error')
            logger.info(e)

if __name__ == '__main__':
    stop_event = threading.Event()
    print(f'Running strategy {strategy['pair']}, production is {str(strategy['production'])}')
    asyncio.run(main_loop(stop_event))
