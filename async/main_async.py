import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from datetime import date, datetime
import logging

import ccxt.async_support as ccxt
import asyncio
from make1_async import make_and_take
from config.option_picker import strategy_picker
import time

##### THIS SECTION WILL BE REPLACED BY A PARSER
strategy = strategy_picker()

# Using getattr to use id from strategy to generate client

taker_client = getattr(ccxt, strategy['taker_exchange']['id'])({'apiKey': strategy['taker_exchange']['key'],
                                                                'secret': strategy['taker_exchange']['secret']})

maker_client = ccxt.mexc({'apiKey': strategy['maker_exchanges'][0]['key'],
                          'secret': strategy['maker_exchanges'][0]['secret']})

instance_config = strategy['maker_exchanges'][0]['settings']

pair = strategy['pair']
######

today = str(date.today())
now = datetime.now()

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG,
                    filename=f'../Logs/{today}_maker_{maker_client.name}.txt')
logger = logging.getLogger(__name__)


async def main_loop():
    making = True

    while making is True:
        try:
            await make_and_take(taker_client, maker_client, instance_config, pair)

        except ccxt.NetworkError as e:
            print('Main loop level Network error')
            logger.info('Main loop level Network error')
            logger.info(e)

        except ccxt.ExchangeError as e:

            # Has happened because of too many requests.
            time.sleep(5)
            print('Main loop level Exchange error')
            logger.info('Main loop level Exchange error')
            logger.info(e)

        except RuntimeError as e:
            print('Main loop level Runtime error')
            logger.info('Main loop level Runtime error')
            logger.info(e)

if __name__ == '__main__':
    print(f'Running strategy {strategy['pair']}, production is {str(strategy['production'])}')
    asyncio.run(main_loop())
