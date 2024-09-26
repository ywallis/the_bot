import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from datetime import date, datetime
import logging

import ccxt.async_support as ccxt
import asyncio
from make1_async import make_and_take
from config.config import pair, gateio_key_test, gateio_secret_test, mexc_key_test, mexc_secret_test

import time

# Move to config after testing

gate_client = ccxt.gateio({'apiKey': gateio_key_test, 'secret': gateio_secret_test})
mexc_client = ccxt.mexc({'apiKey': mexc_key_test, 'secret': mexc_secret_test})

instance_config = {'taker_client': gate_client,
                   'maker_client': mexc_client,
                   'maker_spread': 1.002,
                   'maker_size': 10,
                   'taker_spread': 1.0035,
                   'taker_sizing': 0.8,
                   'taker_max_order_size': 10,
                   'taker_only': False,
                   'spread_extension': 2,
                   }

######

today = str(date.today())
now = datetime.now()

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG,
                    filename=f'../Logs/{today}_maker_{instance_config['maker_client'].name}.txt')
logger = logging.getLogger(__name__)

making = True

if __name__ == '__main__':

    while making is True:
        try:
            asyncio.run(make_and_take(instance_config, pair))

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