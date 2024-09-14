from datetime import date, datetime
import logging
from config import *
from make1 import make_and_take
import time

today = str(date.today())
now = datetime.now()

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG,
                    filename=f'./Logs/{today}_maker_{maker_client.name}.txt')
logger = logging.getLogger(__name__)

making = True

if __name__ == '__main__':

    while making is True:
        try:
            make_and_take(taker_client=taker_client, maker_client=maker_client, pair=pair, maker_spread=maker_spread,
                          maker_size=maker_size, taker_spread=taker_min_spread,
                          taker_sizing=taker_sizing, taker_max_order_size=taker_max_order_size, taker_only=taker_only,
                          spread_extension=spread_extension)

        except ccxt.NetworkError as e:
            print('Network error')
            logger.info('Network error')
            logger.info(e)

        except ccxt.ExchangeError as e:

            # Has happened because of too many requests.
            time.sleep(5)
            print('Exchange error')
            logger.info(e)


# In case of emergencies, kill all open orders on maker client.

# Replace this by an entry in config for selection?
# maker_client.cancel_all_orders(pair)
