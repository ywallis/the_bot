import logging
import time
from datetime import date, datetime

import requests.exceptions

from boiler import *
from notifications import send_email
from config import *

today = str(date.today())
now = datetime.now()

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG, filename=f'./Logs/{today}.txt')
logger = logging.getLogger(__name__)

all_prices = {}
buy_client = None
sell_client = None

if __name__ == '__main__':
    bot_activated = True
    funds_low_email_sent = False

    while bot_activated:
        try:
            buy_client, sell_client = overwatch(client_a, client_b, current_ticker, min_spread)
            bids = retrieve_books(sell_client, side='bids', ticker=current_ticker)
            asks = retrieve_books(buy_client, side='asks', ticker=current_ticker)

            # Placeholder for exchange and direction dependent spread/sizing definition

            try:
                target_ask, target_bid, order_size = order_book_matcher(bids, asks,
                                                                        spread=min_spread, sizing=current_sizing,
                                                                        max_order_size=current_max_order_size)
                if check_if_solvent(buy_exchange=buy_client, sell_exchange=sell_client,
                                    quantity=order_size, price=target_ask):
                    try:

                        place_sell_order(current_ticker, sell_client, target_bid, order_size)
                        place_buy_order(current_ticker, buy_client, target_ask, order_size)
                        # send_email(subject='ALPH Bot', message=f'{sell_client} higher than '
                        #                                        f'{buy_client}.'
                        #                                        f'\n {order_size} ALPH orders placed for '
                        #                                        f'{target_ask} and {target_bid}')
                    except RuntimeError:
                        print("Going too fast.")
                        time.sleep(10)

                    except requests.exceptions.HTTPError:
                        print('HTTP Error')
                else:
                    print('Insufficient funds!')
                    logger.info('Insufficient funds!')
                    if not funds_low_email_sent:
                        send_email(subject='ALPH Bot URGENT', message=f'Funds too low!')
                        funds_low_email_sent = True

            except TypeError:
                print("Could not match order books.")
                logger.info("Could not match order books.")

        except KeyError:
            print('Error retrieving prices')
            logger.info('Error retrieving prices')

        except requests.exceptions.HTTPError:
            print('HTTP Error')
