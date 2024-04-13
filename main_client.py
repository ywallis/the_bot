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
exchange_with_lowest_price = ''
exchange_with_highest_price = ''
current_ticker = 'ALPH/USDT'

bot_activated = True
funds_low_email_sent = False

while bot_activated:
    try:
        if target == 1:
            all_prices, exchange_with_lowest_price, exchange_with_highest_price = overwatch_mexc(pair=current_ticker)
        if target == 2:
            all_prices, exchange_with_lowest_price, exchange_with_highest_price = overwatch_bitmart(pair=current_ticker)

        if all_prices[exchange_with_highest_price] >= all_prices[exchange_with_lowest_price] * min_spread:

            bids = retrieve_books(exchange_with_highest_price, side='bids', ticker=current_ticker)
            asks = retrieve_books(exchange_with_lowest_price, side='asks', ticker=current_ticker)

            # Placeholder for exchange and direction dependent spread/sizing definition

            try:
                target_ask, target_bid, order_size = order_book_matcher(bids, asks,
                                                                        spread=min_spread, sizing=current_sizing,
                                                                        max_order_size=current_max_order_size)
                if check_if_solvent(buy_exchange=exchange_with_lowest_price, sell_exchange=exchange_with_highest_price,
                                    quantity=order_size, price=target_ask):
                    try:
                        place_sell_order(current_ticker, exchange_with_highest_price, target_bid, order_size)
                        place_buy_order(current_ticker, exchange_with_lowest_price, target_ask, order_size)
                        # send_email(subject='ALPH Bot', message=f'{exchange_with_highest_price} higher than '
                        #                                        f'{exchange_with_lowest_price}.'
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
