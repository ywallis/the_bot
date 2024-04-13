import logging
from datetime import datetime
import time

# from notifications import send_email

from config import *

logger = logging.getLogger(__name__)


# Retrieve current gate.io fee

gate_fee = gate_client.fetch_trading_fee(symbol='ALPH/USDT')['taker']


def retrieve_books(client, side, ticker):

    return client.fetch_order_book(symbol=ticker)[side]


def order_book_matcher(bids, asks, spread=1.002, sizing=0.7, max_order_size=10, min_order_size=0.01):

    dynamic_arb = True
    bid_counter = 0
    ask_counter = 0

    while dynamic_arb:

        highest_bid = bids[bid_counter]

        highest_bid_price = float(highest_bid[0])
        highest_bid_quantity = float(highest_bid[1])

        lowest_ask = asks[ask_counter]

        lowest_ask_price = float(lowest_ask[0])
        lowest_ask_quantity = float(lowest_ask[1])

        # Define arbitrage condition

        if highest_bid_price >= lowest_ask_price * spread:
            print("This should be arbed.")
            logger.info("This should be arbed.")

            target_ask = float(asks[(ask_counter + 2)][0])
            target_bid = float(bids[(bid_counter + 2)][0])
            order_size = round(min(highest_bid_quantity, lowest_ask_quantity) * sizing, 2)

            print(f'Optimal spread currently between {lowest_ask_price} and {highest_bid_price}.'
                  f'\nTargeting a sell for {target_bid} and a buy for {target_ask} with a quantity of {order_size}.')
            logger.info(f'Optimal spread currently between {lowest_ask_price} and {highest_bid_price}.'
                        f'\nTargeting a sell for {target_bid} and a buy for {target_ask} '
                        f'with a quantity of {order_size}.')

            if order_size < min_order_size:
                order_size = min_order_size
                print(f'Below minimum order size, increasing to {min_order_size}.')
                logger.info(f'Below minimum order size, increasing to {min_order_size}.')
            if order_size > max_order_size:
                order_size = max_order_size
                print(f'Over maximum order size, lowering to {max_order_size}.')
                logger.info(f'Over maximum order size, lowering to {max_order_size}.')

            total_order_value = order_size * target_ask

            if total_order_value < 10.1:
                print('Order value too small.')
                logger.info('Order value too small.')
                if lowest_ask_quantity <= highest_bid_quantity:
                    ask_counter += 1
                    print('Moving up asks.')
                    logger.info('Moving up asks')
                elif highest_bid_quantity < lowest_ask_quantity:
                    bid_counter += 1
                    print('Moving up bids.')
                    logger.info('Moving up bids')

            else:

                # THIS IS THE CRUCIAL PART TO EXTRACT FROM THE FUNCTION

                return target_ask, target_bid, order_size

                # THIS IS THE CRUCIAL PART TO EXTRACT FROM THE FUNCTION

        else:
            print("Spread too thin, consider lower settings.")
            logger.info("Spread too thin, consider lower settings.")
            dynamic_arb = False


def place_sell_order(pair, client, price, quantity):

    client.create_limit_sell_order(symbol=pair, amount=quantity, price=price)

    print(f'Placed a {quantity} {pair} sell order on {client.name} for {price}.')
    logger.info(f'Placed a {quantity} {pair} sell order on {client.name} for {price}.')


def place_buy_order(pair, client, price, quantity):

    if client.name == 'Gate.io':

        # Apply current fee level
        fee_ratio = 1 / (1 - gate_fee)

        quantity_with_fee = round(quantity * fee_ratio, 2)

        client.create_limit_buy_order(symbol=pair, amount=quantity_with_fee, price=price)

    else:

        client.create_limit_buy_order(symbol=pair, amount=quantity, price=price)

    print(f'Placed a {quantity} {pair} buy order on {client.name} for {price}.')
    logger.info(f'Placed a {quantity} {pair} buy order on {client.name} for {price}.')


def retrieve_balance(client, ticker):

    return client.fetch_balance()[ticker]['free']


def check_if_solvent(buy_exchange, sell_exchange, price, quantity):

    if quantity * price * 1.02 < retrieve_balance(buy_exchange, 'USDT') and quantity * 1.02 < retrieve_balance(sell_exchange, 'ALPH'):
        return True
    else:
        return False


def overwatch(client_a, client_b, pair, spread):
    # Manual rate limiting for BitMart
    if client_b.name == 'BitMart':
        time.sleep(1)

    ticker_a = client_a.fetch_ticker(pair)
    ticker_b = client_b.fetch_ticker(pair)
    last_a = ticker_a['last']
    bid_a = ticker_a['bid']
    ask_a = ticker_a['ask']
    last_b = ticker_b['last']
    bid_b = ticker_b['bid']
    ask_b = ticker_b['ask']

    all_prices = {client_a.name: last_a, client_b.name: last_b}
    lowest = min(all_prices, key=all_prices.get)
    highest = max(all_prices, key=all_prices.get)

    spread = round((all_prices[highest] / all_prices[lowest] - 1) * 100, 2)

    print(f'Watching at {datetime.now()}.'
          f'\nLowest price on {lowest} for {all_prices[lowest]}, '
          f'highest on {highest} for {all_prices[highest]} ({spread}%).')

    if bid_b >= ask_a * spread:
        return client_a, client_b

    if bid_a >= ask_b * spread:
        return client_b, client_a


# buy_client, sell_client = overwatch(gate_client, mexc_client, 'ALPH/USDT', 1)
# print(f'Buy on {buy_client.name}, sell on {sell_client.name}')
#
# bids = retrieve_books(sell_client, 'bids', 'ALPH/USDT')
# asks = retrieve_books(buy_client, 'asks', 'ALPH/USDT')
#
#
# target_ask, target_bid, order_size = order_book_matcher(bids, asks, spread=1)
# print(target_ask, target_bid, order_size)
