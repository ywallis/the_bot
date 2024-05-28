import ccxt
from config import gate_take, mexc_maker, maker_size
from boiler import check_and_take, check_if_solvent, place_buy_order, place_sell_order
from take_take import take_take
from datetime import datetime, date
import time
import logging

today = str(date.today())

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG, filename=f'./Logs/{today}.txt')
logger = logging.getLogger(__name__)


def make_and_take(taker_client, maker_client, pair, spread):
    logger.info('Testing')
    buy_exists = False
    sell_exists = False
    buy_order = None
    sell_order = None
    watching = True
    buy_arbitrage = False
    sell_arbitrage = False
    open_orders = maker_client.fetch_open_orders(pair)
    open_buy_count = 0
    open_sell_count = 0

    # Check for hanging orders in case of errors

    for order in open_orders:
        if order['side'] == 'buy':
            buy_order = order
            buy_exists = True
            open_buy_count += 1

        if order['side'] == 'sell':
            sell_order = order
            sell_exists = True
            open_sell_count += 1

    # Kill all open orders if more than 1 per side.

    if open_buy_count > 1 or open_sell_count > 1:
        print('Too many orders for current logic, cancelling all!')
        maker_client.cancel_all_orders(pair)
        buy_exists = False
        sell_exists = False

    while watching:

        # Slow watching if no open order

        if not buy_arbitrage and not sell_arbitrage:
            time.sleep(1)

        ticker_a = taker_client.fetch_ticker(pair)
        ticker_b = maker_client.fetch_ticker(pair)
        last_a = ticker_a['last']
        bid_a = ticker_a['bid']
        ask_a = ticker_a['ask']
        last_b = ticker_b['last']
        bid_b = ticker_b['bid']
        ask_b = ticker_b['ask']

        all_prices = {taker_client.name: last_a, maker_client.name: last_b}
        lowest = min(all_prices, key=all_prices.get)
        highest = max(all_prices, key=all_prices.get)

        watch_spread = round((all_prices[highest] / all_prices[lowest] - 1) * 100, 2)

        print(f'Watching at {datetime.now()}.'
              f'\nLowest price on {lowest} for {all_prices[lowest]}, '
              f'highest on {highest} for {all_prices[highest]} ({watch_spread}%).')

        # Include taker logic

        # Start take_take with client_a as buyer, client_b as seller.
        if bid_b >= ask_a * spread:

            try:
                target_ask, target_bid, order_size = take_take(taker_client, maker_client)

                if check_if_solvent(buy_exchange=taker_client, sell_exchange=maker_client,
                                    quantity=order_size, price=target_ask):
                    try:

                        place_sell_order(pair, maker_client, target_bid, order_size)
                        place_buy_order(pair, taker_client, target_ask, order_size)

                    except RuntimeError:
                        print("Going too fast.")
                        time.sleep(10)

                else:
                    print('Insufficient funds!')

                    # # Section for email information
                    # if not funds_low_email_sent:
                    #     send_email(subject='ALPH Bot URGENT', message=f'Funds too low!')
                    #     funds_low_email_sent = True
            except TypeError:
                print("Could not match order books.")

            except AttributeError:
                print('Attribute error')

            except IndexError:
                (print('End of orderbook'))

        if bid_a >= ask_b * spread:

            # Start take_take with client_b as buyer, client_a as seller.

            try:
                target_ask, target_bid, order_size = take_take(maker_client, taker_client)

                if check_if_solvent(buy_exchange=maker_client, sell_exchange=taker_client,
                                    quantity=order_size, price=target_ask):
                    try:

                        place_sell_order(pair, taker_client, target_bid, order_size)
                        place_buy_order(pair, maker_client, target_ask, order_size)

                    except RuntimeError:
                        print("Going too fast.")
                        time.sleep(10)

                else:
                    print('Insufficient funds!')

                    # # Section for email information
                    # if not funds_low_email_sent:
                    #     send_email(subject='ALPH Bot URGENT', message=f'Funds too low!')
                    #     funds_low_email_sent = True
            except TypeError:
                print("Could not match order books.")

            except AttributeError:
                print('Attribute error')

            except IndexError:
                (print('End of orderbook'))

        # Sell side arbitrage

        if ask_b >= ask_a * spread:

            # Introducing parameter for speed control

            sell_arbitrage = True

            # If the flag for an existing sell doesn't exist yet, create a sell order at the bottom ask.

            if not sell_exists:
                print(f'Make on {maker_client.name}')
                print(f'Sell {ask_b}')
                logger.info(f'Sell on {maker_client.name}, sell {ask_b}')

                if check_if_solvent(taker_client, maker_client, ask_b, maker_size):
                    sell_order = maker_client.create_limit_sell_order(symbol=pair, amount=maker_size, price=ask_b)
                    sell_exists = True
                    logger.info(f'Solvent, sell order created')

                else:
                    print('Insufficient funds!')
                    logger.info(f'Insufficient funds!')

            # If the flag for an existing sell order exists, check if it has been filled.

            else:
                print(f"Sell order already present at {sell_order['price']}")
                logger.info(f"Sell order already present at {sell_order['price']}")

                # Check if some of the order has been filled. If yes, the order is cancelled and the flag removed.

                if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                    sell_exists = False
                    logger.info(f"C&T, sell doesn't exist anymore")

                # If the existing order is no longer at the bottom of the asks, try to cancel it and place a new one.

                elif sell_order['price'] != ask_b:
                    print('Order no longer at bottom of asks, cancelling.')
                    logger.info('Order no longer at bottom of asks, cancelling.')

                    try:
                        # If the order is partially filled, start the take process.
                        if maker_client.cancel_order(id=sell_order['id'], symbol=pair)['filled'] != 0:
                            print('Order partially filled! Starting C&T')
                            check_and_take(taker_client, maker_client, sell_order, pair, 'buy')

                        else:
                            check_and_take(taker_client, maker_client, sell_order, pair, 'buy')

                        sell_exists = False

                    except ccxt.BadRequest as error:
                        logger.info(error)
                        print(f'Order has been fully filled, taking {sell_order["amount"]}')

                        if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                            sell_exists = False
                            logger.info('Was filled in the mean time, C&T')

                    if check_if_solvent(taker_client, maker_client, ask_b, maker_size):
                        sell_order = maker_client.create_limit_sell_order(symbol=pair, amount=maker_size, price=ask_b)
                        print(f'Sell {ask_b}')
                        sell_exists = True
                        logger.info('Order no longer at bottom of asks, cancelling.')
        else:

            # The arbitrage condition is no longer there, cancel open orders after checking them.

            sell_arbitrage = False
            logger.info('Arb now false')

            if sell_exists:

                if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                    sell_exists = False
                    logger.info('C&T success, sell no longer exists')

                else:
                    print('No more arb, cancelling sells.')
                    logger.info('No more arb, cancelling sells.')

                    try:

                        # If the order is partially filled, start the take process.

                        if maker_client.cancel_order(id=sell_order['id'], symbol=pair)['filled'] != 0:
                            print('Order partially filled! Starting C&T')
                            check_and_take(taker_client, maker_client, sell_order, pair, 'buy')
                        sell_exists = False

                    except ccxt.BadRequest as error:
                        logger.info(error)
                        print(f'Order has been fully filled, taking {sell_order["amount"]}')

                        if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                            logger.info('Was filled in the mean time, C&T')
                            sell_exists = False

        # Buy side arbitrage

        if bid_a >= bid_b * spread:

            # Introducing parameter for speed control

            buy_arbitrage = True

            # If the flag for an existing buy doesn't exist yet, create a buy order at the top bid.

            if not buy_exists:
                print(f'Make on {maker_client.name}')
                print(f'Buy {bid_b}')
                logger.info(f'Buy on {maker_client.name}, buy {bid_b}')

                if check_if_solvent(maker_client, taker_client, bid_b, maker_size):
                    buy_order = maker_client.create_limit_buy_order(symbol=pair, amount=maker_size, price=bid_b)
                    buy_exists = True
                    logger.info(f'Solvent, buy order created')

                else:
                    print('Insufficient funds!')
                    logger.info(f'Insufficient funds!')

            # If the flag for an existing buy order exists, check if it has been filled.

            else:
                print(f"Buy order already present at {buy_order['price']}")
                logger.info(f"Buy order already present at {buy_order['price']}")

                # Check if some of the order has been filled. If yes, the order is cancelled and the flag removed.

                if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                    buy_exists = False
                    logger.info(f"C&T, buy doesn't exist anymore")

                # If the existing order is no longer at the top of the bids, try to cancel it and place a new one.

                elif buy_order['price'] != bid_b:
                    print('Order no longer at top of bids, cancelling.')
                    logger.info('Order no longer at top of bids, cancelling.')

                    try:

                        # If the order is partially filled, start the take process.

                        if maker_client.cancel_order(id=buy_order['id'], symbol=pair)['filled'] != 0:
                            print('Order partially filled! Starting C&T')
                            check_and_take(taker_client, maker_client, buy_order, pair, 'sell')

                        else:
                            check_and_take(taker_client, maker_client, buy_order, pair, 'sell')

                        buy_exists = False

                    except ccxt.BadRequest as error:
                        logger.info(error)
                        print(f'Order has been fully filled, taking {buy_order["amount"]}')

                        if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                            buy_exists = False
                            logger.info('Was filled in the mean time, C&T')

                    if check_if_solvent(taker_client, maker_client, bid_b, maker_size):
                        buy_order = maker_client.create_limit_buy_order(symbol=pair, amount=maker_size, price=bid_b)
                        print(f'Buy {bid_b}')
                        buy_exists = True
                        logger.info('Order no longer at bottom of asks, cancelling.')
        else:

            # The arbitrage condition is no longer there, cancel open orders after checking them.

            buy_arbitrage = False
            logger.info('Arb now false')

            if buy_exists:

                if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                    buy_exists = False
                    logger.info('C&T success, buy no longer exists')

                else:
                    print('No more arb, cancelling buys.')
                    logger.info('No more arb, cancelling buys.')

                    try:

                        # If the order is partially filled, start the take process.

                        if maker_client.cancel_order(id=buy_order['id'], symbol=pair)['filled'] != 0:
                            print('Order partially filled! Starting C&T')
                            check_and_take(taker_client, maker_client, buy_order, pair, 'sell')
                        buy_exists = False

                    except ccxt.BadRequest as error:
                        logger.info(error)
                        print(f'Order has been fully filled, taking {buy_order["amount"]}')

                        if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                            logger.info('Was filled in the mean time, C&T')
                            buy_exists = False


making = True

if __name__ == '__main__':

    while making is True:
        try:
            make_and_take(gate_take, mexc_maker, 'ALPH/USDT', 1.002)

        except ccxt.NetworkError as e:
            print('Network error')
            logger.info('Network error')
            logger.info(e)

        except ccxt.ExchangeError as e:

            # Has happened because of too many requests.
            time.sleep(5)
            print('Exchange error')
            logger.info(e)


# # In case of emergencies, kill all open orders on maker client.
# mexc_maker.cancel_all_orders('ALPH/USDT')

