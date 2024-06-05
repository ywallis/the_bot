import ccxt
from boiler import check_and_take, check_if_solvent, place_buy_order, place_sell_order, order_book_matcher
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)


def make_and_take(taker_client, maker_client, pair, maker_spread, maker_size,
                  taker_spread, taker_sizing, taker_max_order_size, taker_only=False):

    """This function acts as a basic market making system, with the following two logics:
    1. A taker logic, acting immediately in two order books in case a profitable imbalance is spotted.
    2. A maker1 logic, offering liquidity on one side, if the position can be hedged profitably on the other."""

    buy_exists = False
    sell_exists = False
    buy_order = None
    sell_order = None
    watching = True

    # Arbitrage set to true for taker_only loops

    buy_arbitrage = True
    sell_arbitrage = True
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

        taker_order_book = taker_client.fetch_order_book(pair)
        maker_order_book = maker_client.fetch_order_book(pair)
        taker_bids = taker_order_book['bids']
        taker_asks = taker_order_book['asks']
        maker_bids = maker_order_book['bids']
        maker_asks = maker_order_book['asks']

        best_bid_taker = taker_bids[0][0]
        best_ask_taker = taker_asks[0][0]
        best_bid_maker = maker_bids[0][0]
        best_ask_maker = maker_asks[0][0]

        all_bids = {taker_client.name: best_bid_taker, maker_client.name: best_bid_maker}
        all_asks = {taker_client.name: best_ask_taker, maker_client.name: best_ask_maker}

        lowest_bid = min(all_bids, key=all_bids.get)
        highest_ask = max(all_asks, key=all_asks.get)

        watch_spread = round((all_asks[highest_ask] / all_bids[lowest_bid] - 1) * 100, 2)

        print(f'Watching at {datetime.now()}.'
              f'\nLowest bid on {lowest_bid} for {all_bids[lowest_bid]}, '
              f'highest ask on {highest_ask} for {all_asks[highest_ask]} ({watch_spread}%).')

        # Include taker logic

        # Start take_take with client_a as buyer, client_b as seller.

        if best_bid_maker >= best_ask_taker * taker_spread:

            try:

                taker_target_ask, taker_target_bid, taker_order_size = order_book_matcher(maker_bids, taker_asks,
                                                                                          taker_spread, taker_sizing,
                                                                                          taker_max_order_size)

                if check_if_solvent(buy_client=taker_client, sell_client=maker_client,
                                    quantity=taker_order_size, price=taker_target_ask, pair=pair):
                    try:
                        place_sell_order(pair, maker_client, taker_target_bid, taker_order_size)
                        place_buy_order(pair, taker_client, taker_target_ask, taker_order_size)

                        # The continue statement puts the priority on taking whenever possible,
                        # since it is most efficient. Downside is that some orders may remain stuck
                        # if the account is no longer solvent.

                        continue

                    except RuntimeError:
                        print("Going too fast.")
                        time.sleep(10)

                else:
                    print('Insufficient funds!')

            except TypeError:
                print("Could not match order books.")

            except AttributeError:
                print('Attribute error')

            except IndexError:
                (print('End of orderbook'))

        if best_bid_taker >= best_ask_maker * taker_spread:

            # Start take_take with client_b as buyer, client_a as seller.

            try:

                taker_target_ask, taker_target_bid, taker_order_size = order_book_matcher(taker_bids, maker_asks,
                                                                                          taker_spread, taker_sizing,
                                                                                          taker_max_order_size)

                if check_if_solvent(buy_client=maker_client, sell_client=taker_client,
                                    quantity=taker_order_size, price=taker_target_ask, pair=pair):
                    try:

                        place_sell_order(pair, taker_client, taker_target_bid, taker_order_size)
                        place_buy_order(pair, maker_client, taker_target_ask, taker_order_size)

                        # The continue statement puts the priority on taking whenever possible,
                        # since it is most efficient. Downside is that some orders may remain stuck
                        # if the account is no longer solvent.

                        continue

                    except RuntimeError:
                        print("Going too fast.")
                        time.sleep(10)

                else:
                    print('Insufficient funds!')

            except TypeError:
                print("Could not match order books.")

            except AttributeError:
                print('Attribute error')

            except IndexError:
                (print('End of orderbook'))

        # Function loops here for taker_only

        if taker_only:
            continue

        # Sell side arbitrage

        if best_ask_maker >= best_ask_taker * maker_spread:

            # Introducing parameter for speed control

            sell_arbitrage = True

            # If the flag for an existing sell doesn't exist yet, create a sell order at the bottom ask.

            if not sell_exists:
                print(f'Make on {maker_client.name}')
                print(f'Sell {best_ask_maker}')
                logger.info(f'Sell on {maker_client.name}, sell {best_ask_maker}')

                if check_if_solvent(taker_client, maker_client, best_ask_maker, maker_size, pair=pair):
                    sell_order = maker_client.create_limit_sell_order(symbol=pair,
                                                                      amount=maker_size, price=best_ask_maker)
                    sell_exists = True
                    logger.info(f'Solvent, sell order created')

                else:
                    print('Insufficient funds!')
                    logger.info(f'Insufficient funds!')

            # If the existing order is no longer at the bottom of the asks, try to cancel it and place a new one.

            elif sell_order['price'] != best_ask_maker:
                print('Order no longer at bottom of asks, cancelling.')
                logger.info('Order no longer at bottom of asks, cancelling.')

                try:
                    # If the order is partially filled, start the take process.
                    maker_client.cancel_order(id=sell_order['id'], symbol=pair)
                    check_and_take(taker_client, maker_client, sell_order, pair, 'buy')

                    sell_exists = False

                except ccxt.BadRequest as error:
                    logger.info(error)
                    print(f'Order has been fully filled, taking {sell_order["amount"]}')

                    if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                        sell_exists = False
                        logger.info('Was filled in the mean time, C&T')

                if check_if_solvent(taker_client, maker_client, best_ask_maker, maker_size, pair=pair):
                    sell_order = maker_client.create_limit_sell_order(symbol=pair, amount=maker_size,
                                                                      price=best_ask_maker)
                    print(f'Sell {best_ask_maker}')
                    sell_exists = True
                    logger.info('Order no longer at bottom of asks, cancelling.')

            # If the flag for an existing sell order exists, check if it has been filled.

            else:
                print(f"Sell order already present at {sell_order['price']}")
                logger.info(f"Sell order already present at {sell_order['price']}")

                # Check if some of the order has been filled. If yes, the order is cancelled and the flag removed.

                if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                    sell_exists = False
                    logger.info(f"C&T, sell doesn't exist anymore")

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

                        maker_client.cancel_order(id=sell_order['id'], symbol=pair)
                        check_and_take(taker_client, maker_client, sell_order, pair, 'buy')
                        sell_exists = False

                    except ccxt.BadRequest as error:
                        logger.info(error)
                        print(f'Order has been fully filled, taking {sell_order["amount"]}')

                        if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                            logger.info('Was filled in the mean time, C&T')
                            sell_exists = False

        # Buy side arbitrage

        if best_bid_taker >= best_bid_maker * maker_spread:

            # Introducing parameter for speed control

            buy_arbitrage = True

            # If the flag for an existing buy doesn't exist yet, create a buy order at the top bid.

            if not buy_exists:
                print(f'Make on {maker_client.name}')
                print(f'Buy {best_bid_maker}')
                logger.info(f'Buy on {maker_client.name}, buy {best_bid_maker}')

                if check_if_solvent(maker_client, taker_client, best_bid_maker, maker_size, pair=pair):
                    buy_order = maker_client.create_limit_buy_order(symbol=pair,
                                                                    amount=maker_size, price=best_bid_maker)
                    buy_exists = True
                    logger.info(f'Solvent, buy order created')

                else:
                    print('Insufficient funds!')
                    logger.info(f'Insufficient funds!')

            # If the existing order is no longer at the top of the bids, try to cancel it and place a new one.

            elif buy_order['price'] != best_bid_maker:
                print('Order no longer at top of bids, cancelling.')
                logger.info('Order no longer at top of bids, cancelling.')

                try:

                    # If the order is partially filled, start the take process.

                    maker_client.cancel_order(id=buy_order['id'], symbol=pair)
                    check_and_take(taker_client, maker_client, buy_order, pair, 'sell')

                    buy_exists = False

                except ccxt.BadRequest as error:
                    logger.info(error)
                    print(f'Order has been fully filled, taking {buy_order["amount"]}')

                    if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                        buy_exists = False
                        logger.info('Was filled in the mean time, C&T')

                if check_if_solvent(taker_client, maker_client, best_bid_maker, maker_size, pair=pair):
                    buy_order = maker_client.create_limit_buy_order(symbol=pair,
                                                                    amount=maker_size, price=best_bid_maker)
                    print(f'Buy {best_bid_maker}')
                    buy_exists = True
                    logger.info('Order no longer at bottom of asks, cancelling.')

            # If the flag for an existing buy order exists, check if it has been filled.

            else:
                print(f"Buy order already present at {buy_order['price']}")
                logger.info(f"Buy order already present at {buy_order['price']}")

                # Check if some of the order has been filled. If yes, the order is cancelled and the flag removed.

                if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                    buy_exists = False
                    logger.info(f"C&T, buy doesn't exist anymore")

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

                        maker_client.cancel_order(id=buy_order['id'], symbol=pair)
                        check_and_take(taker_client, maker_client, buy_order, pair, 'sell')

                        buy_exists = False

                    except ccxt.BadRequest as error:
                        logger.info(error)
                        print(f'Order has been fully filled, taking {buy_order["amount"]}')

                        if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                            logger.info('Was filled in the mean time, C&T')
                            buy_exists = False
