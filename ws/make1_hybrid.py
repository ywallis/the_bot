import asyncio

import ccxt.async_support as ccxt
from async_support.boiler_async import (check_if_solvent, place_buy_order, place_sell_order, order_book_matcher,
                    order_time, maker_order_sizer, within_percentage_range)
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)


async def make_and_take(taker_client, maker_client, config, pair):

    """This function acts as a basic market making system, with the following two logics:
    1. A taker logic, acting immediately in two order books in case a profitable imbalance is spotted.
    2. A maker1 logic, offering liquidity on one side, if the position can be hedged profitably on the other."""

    taker_client = taker_client
    maker_client = maker_client
    maker_spread = config['maker_spread']
    maker_size = config['maker_size']
    maker_min_size = config['min_maker_size']
    taker_spread = config['taker_spread']
    taker_sizing = config['taker_sizing']
    taker_max_order_size = config['taker_max_order_size']
    taker_only = config['taker_only']
    spread_extension = config['spread_extension']

    buy_exists = False
    sell_exists = False
    returned_buy_order = None
    returned_sell_order = None
    watching = True

    # Arbitrage set to true for taker_only loops

    buy_arbitrage = True
    sell_arbitrage = True
    open_orders = await maker_client.fetch_open_orders(pair)

    # Check for hanging orders in case of errors

    for order in open_orders:

        client_order_id = order['clientOrderId']

        # Skipping hanging takers, addressed by cleaner function

        if client_order_id is None:
            continue

        # Looking for edge buys

        if client_order_id.endswith('_eb'):
            returned_buy_order = order
            buy_exists = True

        # Looking for edge sells

        if client_order_id.endswith('_es'):
            returned_sell_order = order
            sell_exists = True

    ## Kill all open orders if more than 1 per side.

    # if open_buy_count > 1 or open_sell_count > 1:
    #     print('Too many orders for current logic, cancelling all!')
    #     maker_client.cancel_all_orders(pair)
    #     buy_exists = False
    #     sell_exists = False

    while watching:

        # Slow watching if no open order

        if not buy_arbitrage and not sell_arbitrage:
            time.sleep(1)


        batch = asyncio.gather(taker_client.fetch_order_book(pair), maker_client.fetch_order_book(pair))
        taker_order_book, maker_order_book = await batch

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
              f'\nLowest on {lowest_bid} for {all_bids[lowest_bid]}, '
              f'highest on {highest_ask} for {all_asks[highest_ask]} ({watch_spread}%).')

        # Start of taker logic

        # Start take_take with client_a as buyer, client_b as seller.
        # continue


        if best_bid_maker >= best_ask_taker * taker_spread:

            try:

                taker_target_ask, taker_target_bid, taker_order_size = order_book_matcher(maker_bids, taker_asks,
                                                                                          taker_spread, taker_sizing,
                                                                                          taker_max_order_size,
                                                                                          extend_spread=spread_extension)

                if await check_if_solvent(taker_client, maker_client,
                                    quantity=taker_order_size, price=taker_target_ask, pair=pair):
                    try:
                        take_take_order_id = f't-{order_time()}_tt'

                        order_batch = asyncio.gather(place_buy_order(pair, taker_client, taker_target_ask, taker_order_size, take_take_order_id),
                                                     place_sell_order(pair, maker_client, taker_target_bid, taker_order_size, take_take_order_id))
                        await order_batch

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
                                                                                          taker_max_order_size,
                                                                                          extend_spread=spread_extension)

                if await check_if_solvent(maker_client, taker_client,
                                    quantity=taker_order_size, price=taker_target_ask, pair=pair):
                    try:

                        take_take_order_id = f't-{order_time()}_tt'

                        order_batch = asyncio.gather(place_buy_order(pair, maker_client, taker_target_ask, taker_order_size, take_take_order_id),
                                                     place_sell_order(pair, taker_client, taker_target_bid, taker_order_size, take_take_order_id))
                        await order_batch


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

            # Calculate the current optimal order size

            optimal_sell_size = maker_order_sizer(best_ask_maker, taker_asks, "sell", maker_spread, maker_min_size, maker_size)

            print(f'Optimal sell order size currently {optimal_sell_size}')
            logger.info(f'Optimal sell order size currently {optimal_sell_size}')

            # If the flag for an existing sell doesn't exist yet, create a sell order at the bottom ask.
            # Includes a custom clientOrderId to differentiate these orders from hanging taker order.

            if not sell_exists:
                print(f'Make on {maker_client.name}')
                print(f'Sell {best_ask_maker}')
                logger.info(f'Sell on {maker_client.name}, sell {best_ask_maker}')

                if await check_if_solvent(taker_client, maker_client, best_ask_maker, optimal_sell_size, pair=pair):

                    # Some exchanges only respond with an order id, hence the code complication with the ['id']
                    # to retrieve a full object

                    sell_order = await maker_client.create_limit_sell_order(symbol=pair,
                                                                      amount=optimal_sell_size,
                                                                      price=best_ask_maker,
                                                                      params={'clientOrderId': f't-{order_time()}_es'})
                    sell_exists = True

                    # Retrying in case of error

                    try:
                        returned_sell_order = await maker_client.fetch_order(id=sell_order['id'], symbol=pair)
                        logger.info(f'Solvent, sell order created')

                    except ccxt.ExchangeError as error:
                        print('Order fetch failed, trying again.')
                        time.sleep(0.2)
                        logger.info('Order fetch failed, trying again.')
                        logger.info(error)

                        try:
                            returned_sell_order = await maker_client.fetch_order(id=sell_order['id'], symbol=pair)
                            logger.info(f'Solvent, sell order created')
                        except ccxt.ExchangeError as error:
                            logger.info('Order fetch failed again.')
                            logger.info(error)

                else:
                    print('Insufficient funds!')
                    logger.info(f'Insufficient funds!')

            # If the existing order is no longer at the bottom of the asks, try to cancel it and place a new one.

            elif returned_sell_order['price'] != best_ask_maker:

                print('Order no longer at bottom of asks, cancelling.')
                logger.info('Order no longer at bottom of asks, cancelling.')

                try:

                    await maker_client.cancel_order(id=returned_sell_order['id'], symbol=pair)

                    sell_exists = False

                # TO-DO: This exception is too broad, would like to analyse in detail

                except (ccxt.BadRequest, ccxt.ExchangeError) as error:
                    logger.info(error)
                    print(f'Order has been fully filled, taking {returned_sell_order["amount"]}')

                    time.sleep(0.2)

                    await maker_client.cancel_order(id=returned_sell_order['id'], symbol=pair)
                    logger.info('Double tap cancel')

                if await check_if_solvent(taker_client, maker_client, best_ask_maker, optimal_sell_size, pair=pair):
                    sell_order = await maker_client.create_limit_sell_order(symbol=pair, amount=optimal_sell_size,
                                                                      price=best_ask_maker,
                                                                      params={'clientOrderId': f't-{order_time()}_es'})

                    sell_exists = True

                    # Retrying in case of error

                    try:
                        returned_sell_order = await maker_client.fetch_order(id=sell_order['id'], symbol=pair)
                        logger.info(f'Solvent, sell order created')
                    except ccxt.ExchangeError as error:
                        print('Order fetch failed, trying again.')
                        time.sleep(0.2)
                        logger.info('Order fetch failed, trying again.')
                        logger.info(error)
                        try:
                            returned_sell_order = await maker_client.fetch_order(id=sell_order['id'], symbol=pair)
                            logger.info(f'Solvent, sell order created')
                        except ccxt.ExchangeError as error:
                            logger.info('Order fetch failed again.')
                            logger.info(error)

            # Checks if the current order's amount is within a set range from the optimal size

            elif not within_percentage_range(returned_sell_order['amount'], optimal_sell_size, 20):
                print('Order no longer within acceptable size range, cancelling.')
                logger.info('Order no longer within acceptable size range, cancelling.')

                sell_exists = False

                try:
                    await maker_client.cancel_order(id=returned_sell_order['id'], symbol=pair)


                except (ccxt.BadRequest, ccxt.ExchangeError) as error:
                    logger.info(error)
                    print(f'Order has been fully filled, taking {returned_sell_order["amount"]}')

                    time.sleep(0.2)
                    await maker_client.cancel_order(id=returned_sell_order['id'], symbol=pair)
                    logger.info('Double tap cancel')


                if await check_if_solvent(taker_client, maker_client, best_ask_maker, optimal_sell_size, pair=pair):
                    sell_order = await maker_client.create_limit_sell_order(symbol=pair, amount=optimal_sell_size,
                                                                      price=best_ask_maker,
                                                                      params={'clientOrderId': f't-{order_time()}_es'})
                    sell_exists = True
                    # Retrying in case of error

                    try:
                        returned_sell_order = await maker_client.fetch_order(id=sell_order['id'], symbol=pair)
                        logger.info(f'Solvent, sell order created')
                    except ccxt.ExchangeError as error:
                        print('Order fetch failed, trying again.')
                        time.sleep(0.2)
                        logger.info('Order fetch failed, trying again.')
                        logger.info(error)
                        try:
                            returned_sell_order = await maker_client.fetch_order(id=sell_order['id'], symbol=pair)
                            logger.info(f'Solvent, sell order created')
                        except ccxt.ExchangeError as error:
                            logger.info('Order fetch failed again.')
                            logger.info(error)

            # If the flag for an existing sell order exists, check if it has been filled.

            else:
                print(f"Sell order already present at {returned_sell_order['price']}")
                logger.info(f"Sell order already present at {returned_sell_order['price']}")

                # Code for time-dependent refresh here!


        # The arbitrage condition is no longer there, cancel open orders after checking them.

        else:


            sell_arbitrage = False
            logger.info('Arb now false')

            if sell_exists:

                print('No more arb, cancelling sells.')
                logger.info('No more arb, cancelling sells.')

                try:

                    # If the order is partially filled, start the take process.

                    await maker_client.cancel_order(id=returned_sell_order['id'], symbol=pair)
                    sell_exists = False

                except (ccxt.BadRequest, ccxt.ExchangeError) as error:
                    logger.info(error)
                    print(f'Order has been fully filled, taking {returned_sell_order["amount"]}')
                    sell_exists = False

        # Buy side arbitrage

        if best_bid_taker >= best_bid_maker * maker_spread:

            # Introducing parameter for speed control

            buy_arbitrage = True

            # Calculate the current optimal order size

            optimal_buy_size = maker_order_sizer(best_bid_maker, taker_bids, "buy", maker_spread, maker_min_size, maker_size)

            print(f'Optimal buy order size currently {optimal_buy_size}')
            logger.info(f'Optimal buy order size currently {optimal_buy_size}')


            # If the flag for an existing buy doesn't exist yet, create a buy order at the top bid.
            # Includes a custom clientOrderId to differentiate these orders from hanging taker order.

            if not buy_exists:
                print(f'Make on {maker_client.name}')
                print(f'Buy {best_bid_maker}')
                logger.info(f'Buy on {maker_client.name}, buy {best_bid_maker}')

                if await check_if_solvent(maker_client, taker_client, best_bid_maker, optimal_buy_size, pair=pair):
                    buy_order = await maker_client.create_limit_buy_order(symbol=pair,
                                                                    amount=optimal_buy_size, price=best_bid_maker,
                                                                    params={'clientOrderId': f't-{order_time()}_eb'})


                    buy_exists = True
                    # Retrying in case of error

                    try:
                        returned_buy_order = await maker_client.fetch_order(id=buy_order['id'], symbol=pair)
                        logger.info(f'Solvent, buy order created')
                    except ccxt.ExchangeError as error:
                        print('Order fetch failed, trying again.')
                        time.sleep(0.2)
                        logger.info('Order fetch failed, trying again.')
                        logger.info(error)
                        try:
                            returned_buy_order = await maker_client.fetch_order(id=buy_order['id'], symbol=pair)
                            logger.info(f'Solvent, buy order created')
                        except ccxt.ExchangeError as error:
                            logger.info('Order fetch failed again.')
                            logger.info(error)

                else:
                    print('Insufficient funds!')
                    logger.info(f'Insufficient funds!')

            # If the existing order is no longer at the top of the bids, try to cancel it and place a new one.

            elif returned_buy_order['price'] != best_bid_maker:

                print('Order no longer at top of bids, cancelling.')
                logger.info('Order no longer at top of bids, cancelling.')

                try:

                    # If the order is partially filled, start the take process.

                    await maker_client.cancel_order(id=returned_buy_order['id'], symbol=pair)
                    buy_exists = False

                except (ccxt.BadRequest, ccxt.ExchangeError) as error:
                    logger.info(error)
                    print(f'Order has been fully filled, taking {returned_buy_order["amount"]}')


                    time.sleep(0.2)
                    await maker_client.cancel_order(id=returned_buy_order['id'], symbol=pair)
                    logger.info('Double tap cancel')


                if await check_if_solvent(taker_client, maker_client, best_bid_maker, optimal_buy_size, pair=pair):
                    buy_order = await maker_client.create_limit_buy_order(symbol=pair,
                                                                    amount=optimal_buy_size, price=best_bid_maker,
                                                                    params={'clientOrderId': f't-{order_time()}_eb'})
                    buy_exists = True

                    # Retrying in case of error

                    try:
                        returned_buy_order = await maker_client.fetch_order(id=buy_order['id'], symbol=pair)
                        logger.info(f'Solvent, buy order created')
                    except ccxt.ExchangeError as error:
                        print('Order fetch failed, trying again.')
                        time.sleep(0.2)
                        logger.info('Order fetch failed, trying again.')
                        logger.info(error)
                        try:
                            returned_buy_order = await maker_client.fetch_order(id=buy_order['id'], symbol=pair)
                            logger.info(f'Solvent, buy order created')
                        except ccxt.ExchangeError as error:
                            logger.info('Order fetch failed again.')
                            logger.info(error)

            # Checks if the current order's amount is within a set range from the optimal size

            elif not within_percentage_range(returned_buy_order['amount'], optimal_buy_size, 20):
                print('Order no longer within acceptable size range, cancelling.')
                logger.info('Order no longer within acceptable size range, cancelling.')

                buy_exists = False

                try:

                    # If the order is partially filled, start the take process.

                    await maker_client.cancel_order(id=returned_buy_order['id'], symbol=pair)


                except (ccxt.BadRequest, ccxt.ExchangeError) as error:
                    logger.info(error)
                    print(f'Order has been fully filled, taking {returned_buy_order["amount"]}')

                    time.sleep(0.2)
                    await maker_client.cancel_order(id=returned_buy_order['id'], symbol=pair)
                    logger.info('Double tap cancel')

                if await check_if_solvent(taker_client, maker_client, best_bid_maker, optimal_buy_size, pair=pair):
                    buy_order = await maker_client.create_limit_buy_order(symbol=pair,
                                                                    amount=optimal_buy_size, price=best_bid_maker,
                                                                    params={'clientOrderId': f't-{order_time()}_eb'})
                    buy_exists = True
                    # Retrying in case of error

                    try:
                        returned_buy_order = await maker_client.fetch_order(id=buy_order['id'], symbol=pair)
                        logger.info(f'Solvent, buy order created')
                    except ccxt.ExchangeError as error:
                        print('Order fetch failed, trying again.')
                        time.sleep(0.2)
                        logger.info('Order fetch failed, trying again.')
                        logger.info(error)
                        try:
                            returned_buy_order = await maker_client.fetch_order(id=buy_order['id'], symbol=pair)
                            logger.info(f'Solvent, buy order created')
                        except ccxt.ExchangeError as error:
                            logger.info('Order fetch failed again.')
                            logger.info(error)

            # If the flag for an existing buy order exists, check if it has been filled.

            else:
                print(f"Buy order already present at {returned_buy_order['price']}")
                logger.info(f"Buy order already present at {returned_buy_order['price']}")

                # Check if some of the order has been filled. If yes, the order is cancelled and the flag removed.
                # Don't forget the time-dependent refresh


        else:

            # The arbitrage condition is no longer there, cancel open orders after checking them.

            buy_arbitrage = False
            logger.info('Arb now false')

            if buy_exists:

                print('No more arb, cancelling buys.')
                logger.info('No more arb, cancelling buys.')

                try:

                    # If the order is partially filled, start the take process.

                    await maker_client.cancel_order(id=returned_buy_order['id'], symbol=pair)

                    buy_exists = False

                except (ccxt.BadRequest, ccxt.ExchangeError) as error:
                    logger.info(error)
                    print(f'Order has been fully filled, taking {returned_buy_order["amount"]}')

                    buy_exists = False
