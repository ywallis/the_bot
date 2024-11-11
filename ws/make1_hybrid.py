import asyncio

from boiler_async import (check_and_take, check_if_solvent, maker_order_sizer, within_percentage_range,
                          create_and_return_order_abstraction,
                          cancel_order_abstraction, take_take)
from datetime import datetime
import time
import logging
from ccxt.base.types import Order
from ccxt.base.exchange import Exchange


logger = logging.getLogger(__name__)


async def make_and_take(taker_client: Exchange, maker_client: Exchange, config: dict, pair: str):
    """This function acts as a basic market making system, with the following two logics:
    1. A taker logic, acting immediately in two order books in case a profitable imbalance is spotted.
    2. A maker1 logic, offering liquidity on one side, if the position can be hedged profitably on the other."""

    taker_client = taker_client
    maker_client = maker_client
    maker_spread: float = config['maker_spread']
    maker_size: float = config['maker_size']
    maker_min_size: float = config['min_maker_size']
    taker_spread: float = config['taker_spread']
    taker_sizing: float = config['taker_sizing']
    taker_max_order_size: float = config['taker_max_order_size']
    taker_only: bool = config['taker_only']
    spread_extension: int = config['spread_extension']

    buy_exists: bool = False
    sell_exists: bool = False
    returned_buy_order: Order = Order
    returned_sell_order: Order = Order
    watching: bool = True

    # Arbitrage set to true for taker_only loops

    buy_arbitrage: bool = True
    sell_arbitrage: bool = True
    open_orders: list[Order] = await maker_client.fetch_open_orders(pair)

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

        taker_client_bids: list[list] = taker_order_book['bids']
        taker_client_asks: list[list] = taker_order_book['asks']
        maker_client_bids: list[list] = maker_order_book['bids']
        maker_client_asks: list[list] = maker_order_book['asks']

        best_bid_taker: float = taker_client_bids[0][0]
        best_ask_taker: float = taker_client_asks[0][0]
        best_bid_maker: float = maker_client_bids[0][0]
        best_ask_maker: float = maker_client_asks[0][0]

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

        if best_bid_maker >= best_ask_taker * taker_spread:

            # Using take_take as an if condition allows us to prioritize execution over the rest of the code.

            if await take_take(taker_client, maker_client, pair, maker_client_bids, taker_client_asks, taker_spread,
                               taker_sizing, taker_max_order_size, spread_extension):
                continue

        if best_bid_taker >= best_ask_maker * taker_spread:

            if await take_take(maker_client, taker_client, pair, taker_client_bids, maker_client_asks, taker_spread,
                               taker_sizing,
                               taker_max_order_size, spread_extension):
                continue

        # Function loops here for taker_only

        if taker_only:
            continue

        # Sell side arbitrage

        if best_ask_maker >= best_ask_taker * maker_spread:

            # Introducing parameter for speed control

            sell_arbitrage = True

            # Calculate the current optimal order size

            optimal_sell_size = maker_order_sizer(best_ask_maker, taker_client_asks, "sell", maker_spread,
                                                  maker_min_size,
                                                  maker_size)

            print(f'Optimal order size currently {optimal_sell_size}')
            logger.info(f'Optimal order size currently {optimal_sell_size}')

            # If the flag for an existing sell doesn't exist yet, create a sell order at the bottom ask.
            # Includes a custom clientOrderId to differentiate these orders from hanging taker order.

            if not sell_exists:

                print(f'Sell does not exist')
                logger.info(f'Sell does not exist')

                if await check_if_solvent(taker_client, maker_client, best_ask_maker, optimal_sell_size, pair=pair):
                    returned_sell_order = await create_and_return_order_abstraction(maker_client, best_ask_maker,
                                                                                    optimal_sell_size, pair, 'sell')
                    sell_exists = True

            # If the existing order is no longer at the bottom of the asks, try to cancel it and place a new one.

            elif returned_sell_order['price'] != best_ask_maker:

                print('Order no longer at bottom of asks, cancelling.')
                logger.info('Order no longer at bottom of asks, cancelling.')

                await cancel_order_abstraction(maker_client, returned_sell_order, pair)
                await check_and_take(taker_client, maker_client, returned_sell_order, pair)
                sell_exists = False

                if await check_if_solvent(taker_client, maker_client, best_ask_maker, optimal_sell_size, pair=pair):
                    returned_sell_order = await create_and_return_order_abstraction(maker_client, best_ask_maker,
                                                                                    optimal_sell_size, pair, 'sell')
                    sell_exists = True

            # Checks if the current order's amount is within a set range from the optimal size

            elif not within_percentage_range(returned_sell_order['amount'], optimal_sell_size, 20):

                print('Order no longer within acceptable size range, cancelling.')
                logger.info('Order no longer within acceptable size range, cancelling.')

                await cancel_order_abstraction(maker_client, returned_sell_order, pair)
                await check_and_take(taker_client, maker_client, returned_sell_order, pair)
                sell_exists = False

                if await check_if_solvent(taker_client, maker_client, best_ask_maker, optimal_sell_size, pair=pair):
                    returned_sell_order = await create_and_return_order_abstraction(maker_client, best_ask_maker,
                                                                                    optimal_sell_size, pair, 'sell')
                    sell_exists = True

            # If the flag for an existing sell order exists, check if it has been filled.

            else:
                print(f"Sell order already present at {returned_sell_order['price']}")
                logger.info(f"Sell order already present at {returned_sell_order['price']}")

                # Check if some of the order has been filled. If yes, the order is cancelled and the flag removed.

                if await check_and_take(taker_client, maker_client, returned_sell_order, pair):
                    sell_exists = False
                    logger.info(f"C&T, sell doesn't exist anymore")

        else:

            # The arbitrage condition is no longer there, cancel open orders after checking them.

            sell_arbitrage = False
            logger.info('Sell arb now false')

            if sell_exists:
                print('No more arb, cancelling sells.')
                logger.info('No more arb, cancelling sells.')

                await cancel_order_abstraction(maker_client, returned_sell_order, pair)
                await check_and_take(taker_client, maker_client, returned_sell_order, pair)
                sell_exists = False

        # Buy side arbitrage

        if best_bid_taker >= best_bid_maker * maker_spread:

            # Introducing parameter for speed control

            buy_arbitrage = True

            # Calculate the current optimal order size

            optimal_buy_size = maker_order_sizer(best_bid_maker, taker_client_bids, "buy", maker_spread, maker_min_size,
                                                 maker_size)

            print(f'Optimal order size currently {optimal_buy_size}')

            # If the flag for an existing buy doesn't exist yet, create a buy order at the top bid.
            # Includes a custom clientOrderId to differentiate these orders from hanging taker order.

            if not buy_exists:

                print(f'Buy does not exist')
                logger.info(f'Buy does not exist')

                if await check_if_solvent(maker_client, taker_client, best_bid_maker, optimal_buy_size, pair=pair):
                    returned_buy_order = await create_and_return_order_abstraction(maker_client, best_bid_maker,
                                                                                   optimal_buy_size, pair, 'buy')
                    buy_exists = True

            # If the existing order is no longer at the top of the bids, try to cancel it and place a new one.

            elif returned_buy_order['price'] != best_bid_maker:

                print('Order no longer at top of bids, cancelling.')
                logger.info('Order no longer at top of bids, cancelling.')

                await cancel_order_abstraction(maker_client, returned_buy_order, pair)
                await check_and_take(taker_client, maker_client, returned_buy_order, pair)
                buy_exists = False

                if await check_if_solvent(maker_client, taker_client, best_bid_maker, optimal_buy_size, pair=pair):
                    returned_buy_order = await create_and_return_order_abstraction(maker_client, best_bid_maker,
                                                                                   optimal_buy_size, pair, 'buy')
                    buy_exists = True

            # Checks if the current order's amount is within a set range from the optimal size

            elif not within_percentage_range(returned_buy_order['amount'], optimal_buy_size, 20):
                print('Order no longer within acceptable size range, cancelling.')
                logger.info('Order no longer within acceptable size range, cancelling.')

                await cancel_order_abstraction(maker_client, returned_buy_order, pair)
                await check_and_take(taker_client, maker_client, returned_buy_order, pair)
                buy_exists = False

                if await check_if_solvent(maker_client, taker_client, best_bid_maker, optimal_buy_size, pair=pair):
                    returned_buy_order = await create_and_return_order_abstraction(maker_client, best_bid_maker,
                                                                                   optimal_buy_size, pair, 'buy')
                    buy_exists = True

            # If the flag for an existing buy order exists, check if it has been filled.

            else:
                print(f"Buy order already present at {returned_buy_order['price']}")
                logger.info(f"Buy order already present at {returned_buy_order['price']}")

                # Check if some of the order has been filled. If yes, the order is cancelled and the flag removed.

                if await check_and_take(taker_client, maker_client, returned_buy_order, pair):
                    buy_exists = False
                    logger.info(f"C&T, buy doesn't exist anymore")

        else:

            # The arbitrage condition is no longer there, cancel open orders after checking them.

            buy_arbitrage = False
            logger.info('Buy arb now false')

            if buy_exists:
                print('No more arb, cancelling buys.')
                logger.info('No more arb, cancelling buys.')

                await cancel_order_abstraction(maker_client, returned_buy_order, pair)
                await check_and_take(taker_client, maker_client, returned_buy_order, pair)
                buy_exists = False
