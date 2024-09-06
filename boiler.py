import asyncio
import logging
import ccxt.async_support as ccxt
from datetime import datetime

from config import gate_fee, bitget_fee

logger = logging.getLogger(__name__)


def order_book_matcher(bids, asks, spread, sizing, max_order_size, min_order_size=0.01, extend_spread=0):

    """This function takes in two order books sides represented by lists.
    It will then go both lists, and generate a target ask, target bid, and appropriate size to
    extract maximum value from both books. Other inputs are floats.
    extend_spread is used to target prices beyond the optimal spread.
    This can be used to help guarantee execution for limit orders, or increase skew for cost-based market buy orders."""

    # REMOVE MIN ORDER DEFAULT EVENTUALLY

    dynamic_arb = True
    bid_counter = 0
    ask_counter = 0
    cumulative_bid = 0
    cumulative_ask = 0

    while dynamic_arb:

        highest_bid = bids[bid_counter]

        highest_bid_price = float(highest_bid[0])
        highest_bid_quantity = float(highest_bid[1])

        cumulative_bid += highest_bid_quantity
        logger.info(f'Cumulative bid quantity is: {round(cumulative_bid, 2)}')

        lowest_ask = asks[ask_counter]

        lowest_ask_price = float(lowest_ask[0])
        lowest_ask_quantity = float(lowest_ask[1])

        cumulative_ask += lowest_ask_quantity
        logger.info(f'Cumulative ask quantity is: {round(cumulative_ask, 2)}')

        # Define arbitrage condition

        if highest_bid_price >= lowest_ask_price * spread:
            print("This should be arbed.")
            logger.info("This should be arbed.")

            # If the functions downstream use this with market orders, it may result in quantity deviations.

            target_ask = float(asks[(ask_counter + extend_spread)][0])
            target_bid = float(bids[(bid_counter + extend_spread)][0])
            # Implementing cumulative bid/ask
            # order_size = round(min(highest_bid_quantity, lowest_ask_quantity) * sizing, 2)
            order_size = round(min(cumulative_bid, cumulative_ask) * sizing, 2)

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

                # This here could be replaced by a call to the exchange defining a dynamic order minimum.

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


async def place_sell_order(pair, client, price, quantity, identifier):

    """This function places a sell limit order using a CCXT client.
    It then outputs a confirmation of that order to the console and logs."""

    print(f'Placing a {quantity} {pair} sell order on {client.name} for {price}.')
    logger.info(f'Placing a {quantity} {pair} sell order on {client.name} for {price}.')

    return client.create_limit_order(symbol=pair, side='sell', amount=quantity, price=price,
                                     params={'clientOrderId': identifier})


async def place_buy_order(pair, client, price, quantity, identifier):
    """This function places a buy limit order using a CCXT client.
    It then outputs a confirmation of that order to the console and logs.
    It includes a modification for exchanges using the base asset for fees,
    to keep stable inventory in arbitrage setups."""

    if client.name == 'Gate.io':

        # Apply current fee level to keep stable inventory
        fee_ratio = 1 / (1 - gate_fee)

        quantity_with_fee = round(quantity * fee_ratio, 2)

        print(f'Placing a {quantity_with_fee} {pair} buy order on {client.name} for {price}.')
        logger.info(f'Placing a {quantity_with_fee} {pair} buy order on {client.name} for {price}.')

        return client.create_limit_order(symbol=pair, side='buy', amount=quantity_with_fee, price=price,
                                         params={'clientOrderId': identifier})

    elif client.name == 'Bitget':

        # Apply current fee level to keep stable inventory
        fee_ratio = 1 / (1 - bitget_fee)

        quantity_with_fee = round(quantity * fee_ratio, 2)

        print(f'Placing a {quantity_with_fee} {pair} buy order on {client.name} for {price}.')
        logger.info(f'Placing a {quantity_with_fee} {pair} buy order on {client.name} for {price}.')

        return client.create_limit_order(symbol=pair, side='buy', amount=quantity_with_fee, price=price,
                                         params={'clientOrderId': identifier})

    else:

        print(f'Placing a {quantity} {pair} buy order on {client.name} for {price}.')
        logger.info(f'Placing a {quantity} {pair} buy order on {client.name} for {price}.')

        return client.create_limit_order(symbol=pair, side='buy', amount=quantity, price=price,
                                         params={'clientOrderId': identifier})


async def fetch_balances(buy_client, sell_client):

    batch = asyncio.gather(buy_client.fetch_balance(), sell_client.fetch_balance())
    buy_client_balance, sell_client_balance = await batch

    return buy_client_balance, sell_client_balance


async def check_if_solvent(buy_client, sell_client, price, quantity, pair):
    """This function checks if two exchanges have the necessary balances to place
    two arbitrage orders in their relevant assets."""

    batch = asyncio.gather(buy_client.fetch_balance(), sell_client.fetch_balance())
    buy_client_balance, sell_client_balance = await batch

    base_asset = pair.split('/')[0]
    quote_asset = pair.split('/')[1]

    try:

        if (quantity * price * 2 < buy_client_balance[quote_asset]['free']
                and quantity * 2 < sell_client_balance[base_asset]['free']):
            return True
        else:
            return False

    except KeyError:

        # Error can occur if the subaccount never had an asset balance.

        print('Insufficient funds! Are you sure the right pair is selected?')
        logger.info('Insufficient funds! Are you sure the right pair is selected?')
        return False


async def check_and_take(client_a, client_b, order, pair, market_side):

    """This function checks if a placed maker order has been filled or partially filled
    and generates an equivalent taker order on another exchange."""

    func_order = await client_b.fetch_order(id=order['id'], symbol=pair)
    filled = float(func_order['filled'])
    print(f'{filled} from order filled')
    # retrieve order
    if filled != 0.0:

        if func_order['status'] == 'open':

            # Add try, to prevent issues with orders filled in the meantime
            try:
                await client_b.cancel_order(id=order['id'], symbol=pair)

                # YOU MIGHT BE ABLE TO SPEED THIS UP BY ASSIGNING FILLED TO THE CANCEL ORDER

                filled = float(await client_b.fetch_order(id=order['id'], symbol=pair)['filled'])
                print(f'{filled} from order filled')

            except ccxt.BadRequest:
                filled = float(await client_b.fetch_order(id=order['id'], symbol=pair)['filled'])
                print(f'Order has been fully filled, taking {filled}')

        # market sell any filled on A

        # adding stable inventory condition for gateio

        if market_side == 'buy' and client_a.name == 'Gate.io':

            # Apply current fee level to keep stable inventory
            fee_ratio = 1 / (1 - gate_fee)

            filled = round(filled * fee_ratio, 2)

        # Adding minimum order size condition

        if float(order['price']) * filled <= 3:
            filled = 3.1 / float(order['price'])

        # Targeting best price on taker exchange here would help keep inventory stable.

        await client_a.create_market_order(symbol=pair, side=market_side, amount=filled, price=order['price'],
                                     params={'clientOrderId': func_order['clientOrderId']})
        print(f'Market {market_side} {filled} {pair} on {client_b.name}')

        return True


def order_time():

    """Creates a datetime-based stamp to make unique and custom order numbers."""

    return datetime.now().strftime('%y%m%d_%H%M%S_%f')


def maker_order_sizer(maker_level, taker_book, side, min_spread, min_maker_size, max_maker_size):

    """NEEDS FLESHING OUT!
    Goal of function is to watch how much liquidity is available on the taker client within the defined spread."""

    cumulative = 0
    if side == 'sell':
        for level in taker_book:
            if maker_level >= level[0] * min_spread:
                cumulative += level[1]
            else:
                break

    if side == 'buy':
        for level in taker_book:
            if level[0] >= maker_level * min_spread:
                cumulative += level[1]
            else:
                break

    if cumulative < min_maker_size:
        return min_maker_size
    elif cumulative > max_maker_size:
        return max_maker_size
    else:
        return cumulative


def within_percentage_range(x, y, percentage):

    """Function checks whether x is within a definable percentage range from y."""

    lower_bound = y * (1 - percentage / 100)
    upper_bound = y * (1 + percentage / 100)

    return lower_bound <= x <= upper_bound
