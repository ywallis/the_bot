import asyncio
import logging

logger = logging.getLogger(__name__)


gate_fee = 0.001


async def match_sell(client, order):
    """This function takes in an incoming sell order change, and matches it in an arb setup."""


    quantity = order['filled']
    price = order['price']
    identifier = order['clientOrderId']
    pair = order['symbol']

    if float(price) * quantity <= 3:
        quantity = 3.1 / float(order['price'])

    if client.name == 'Gate.io':

        # Apply current fee level to keep stable inventory

        fee_ratio = 1 / (1 - gate_fee)

        quantity_with_fee = round(quantity * fee_ratio, 2)

        print(f'Placing a {quantity_with_fee} {pair} buy order on {client.name} for {price}.')
        logger.info(f'Placing a {quantity_with_fee} {pair} buy order on {client.name} for {price}.')

        asyncio.create_task(client.create_market_order(symbol=pair, side='buy', amount=quantity_with_fee, price=price,
                                                       params={'clientOrderId': identifier}))

        # Testing with appending to a file
        # if order['filled'] != 0:
        #     with open('matched.txt', 'a') as file:
        #         # file.write(f'\n{datetime.now()}\n{trade}\nPlacing a {quantity} {pair} buy order on {client.name} for {price}.')
        #         file.write(f'\n{str(order)}')


        # client.create_limit_order(symbol=pair, side='buy', amount=quantity_with_fee, price=price,
        #                                  params={'clientOrderId': identifier})


async def match_buy(client, order):
    """This function takes in an incoming buy order change, and matches it in an arb setup."""


    quantity = order['filled']
    price = order['price']
    identifier = order['clientOrderId']
    pair = order['symbol']

    if float(price) * quantity <= 3:
        quantity = 3.1 / float(order['price'])

    print(f'Placing a {quantity} {pair} sell order on {client.name} for {price}.')
    logger.info(f'Placing a {quantity} {pair} sell order on {client.name} for {price}.')

    asyncio.create_task(client.create_market_order(symbol=pair, side='sell', amount=quantity, price=price,
                                               params={'clientOrderId': identifier}))

    # Testing with appending to a file
    # if order['filled'] != 0:
    #     with open('matched.txt', 'a') as file:
    #         # file.write(f'\n{datetime.now()}\n{trade}\nPlacing a {quantity} {pair} sell order on {client.name} for {price}.')
    #         file.write(f'\n{str(order)}')

    # return client.create_limit_order(symbol=pair, side='sell', amount=quantity, price=price,
    #                                  params={'clientOrderId': identifier})


async def process_order_update(taker_client, order):
    """This function processes an order update, and prepares it for matching.
    To do so, it will check that the order has been closed, that part of it, or it's entirety has been filled.
    Finally, it checks the side of relevant orders and calls the appropriate matcher."""

    print('Processing!')


    # Remove file writes after testing
    client_order_id = order['clientOrderId']

    if order['status'] != 'open':
        if order['filled'] != 0:
            if client_order_id.endswith('_eb'):
                asyncio.create_task(match_buy(taker_client, order))


            elif client_order_id.endswith('_es'):
                asyncio.create_task(match_sell(taker_client, order))


    print(f'Processed trade {order['id']}')
    logger.info(f'Processed trade {order['id']}')