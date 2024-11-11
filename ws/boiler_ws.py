import asyncio

async def match_sell(client, order):
    """This function places a buy limit order using a CCXT client.
    It then outputs a confirmation of that order to the console and logs.
    It includes a modification for exchanges using the base asset for fees,
    to keep stable inventory in arbitrage setups."""


    quantity = order['amount']
    price = order['price']
    identifier = order['clientOrderId']
    pair = order['symbol']

    if client.name == 'Gate.io':

        # Apply current fee level to keep stable inventory

        fee_ratio = 1 / (1 - gate_fee)

        quantity_with_fee = round(quantity * fee_ratio, 2)

        await asyncio.sleep(1)

        print(f'Placed a {quantity_with_fee} {pair} buy order on {client.name} for {price}.')

        # Testing with appending to a file
        if order['filled'] != 0:
            with open('matched.txt', 'a') as file:
                # file.write(f'\n{datetime.now()}\n{trade}\nPlacing a {quantity} {pair} buy order on {client.name} for {price}.')
                file.write(f'\n{str(order)}')


        #logger.info(f'Placing a {quantity_with_fee} {pair} buy order on {client.name} for {trade['price']}.')

        # client.create_limit_order(symbol=pair, side='buy', amount=quantity_with_fee, price=price,
        #                                  params={'clientOrderId': identifier})


async def match_buy(client, order):

    quantity = order['amount']
    price = order['price']
    identifier = order['clientOrderId']
    pair = order['symbol']

    """This function places a sell limit order using a CCXT client.
    It then outputs a confirmation of that order to the console and logs."""

    print(f'Placing a {quantity} {pair} sell order on {client.name} for {price}.')
    # logger.info(f'Placing a {quantity} {pair} sell order on {client.name} for {price}.')

    # Testing with appending to a file
    if order['filled'] != 0:
        with open('matched.txt', 'a') as file:
            # file.write(f'\n{datetime.now()}\n{trade}\nPlacing a {quantity} {pair} sell order on {client.name} for {price}.')
            file.write(f'\n{str(order)}')

    # return client.create_limit_order(symbol=pair, side='sell', amount=quantity, price=price,
    #                                  params={'clientOrderId': identifier})


async def process_order_update(taker_client, order):
    """This function processes an order update, and prepares it for matching.
    To do so, it will check that the order has been closed, that part of it, or it's entirety has been filled.
    Finally, it checks the side of relevant orders and calls the appropriate matcher."""

    print('Processing!')


    # Remove file writes after testing

    if order['status'] != 'open':
        if order['filled'] != 0:
            if order['side'] == 'buy':
                with open('buys.txt', 'a') as file:
                    await asyncio.sleep(1)
                    file.write(f'\n{str(order)}')
                    asyncio.create_task(match_buy(taker_client, order))
            else:
                with open('sells.txt', 'a') as file:
                    await asyncio.sleep(1)
                    file.write(f'\n{str(order)}')
                    asyncio.create_task(match_sell(taker_client, order))

    print(f'Processed trade {order['id']}')