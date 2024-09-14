import ccxt.pro as ccxt
import asyncio
from datetime import datetime, timezone
from config import gateio_key_test, gateio_secret_test, mexc_key_test, mexc_secret_test

# TESTING - IMPLEMENTING USING ALL TRADES INSTEAD OF MY TRADES TO AVOID DEALING WITH KEYS

#Introduce logging
# Will move to boiler once tested


async def match_sell(client, trade):
    """This function places a buy limit order using a CCXT client.
    It then outputs a confirmation of that order to the console and logs.
    It includes a modification for exchanges using the base asset for fees,
    to keep stable inventory in arbitrage setups."""


    quantity = trade['amount']
    price = trade['price']
    # SET WHEN MOVING TO PROD
    # identifier = trade['clientOrderId']
    pair = trade['symbol']

    if client.name == 'Gate.io':

        # Apply current fee level to keep stable inventory
        fee_ratio = 1 / (1 - gate_fee)
        # Amount and quantity are different keys! relevant because testing in public data

        quantity_with_fee = round(quantity * fee_ratio, 2)

        await asyncio.sleep(1)

        print(f'Placed a {quantity_with_fee} {pair} buy order on {client.name} for {trade['price']}.')

        #logger.info(f'Placing a {quantity_with_fee} {pair} buy order on {client.name} for {trade['price']}.')

        # client.create_limit_order(symbol=pair, side='buy', amount=quantity_with_fee, price=price,
        #                                  params={'clientOrderId': identifier})


async def match_buy(client, trade):

    quantity = trade['amount']
    price = trade['price']
    # SET WHEN MOVING TO PROD
    # identifier = trade['clientOrderId']
    pair = trade['symbol']

    """This function places a sell limit order using a CCXT client.
    It then outputs a confirmation of that order to the console and logs."""

    print(f'Placing a {quantity} {pair} sell order on {client.name} for {price}.')
    # logger.info(f'Placing a {quantity} {pair} sell order on {client.name} for {price}.')

    # return client.create_limit_order(symbol=pair, side='sell', amount=quantity, price=price,
    #                                  params={'clientOrderId': identifier})

class Matcher:
    """This is a class designed to represent a websocket "watcher",
    which will monitor trades executed on the maker clients and mirror them on the taker client. """
    def __init__(self, taker_client, *maker_clients):

        self.taker_client = taker_client
        self.maker_clients = maker_clients

    async def watch_trades(self, client):
        since = datetime.now(timezone.utc)
        timestamp = int(since.timestamp() * 1000)

        while True:
            try:
                trades = await client.watch_trades('ALPH/USDT', since=timestamp)
                # Printing as a first placeholder for further logic
                print(len(trades))
                await self.process_trades(trades, client.name)
            except Exception as e:
                print(str(e))
                break

        await client.close()

    async def process_trades(self, trades, client_name):
        tasks = []

        for trade in trades:
            # Compare order IDs here to exclude tt's, and possibly combine any orders splitting into multiple trades
            print(trade)
            print(f"Received trade {trade['amount']} from {client_name}")
            tasks.append(self.match_trade(trade))

        await asyncio.gather(*tasks)

    async def match_trade(self, trade):
        print(f'Sending {trade["amount"]}')

        # CAREFUL HERE! PRODUCTION ONLY

        if trade['side'] == 'buy':
            print(f'This is a {trade['side']}')
            await match_buy(self.taker_client, trade)
            # await asyncio.sleep(2)
            print(f'Executed {trade["amount"]}')

        if trade['side'] == 'sell':
            print(f'This is a {trade['side']}')
            await match_sell(self.taker_client, trade)
            # await asyncio.sleep(2)
            print(f'Executed {trade["amount"]}')



    async def run(self):
        await asyncio.gather(*[self.watch_trades(client) for client in self.maker_clients])


# Can't import clients from config since they don't use ccxt.pro
gate_fee = 0
gate = ccxt.gateio({'apiKey': gateio_key_test, 'secret': gateio_secret_test})
mexc = ccxt.mexc({'apiKey': mexc_key_test, 'secret': mexc_secret_test})

watch_me = Matcher(gate,mexc)

print(watch_me.maker_clients)
asyncio.run(watch_me.run())

# since = datetime.now()
# timestamp = int(since.timestamp() * 1000)
# print(since)
# print(timestamp)
#
# print(bitget_client.iso8601(timestamp))
# print(gate_client.iso8601(timestamp))
# print(mexc_client.iso8601(timestamp))