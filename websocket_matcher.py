import ccxt.pro
import asyncio
from datetime import datetime, timezone
from config import *

# TESTING - IMPLEMENTING USING ALL TRADES INSTEAD OF MY TRADES TO AVOID DEALING WITH KEYS


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
                trades = await client.watch_my_trades('ALPH/USDT', since=timestamp)
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
            # print(trade)
            print(f"Received trade {trade['amount']} from {client_name}")
            tasks.append(self.match_trade(trade))

        await asyncio.gather(*tasks)

    async def match_trade(self, trade):
        print(f'Sending {trade["amount"]}')
        await asyncio.sleep(2)
        print('Executed')

    async def run(self):
        await asyncio.gather(*[self.watch_trades(client) for client in self.maker_clients])


gate_client = ccxt.pro.gateio()
bitget_client = ccxt.pro.bitget({'apiKey': bitget_key, 'secret': bitget_secret, 'password': 'bgtest123456'})
mexc_client = ccxt.pro.mexc({'apiKey': mexc_key, 'secret': mexc_secret})
watch_me = Matcher(gate_client,mexc_client, bitget_client)


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