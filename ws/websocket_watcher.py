import ccxt.pro as ccxt
import asyncio

from config.env_var import mexc_secret_test, mexc_key_test, gateio_secret_test, gateio_key_test


# from config import gateio_key_test, gateio_secret_test, mexc_key_test, mexc_secret_test
# from make1 import make_and_take

class Watcher:
    def __init__(self, taker_client, maker_client):

        self.taker_client = taker_client
        self.maker_client = maker_client
        self.taker_ob = None
        self.maker_ob = None

    async def watch_ob(self, client, update_func):
        while True:
            try:
                order_book = await client.watch_order_book('ALPH/USDT')
                print(f'Bid {order_book["bids"][0][0]} and ask {order_book["asks"][0][0]} on {client.name}')
                update_func(order_book)
                await self.compare_order_books()
            except Exception as e:
                print(str(e))
                break
        await client.close()

    async def compare_order_books(self):
        if self.taker_ob is not None and self.maker_ob is not None:
            if self.taker_ob['bids'][0] > self.maker_ob['bids'][0]:
                print('ARBARBARB')
                await asyncio.sleep(1)
                # self.maker_client.create_order_ws()

    def update_taker_ob(self, ob):
        self.taker_ob = ob

    def update_maker_ob(self, ob):
        self.maker_ob = ob

    async def run(self):
        await asyncio.gather(
            self.watch_ob(self.taker_client, self.update_taker_ob),
            self.watch_ob(self.maker_client, self.update_maker_ob),
        )

    async def run2(self):
        pass


gate = ccxt.gateio({'apiKey': gateio_key_test, 'secret': gateio_secret_test})
mexc = ccxt.mexc({'apiKey': mexc_key_test, 'secret': mexc_secret_test})

comparator = Watcher(gate, mexc)

asyncio.run(comparator.run())
