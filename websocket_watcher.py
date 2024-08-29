import ccxt.pro
import asyncio


class Watcher:
    def __init__(self, taker_exchange, maker_exchange):

        self.taker_client = getattr(ccxt.pro, taker_exchange)()
        self.maker_client = getattr(ccxt.pro, maker_exchange)()
        self.taker_ob = None
        self.maker_ob = None

    async def watch_ob(self, client, update_func):
        while True:
            try:
                order_book = await client.watch_order_book('ALPH/USDT')
                print(f'Bid {order_book["bids"][0][0]} and ask {order_book["asks"][0][0]} on {client.name}')
                update_func(order_book)
                self.compare_order_books()
            except Exception as e:
                print(str(e))
                break
        await client.close()

    def compare_order_books(self):
        if self.taker_ob is not None and self.maker_ob is not None:
            if self.taker_ob['bids'][0] > self.maker_ob['bids'][0]:
                print('ARBARBARB')
                self.maker_client.create_order_ws()

    def update_taker_ob(self, ob):
        self.taker_ob = ob

    def update_maker_ob(self, ob):
        self.maker_ob = ob

    async def run(self):
        await asyncio.gather(
            self.watch_ob(self.taker_client, self.update_taker_ob),
            self.watch_ob(self.maker_client, self.update_maker_ob),
        )


comparator = Watcher('gate', 'bitget')

asyncio.run(comparator.run())
