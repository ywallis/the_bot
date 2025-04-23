from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.utils import exchange_and_pair

import asyncio


async def loop():
    while True:
        for exchange_name, pair in exchange_and_pair:
            client = authenticated_clients[exchange_name]
            # logger.info()
            orders = await client.fetch_open_orders(pair)
            # trades = await client.fet(pair)


        await asyncio.sleep(10)



if __name__ == "__main__":
    asyncio.run(loop())
