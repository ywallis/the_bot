import asyncio

from apps.shared.src.exchange_clients import authenticated_clients

async def main():
    for client in authenticated_clients.values():
        balances = await client.fetch_balance()
        assert isinstance(balances["free"], dict)
        for symbol, amount in balances["free"].items():
            print(symbol, amount)



if __name__ == "__main__":
    asyncio.run(main())
