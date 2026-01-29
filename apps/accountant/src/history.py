import asyncio
from datetime import datetime, timedelta

import pytz

from apps.accountant.src.utils import (
    export_to_sql,
    load_pg_config,
    retrieve_and_prepare_orders,
    retrieve_and_prepare_trades,
)
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.structs import CustomExchange
from apps.shared.src.errors import NetworkError, RequestTimeout, ExchangeError
from apps.shared.src.utils import exchange_and_pair


async def get_history(
    client: CustomExchange, pg_config: dict[str, str], pair: str, start_date_str: str
):
    start_date = datetime.strptime(start_date_str, "%d/%m/%y")
    loop_start = start_date
    original_loop_size = timedelta(minutes=30)
    loop_size = original_loop_size

    while loop_start < start_date + timedelta(days=1):
        loop_end = loop_start + loop_size
        loop_start_utc = pytz.timezone("UTC").localize(loop_start)
        loop_end_utc = pytz.timezone("UTC").localize(loop_end)
        loop_start_input = int(loop_start_utc.timestamp() * 1000)
        loop_end_input = int(loop_end_utc.timestamp() * 1000)

        print(loop_start_utc)
        orders_in_timeframe = await retrieve_and_prepare_orders(
            client, pair, loop_start_input, loop_end_input
        )
        print(
            f"There are {len(orders_in_timeframe)} {pair} orders between {loop_start_utc.strftime('%Y-%m-%d %H:%M:%S')} "
            f"and {loop_end_utc.strftime('%Y-%m-%d %H:%M:%S')} on {client}."
        )
        trades_in_timeframe = await retrieve_and_prepare_trades(
            client, pair, loop_start_input, loop_end_input
        )
        print(
            f"There are {len(trades_in_timeframe)} {pair} trades between {loop_start_utc.strftime('%Y-%m-%d %H:%M:%S')} "
            f"and {loop_end_utc.strftime('%Y-%m-%d %H:%M:%S')} on {client}."
        )

        # This section effectively implements a dirty bisection style sizing of the timeframe

        if len(orders_in_timeframe) > 99 or len(trades_in_timeframe) > 99:
            loop_size = loop_size // 2
            print(f"Over limit, reducing to {loop_size}")
            continue

        print(loop_end_utc)
        if len(trades_in_timeframe) != 0:
            export_to_sql(trades_in_timeframe, pg_config, "trades", client.name)
        if len(orders_in_timeframe) != 0:
            export_to_sql(orders_in_timeframe, pg_config, "orders", client.name)

        await asyncio.sleep(1)

        loop_start = loop_end

        # Resetting loop size to higher value.

        loop_size = original_loop_size


async def loop(
    all_clients: dict[str, CustomExchange],
    exchange_and_pair: set[tuple[str, str]],
    pg_config: dict[str, str],
    start_date_input: str,
):
    for option in exchange_and_pair:
        try:
            await get_history(
                all_clients[option[0]], pg_config, option[1], start_date_input
            )
        except ExchangeError as e:
            print("Exchange error, retrying.")
            print(e)
        except RequestTimeout as e:
            print("Request timeout, retrying.")
            print(e)
        except NetworkError as e:
            print("Network error, retrying.")
            print(e)

    for client in all_clients.values():
        await client.close()


if __name__ == "__main__":
    pg_config = load_pg_config()
    start_date_input = input(
        "Enter the date (DD/MM/YY) for historical downloads (7D history):"
    )
    print(exchange_and_pair)

    asyncio.run(
        loop(authenticated_clients, exchange_and_pair, pg_config, start_date_input)
    )
