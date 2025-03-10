import json
import logging
import asyncio

from redis.asyncio import Redis

import apps.maker.src.logging_config as logging_config
from apps.maker.src.enums import OrderSide

logging_config.setup_logging()
logger = logging.getLogger(__name__)

async def check_if_solvent(buy_client: str, sell_client: str, price: float, quantity: float,
                           pair: str) -> bool:
    """This function checks if two exchanges have the necessary balances to place
    two arbitrage orders in their relevant assets."""

    batch = asyncio.gather()
    buy_client_balance, sell_client_balance = await batch

    base_asset = pair.split('/')[0]
    quote_asset = pair.split('/')[1]

    try:

        if (quantity * price * 2 < buy_client_balance[quote_asset]['free']
                and quantity * 2 < sell_client_balance[base_asset]['free']):
            logger.info(f'Check if solvent success.')
            return True
        else:
            logger.info(f'Insufficient funds!')
            return False

    except KeyError:

        # Error can occur if the subaccount never had an asset balance.

        print('Insufficient funds! Are you sure the right pair is selected?')
        logger.info('Insufficient funds! Are you sure the right pair is selected?')
        return False

async def retrieve_balances_redis(redis_instance: Redis, key: str):
    # Get the JSON string from Redis
    serialized_balances = await redis_instance.get(key)
    if serialized_balances is None:
        return None  # Key not found
    # Deserialize the JSON string back to a CCXT ob
    return json.loads(serialized_balances)

async def retrieve_ob_redis(redis_instance: Redis, key: str):
    # Get the JSON string from Redis
    serialized_ob = await redis_instance.get(key)
    if serialized_ob is None:
        return None  # Key not found
    # Deserialize the JSON string back to a CCXT ob
    return json.loads(serialized_ob)


def maker_order_sizer(
    maker_level: float,
    taker_book: list[list],
    side: OrderSide,
    min_spread: float,
    max_maker_size: float,
    min_maker_size: float = 10,
) -> float:
    """NEEDS FLESHING OUT!
    Goal of function is to watch how much liquidity is available on the taker client within the defined spread.
    """

    cumulative: float = 0
    if side == OrderSide.SELL:
        for level in taker_book:
            if maker_level >= level[0] * min_spread:
                cumulative += level[1]
            else:
                break

    if side == OrderSide.BUY:
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
