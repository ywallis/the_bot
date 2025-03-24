import json
import logging
import asyncio

from redis.asyncio import Redis

from apps.maker.src.structs import CancellationMessage, OrderMessage
from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL 
from apps.maker.src.enums import MessageType

import apps.maker.src.logging_config as logging_config
from apps.maker.src.enums import OrderSide

logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def send_processor_init_cancellation(redis: Redis, strategy: dict[str, str]):
    cancellation = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=strategy["identifier"],
        exchange=strategy["maker_exchange"],
        id="",
        pair=strategy["symbol"],
    )
    flattened = json.dumps(dict(cancellation), default=str)
    await redis.publish(MESSAGE_PROCESSOR_CHANNEL, flattened)

async def send_processor_order(redis: Redis, order: OrderMessage):
    flattened = json.dumps(dict(order), default=str)
    await redis.publish(MESSAGE_PROCESSOR_CHANNEL, flattened)


def min_max_usd_converter(
    price: float, min_size_usdt: float, max_size_usdt: float
) -> tuple[float, float]:
    min_size = round(min_size_usdt / price, 4)
    max_size = round(max_size_usdt / price, 4)

    logger.debug(f"Using best taker bid as price: {price}.")
    logger.debug(f"Min size is {min_size}.")
    logger.debug(f"Max size is {max_size}.")

    return min_size, max_size


async def check_if_solvent(
    redis_instance: Redis,
    buy_client_id: str,
    sell_client_id: str,
    price: float,
    quantity: float,
    pair: str,
) -> bool:
    """This function checks if two exchanges have the necessary balances to place
    two arbitrage orders in their relevant assets."""

    batch = asyncio.gather(
        retrieve_balances_redis(redis_instance, f"balance-{buy_client_id}"),
        retrieve_balances_redis(redis_instance, f"balance-{sell_client_id}"),
    )
    buy_client_balance, sell_client_balance = await batch

    base_asset = pair.split("/")[0]
    quote_asset = pair.split("/")[1]

    logger.debug(f"Buy client balance is {buy_client_balance}")
    logger.debug(f"Sell client balance is {sell_client_balance}")
    if buy_client_balance is None or sell_client_balance is None:
        logger.error("Error retrieving balance from Redis")
        raise Exception("Error retrieving balance")

    try:
        if (
            quantity * price * 2 < buy_client_balance[quote_asset]["free"]
            and quantity * 2 < sell_client_balance[base_asset]["free"]
        ):
            logger.debug("Solvency check sucessful")
            return True
        else:
            logger.debug("Insufficient funds!")
            return False

    except KeyError:
        # Error can occur if the subaccount never had an asset balance.

        logger.debug("Insufficient funds! Are you sure the right pair is selected?")
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
