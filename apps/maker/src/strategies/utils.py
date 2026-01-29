"""Utility functions for trading strategies."""

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal

from redis.asyncio import Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.structs import (
    CancellationMessage,
    OrderBatchMessage,
    OrderBook,
    OrderMessage,
)

logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def send_processor_cancellation(
    redis: Redis, strategy: dict[str, str], identifier: str
) -> None:
    """
    Send a cancellation to the message processor for a strategy.

    Parameters
    ----------
    redis : Redis
        A redis client instance.
    strategy : dict[str, str]
        The current strategy item.
    identifier : str
        The identifier for the order to be cancelled.
    """
    cancellation = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=f"{strategy['identifier']}_{identifier}",
        exchange=strategy["maker_exchange"],
        id="",
        pair=strategy["symbol"],
    )
    flattened = json.dumps(dict(cancellation), default=str)
    await redis.publish(MESSAGE_PROCESSOR_CHANNEL, flattened)
    logger.info(f"Cancelation was sent: {cancellation}")


async def send_processor_order(redis: Redis, order: OrderMessage | OrderBatchMessage):
    """
    Send an order to the message processor for a strategy.

    Parameters
    ----------
    redis : Redis
        A redis client instance.
    order : OrderMessage | OrderBatchMessage
        The order item to be placed.
    """
    flattened = json.dumps(dict(order), default=str)
    await redis.publish(MESSAGE_PROCESSOR_CHANNEL, flattened)
    logger.info(f"Order was sent: {order}")


def min_max_usd_converter(
    price: float, min_size_usdt: float, max_size_usdt: float
) -> tuple[float, float]:
    """
    Convert the usd bandwith to an asset quantity.

    For example, if given "2, 10, 100", will return 5, 50.

    Parameters
    ----------
    price : float
        The use price of the asset.
    min_size_usdt : float
        The lowest usd value an order can have.
    max_size_usdt : float
        The highest usd value an order can have.

    Returns
    -------
    tuple[float, float]
        The min and max sizes in asset quantity.
    """
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
    last_order_timestamp: None | int = None,
) -> bool:
    """
    Check if clients have enough balance to execute a balanced order.

    Parameters
    ----------
    redis_instance : Redis
        A redis client instance.
    buy_client_id : str
        The short CCXT id for the buying client.
    sell_client_id : str
        The short CCXT id for the selling client.
    price : float
        The current price of an asset.
    quantity : float
        The quantity to be executed on both sides.
    pair : str
        The traded pair.
    last_order_timestamp : None | int
        The timestamp for the last order.

    Returns
    -------
    bool
        Whether enough balance is available on both sides.
    """
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

    if last_order_timestamp is not None:
        if last_order_timestamp > int(buy_client_balance["timestamp"]):
            logger.debug(
                f"Balance on {buy_client_id} hasn't been updated since last order."
            )
            return False
        if last_order_timestamp > int(sell_client_balance["timestamp"]):
            logger.debug(
                f"Balance on {sell_client_id} hasn't been updated since last order."
            )
            return False

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


async def retrieve_balances_redis(redis_instance: Redis, key: str) -> dict | None:
    """
    Retrieve the balance for a single exchange from redis.

    Parameters
    ----------
    redis_instance : Redis
        A redis client instance.
    key : str
        The symbol balance queried.

    Returns
    -------
    dict | None
        A dict of balances.
    """
    # Get the JSON string from Redis
    serialized_balances = await redis_instance.get(key)
    if serialized_balances is None:
        return None
    # Deserialize the JSON string back to a CCXT ob
    return json.loads(serialized_balances)


async def retrieve_ob_redis(redis_instance: Redis, key: str) -> OrderBook | None:
    """
    Retrieve an orderbook snapshot from redis.

    Parameters
    ----------
    redis_instance : Redis
        A redis client instance.
    key : str
        The key for the desired orderbook.

    Returns
    -------
    OrderBook | None
        The returned orderbook.
    """
    # Get the JSON string from Redis
    serialized_ob = await redis_instance.get(key)
    if serialized_ob is None:
        return None
    # Deserialize the JSON string back to a CCXT ob
    ob = json.loads(serialized_ob)
    return ob


def order_time() -> str:
    """
    Create a datetime-based stamp to make unique and custom order numbers.

    Returns
    -------
    str
        The custom timestamp, as a string.
    """
    return datetime.now().strftime("%y%m%d%H%M%S%f")


def generate_oid(strategy_identifier: str, order_identifier: str) -> str:
    """
    Generate a unique identifier for an order.

    Parameters
    ----------
    strategy_identifier : str
        The identifier for the strategy.
    order_identifier : str
        The identifier for the order.

    Returns
    -------
    str
        The custom time-based identifier.
    """
    return f"t-{order_time()}_{strategy_identifier}_{order_identifier}"


def generate_order(
    maker_id: str,
    price: float,
    amount: float,
    pair: str,
    side: OrderSide,
    strategy: dict[str, str],
    identifier: str,
) -> OrderMessage:
    """
    Generate a order item from parameters.

    Parameters
    ----------
    maker_id : str
        The ccxt id for the maker exchange.
    price : float
        The order price.
    amount : float
        The order amount.
    pair : str
        The pair for the order.
    side : OrderSide
        The side of the order.
    strategy : dict[str, str]
        The strategy the order comes from.
    identifier : str
        The order identifier.

    Returns
    -------
    OrderMessage
        A full order message.
    """
    order = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{strategy['identifier']}_{identifier}",
        exchange=maker_id,
        id=generate_oid(strategy["identifier"], identifier),
        exchange_id="_",
        pair=pair,
        side=side,
        order_type=OrderType.REPLACE,
        price=Decimal(price),
        amount=Decimal(amount),
    )
    return order


async def generate_order_replace(
    redis: Redis,
    maker_id: str,
    taker_id: str,
    price: float,
    amount: float,
    pair: str,
    side: OrderSide,
    strategy: dict[str, str],
    identifier: str,
    refresh: bool,
) -> OrderMessage | None:
    """
    Generate a new order and return it.

    The function also checks if both sides are solvent.
    If the refresh flag is True, a cancellation will also be sent.

    Parameters
    ----------
    redis : Redis
        A redis client instance.
    maker_id : str
        The CCXT short id for the maker side.
    taker_id : str
        The CCXT short id for the taker side.
    price : float
        The price for the order.
    amount : float
        The amount for the order.
    pair : str
        The traded pair.
    side : OrderSide
        The side for the order.
    strategy : dict[str, str]
        The full strategy item.
    identifier : str
        The identifier for the order.
    refresh : bool
        Whether a pre-existing order should be cancelled if any client is insolvent.

    Returns
    -------
    OrderMessage | None
        A full order message if solvent.
    """
    if side == OrderSide.SELL:
        sell_client_id = maker_id
        buy_client_id = taker_id
    else:
        sell_client_id = taker_id
        buy_client_id = maker_id

    if await check_if_solvent(
        redis, buy_client_id, sell_client_id, price, amount, pair
    ):
        order = generate_order(
            maker_id, price, amount, pair, side, strategy, identifier
        )

        await send_processor_order(redis, order)
        logger.debug(f"Order was created and sent: {order}")
        return order

    else:
        logger.debug("Client not solvent.")
        if refresh:
            logger.debug("Cancelling previous orders")
            await send_processor_cancellation(redis, strategy, identifier)
        return None


async def generate_take_take_order(
    redis: Redis,
    buy_exchange: str,
    sell_exchange: str,
    buy_price: float,
    sell_price: float,
    amount: float,
    pair: str,
    strategy: dict[str, str],
    identifier: str,
    last_order_timestamp: int,
) -> OrderBatchMessage | None:
    """
    Generate an order batch from two concurrent orders.

    This targets an immediate price discrepancy with two opposing orders.
    The function will increase amount for exchanges requiring the fee to be paid in the asset.
    Includes a simple throttle to avoid acting multiple times on the same snapshot.

    Parameters
    ----------
    redis : Redis
        A Redis client instance.
    buy_exchange : str
        The CCXT identifier for the buy-side exchange.
    sell_exchange : str
        The CCXT identifier for the sell-side exchange.
    buy_price : float
        The target buy price.
    sell_price : float
        The target sell price.
    amount : float
        The amount per side.
    pair : str
        The traded pair.
    strategy : dict[str, str]
        The full strategy item.
    identifier : str
        The common identifier for both orders.
    last_order_timestamp : int
        The timestamp for the last similar order. This is to prevent sending too many orders by accident.

    Returns
    -------
    OrderBatchMessage | None
        The order batch with both orders if solvent.
    """
    if await check_if_solvent(
        redis,
        buy_exchange,
        sell_exchange,
        sell_price,
        amount,
        pair,
        last_order_timestamp,
    ):
        common_id = generate_oid(strategy["identifier"], identifier)

        if buy_exchange == "gate" or buy_exchange == "bitget":
            fee_ratio = 1 / (1 - 0.001)

            buy_amount = round(amount * fee_ratio, 5)

        else:
            buy_amount = amount
        buy_order = OrderMessage(
            kind=MessageType.ORDER,
            strategy=f"{strategy['identifier']}_{identifier}",
            exchange=buy_exchange,
            id=common_id,
            exchange_id="_",
            pair=pair,
            side=OrderSide.BUY,
            order_type=OrderType.UNIQUE,
            price=Decimal(buy_price),
            amount=Decimal(buy_amount),
        )
        sell_order = OrderMessage(
            kind=MessageType.ORDER,
            strategy=f"{strategy['identifier']}_{identifier}",
            exchange=sell_exchange,
            id=common_id,
            exchange_id="_",
            pair=pair,
            side=OrderSide.SELL,
            order_type=OrderType.UNIQUE,
            price=Decimal(sell_price),
            amount=Decimal(amount),
        )

        order_batch = OrderBatchMessage(
            kind=MessageType.ORDERBATCH,
            strategy=f"{strategy['identifier']}_{identifier}",
            id=common_id,
            orders=[buy_order, sell_order],
        )

        await send_processor_order(redis, order_batch)
        logger.debug(f"Order batch was created and sent: {order_batch}")
        return order_batch

    else:
        logger.debug("Client not solvent.")
        return None


def maker_order_sizer(
    maker_level: float,
    taker_book: list[list[float]] | list[list[int]],
    side: OrderSide,
    min_spread: float,
    max_maker_size: float,
    min_maker_size: float = 0,
) -> float:
    """
    Check how much liquidity is available on the taker client for a spread.

    This gives us an estimate of how large of an order we can safely make.

    Parameters
    ----------
    maker_level : float
        The intended price of our maker order.
    taker_book : list[list[float]] | list[list[int]]
        The order book for the taker side.
    side : OrderSide
        The side of our maker order.
    min_spread : float
        The minimum spread required by our strategy.
    max_maker_size : float
        The maximum size allowed by our strategy.
    min_maker_size : float
        The minimum size required by our strategy.

    Returns
    -------
    float
        The future size of our order.
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


def within_percentage_range(
    x: float | Decimal, y: float | Decimal, percentage: float
) -> bool:
    """
    Check whether x is within a percentage range from y.

    Parameters
    ----------
    x : float | Decimal
        Our original value.
    y : float | Decimal
        The target value.
    percentage : float
        The range we allow x to be from y.

    Returns
    -------
    bool
        The result of the calculation.
    """
    if type(x) is not float:
        x = float(x)
    if type(y) is not float:
        y = float(y)
    lower_bound = y * (1 - percentage / 100)
    upper_bound = y * (1 + percentage / 100)

    return lower_bound <= x <= upper_bound


def ob_matcher(
    bids: list[list[float]],
    asks: list[list[float]],
    spread: float,
    sizing: float,
    max_order_size: float,
    min_order_size: float,
    extend_spread: int = 0,
) -> tuple[float, float, float] | None:
    """
    Go through two order books and return arbitrage values.

    This targets order books with significant inefficiencies (bid > ask).

    Parameters
    ----------
    bids : list[list[float]]
        The order book for bids.
    asks : list[list[float]]
        The order book for asks.
    spread : float
        By how much we want bid to be larger than ask.
    sizing : float
        How much of the current inefficiency we want to target (%).
    max_order_size : float
        The maximum order size allowed by our strategy.
    min_order_size : float
        The minimum order size required by our strategy.
    extend_spread : int
        How many levels beyond the optimal we want to push beyond the optimal spread.
        This can be used to help guarantee execution for limit orders, or increase skew for cost-based market buy orders.

    Returns
    -------
    tuple[float, float, float] | None
        The target ask, bid and order size if an efficiency exists.
    """
    depth: int = 0
    cumulative_bids: float = 0
    cumulative_asks: float = 0

    while depth + extend_spread < len(bids) and depth + extend_spread < len(asks):
        if bids[depth][0] >= asks[depth][0] * spread:
            cumulative_bids += bids[depth][1]
            cumulative_asks += asks[depth][1]

            target_order_size: float = min(cumulative_bids, cumulative_asks) * sizing

            if target_order_size > max_order_size:
                target_order_size = max_order_size

            if target_order_size > min_order_size:
                target_bid = bids[depth + extend_spread][0]
                target_ask = asks[depth + extend_spread][0]
                logger.debug(
                    f"Matched order books, ask:{target_ask}, bid:{target_bid}, amount {target_order_size}"
                )
                return target_ask, target_bid, target_order_size
            else:
                depth += 1
        else:
            break

    logger.debug("Could not successfully match order books")
    return None
