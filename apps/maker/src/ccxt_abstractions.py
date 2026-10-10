"""Module for abstracting CCXT operations."""

import asyncio
import logging

import apps.shared.src.logging_config as logging_config
from apps.maker.src.enums import OrderSide, OrderType
from apps.maker.src.errors import BrokerError
from apps.maker.src.structs import CancellationMessage, OrderMessage
from apps.shared.src.errors import (
    BadRequest,
    ExchangeError,
    InvalidOrder,
    NetworkError,
    RequestTimeout,
)
from apps.shared.src.structs import CustomExchange

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def cancel_order_return_confirmation(
    cancellation: CancellationMessage, client: CustomExchange
) -> CancellationMessage | dict[str, str]:
    """Allow the broker to cancel an order with a CCXT client.

    Retry logic and behavior in case the order has been filled prior
    to cancellation is abstracted away from the user.

    Parameters
    ----------
    cancellation : CancellationMessage
        The cancellation to be executed
    client : CustomExchange
        The CCXT exchange client instance to be used

    Returns
    -------
    CancellationMessage | dict[str, str]
        The original cancellation in case the order was fully filled,
        a CCXT order object otherwise

    """
    last_error: Exception = Exception()

    for attempt in range(5):
        try:
            cancellation_confirmation = await client.cancel_order(
                symbol=cancellation["pair"], id=cancellation["id"]
            )

        except BadRequest as e:
            logger.info(e)
            logger.info("Bad Request, order was likely fully filled.")
            return cancellation

        except InvalidOrder as e:
            logger.info(e)
            logger.info("Invalid order, order was likely fully filled.")
            return cancellation

        except RequestTimeout as e:
            logger.error(
                f"Cancellation timed out, attempt num.:{attempt + 1}. Error was: {e}"
            )
            last_error = e
            await asyncio.sleep(0.2)

        except (ExchangeError, NetworkError) as e:
            logger.error(
                f"Cancellation rejected by exchange, attempt num.:{attempt + 1}. Error was: {e}"
            )
            last_error = e
            await asyncio.sleep(0.2)

        else:
            return cancellation_confirmation

    # If this point is reached, it means no cancellation was confirmed after all attempts

    raise BrokerError(
        f"Broker was unable to confirm a cancellation. Last error was {last_error}"
    )


def market_order_needs_price(client: CustomExchange, order: OrderMessage) -> bool:
    """
    Tell whether a market order must reach CCXT with a price.

    A market sell never does. A market buy does unless the venue's options
    say ``createMarketBuyOrderRequiresPrice = false``, at the top level or
    under ``createOrder``: some CCXT classes refuse a market buy without a
    price and only their code knows it, so the safe default is to pass one,
    and a venue that sizes market buys by quantity says so in config.

    Parameters
    ----------
    client : CustomExchange
        The CCXT exchange client.
    order : OrderMessage
        The order.

    Returns
    -------
    bool
        True if the price has to be passed.
    """
    if order["side"] != OrderSide.BUY:
        return False
    options = getattr(client, "options", None)
    if not isinstance(options, dict):
        return True
    key = "createMarketBuyOrderRequiresPrice"
    nested = options.get("createOrder")
    for scope in (options, nested if isinstance(nested, dict) else {}):
        if scope.get(key) is False:
            return False
    return True


async def create_and_return_order(
    order: OrderMessage, client: CustomExchange
) -> dict[str, str]:
    """Allow the broker to place an order with a CCXT client.

    Retry logic is abstracted away from the user.

    Parameters
    ----------
    order : OrderMessage
        The order message to be executed
    client : CustomExchange
        The CCXT exchange client instance to be used

    Returns
    -------
    dict[str, str]
        A CCXT order object

    """
    last_error: Exception = Exception()

    if order.get("order_type") == OrderType.MARKET:
        order_type = "market"
    else:
        order_type = "limit"

    # CCXT's unified post-only flag: the venue rejects the order instead of
    # letting it take, so a quote that crossed the book by the time it
    # arrived is refused rather than filled at taker fee against the market
    # it was priced off.
    params: dict[str, object] = {"clientOrderId": order["id"]}
    if order.get("post_only"):
        params["postOnly"] = True
    # CCXT's unified reduce-only flag for contract markets: the venue
    # shrinks the position and refuses to open one the other way.
    if order.get("reduce_only"):
        params["reduceOnly"] = True

    price: float | None = float(order["price"])
    if order_type == "market" and not market_order_needs_price(client, order):
        # Given a price, some venues' CCXT classes turn a market order into
        # a quote-cost order of amount times price. A hedge carries the
        # maker fill's price, and when the market had run away from it the
        # hedge came out short: 13.35 bought for a 14.5 fill on 2026-10-10.
        price = None

    for attempt in range(5):
        try:
            order_confirmation = await client.create_order(
                symbol=order["pair"],
                type=order_type,
                side=order["side"].value,
                amount=float(order["amount"]),
                price=price,
                params=params,
            )

        except RequestTimeout as e:
            logger.error(f"Order timed out, attempt num.:{attempt + 1}. Error was: {e}")
            last_error = e
            await asyncio.sleep(0.2)

        except (ExchangeError, NetworkError) as e:
            logger.error(
                f"Order rejected by exchange, attempt num.:{attempt + 1}. Error was: {e}"
            )
            last_error = e
            await asyncio.sleep(0.2)

        else:
            return order_confirmation

    # If this point is reached, it means no order was confirmed after all attempts

    raise BrokerError(
        f"Broker was unable to confirm an order. Last error was {last_error}"
    )
