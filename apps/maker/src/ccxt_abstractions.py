"""
This module provides abstraction layers over CCXT exchange operations.
It handles retry logic for creating and canceling orders, managing common exchange errors.
"""

import asyncio
import logging

import apps.shared.src.logging_config as logging_config
from apps.maker.src.enums import OrderType
from apps.maker.src.errors import BrokerError
from apps.maker.src.structs import CancellationMessage, OrderMessage
from apps.shared.src.errors import (
    BadRequest,
    ExchangeError,
    NetworkError,
    InvalidOrder,
    RequestTimeout,
)
from apps.shared.src.structs import CustomExchange

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def cancel_order_return_confirmation(
    cancellation: CancellationMessage, client: CustomExchange
):
    """
    Attempt to cancel an order on the exchange with retry logic.

    Parameters
    ----------
    cancellation : CancellationMessage
        The cancellation message containing order details.
    client : CustomExchange
        The exchange client instance.

    Returns
    -------
    dict
        The confirmation message from the exchange (or the original cancellation message if already filled).

    Raises
    ------
    BrokerError
        If the cancellation fails after all retry attempts.
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


async def create_and_return_order(order: OrderMessage, client: CustomExchange):
    """
    Attempt to create an order on the exchange with retry logic.

    Parameters
    ----------
    order : OrderMessage
        The order message containing order details.
    client : CustomExchange
        The exchange client instance.

    Returns
    -------
    dict
        The order confirmation from the exchange.

    Raises
    ------
    BrokerError
        If the order creation fails after all retry attempts.
    """
    last_error: Exception = Exception()

    if order.get("order_type") == OrderType.MARKET:
        order_type = "market"
    else:
        order_type = "limit"

    for attempt in range(5):
        try:
            order_confirmation = await client.create_order(
                symbol=order["pair"],
                type=order_type,
                side=order["side"].value,
                amount=float(order["amount"]),
                price=float(order["price"]),
                params={"clientOrderId": order["id"]},
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
