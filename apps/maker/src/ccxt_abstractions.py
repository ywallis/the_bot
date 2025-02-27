import asyncio
import logging

import apps.maker.src.logging_config as logging_config
from apps.maker.src.errors import BadRequest, BrokerError, ExchangeError, RequestTimeout
from apps.maker.src.structs import CancellationMessage, CustomExchange, OrderMessage

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def cancel_order_return_confirmation(
    cancellation: CancellationMessage, client: CustomExchange
):
    last_error: Exception = Exception()

    for attempt in range(5):
        try:
            cancellation_confirmation = await client.cancel_order(
                symbol=cancellation["pair"], id=cancellation["id"]
            )

        except BadRequest as e:
            logger.info(e)
            logger.info("Order was likely fully filled.")
            return cancellation

        except RequestTimeout as e:
            logger.error(
                f"Cancellation timed out, attempt num.:{attempt + 1}. Error was: {e}"
            )
            last_error = e
            await asyncio.sleep(0.2)

        except ExchangeError as e:
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
    last_error: Exception = Exception()

    for attempt in range(5):
        try:
            order_confirmation = await client.create_limit_order(
                symbol=order["pair"],
                side=order["side"].value,
                amount=float(order["amount"]),
                price=float(order["price"]),
            )

        except RequestTimeout as e:
            logger.error(f"Order timed out, attempt num.:{attempt + 1}. Error was: {e}")
            last_error = e
            await asyncio.sleep(0.2)

        except ExchangeError as e:
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
