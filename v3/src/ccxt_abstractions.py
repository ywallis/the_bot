import asyncio
import logging
import logging_config
from structs import CancellationMessage, OrderMessage, CustomExchange
from errors import ExchangeError, BrokerError

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)

async def cancel_order_return_confirmation(cancellation: CancellationMessage, client: CustomExchange):

    for attempt in range(5):

        try:

            cancellation_confirmation = await client.cancel_order(symbol=cancellation['pair'], id=cancellation['id'])
        
        except ExchangeError as e:

            logger.error(f"Order rejected by exchange, attempt num.:{attempt + 1}. Error was: {e}")
            await asyncio.sleep(0.2)

        else:

            return cancellation_confirmation 
    
    # If this point is reached, it means no cancellation was confirmed after all attempts
    
    raise BrokerError("Broker was unable to confirm an order") 

async def create_and_return_order(order: OrderMessage, client: CustomExchange):

    for attempt in range(5):

        try:

            order_confirmation = await client.create_limit_order(symbol=order['pair'], side=order['side'].value, amount=float(order['amount']), price=float(order['price']))
        
        except ExchangeError as e:

            logger.error(f"Order rejected by exchange, attempt num.:{attempt + 1}. Error was: {e}")
            await asyncio.sleep(0.2)

        else:

            return order_confirmation
    
    # If this point is reached, it means no order was confirmed after all attempts
    
    raise BrokerError("Broker was unable to confirm an order") 
