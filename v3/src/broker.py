import logging
import logging_config 
from typing import Protocol
from enums import OrderSide, MessageType
from structs import CancellationMessage, OrderMessage
from utils import parse_message
import ccxt.async_support as ccxt
import tomllib
import asyncio
from redis.asyncio import Redis


# TODO
# - Replying back to the message processor via redis needs to happen in a new "worked" which collects results.
#   As of now, I don't know how I will handle things such as retry mechanisms? Do they need to be integrated in the flow? I will essentially await my abstraction?


# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)

redis_in = Redis(host="localhost", port=6379, decode_responses=True)
redis_out = Redis(host="localhost", port=6379, decode_responses=True)

CONFIG_FILE = "config.toml"
CHANNEL_NAME = "broker"

fake_clients = {"mexc": "mexc_client",
                "gate": "gate_client",
                "bitget": "bitget_client"}


class CustomExchange(Protocol):
    async def create_limit_order(self, symbol: str, side: str, amount: float, price: float) -> dict: ...


def load_worker_settings():
    """Load worker settings from a TOML file."""
    with open(CONFIG_FILE, "rb") as f:
        config = tomllib.load(f)
    return config.get("exchanges", {})


async def worker(exchange_name: str, queue: asyncio.Queue, ccxt_client: CustomExchange):
    """Processes messages from the queue and sends them to the correct ccxt_client."""

    # STILL A BIG GAP IN THE PROCESSING, THE TRY/EXCEPT ONLY LOOKS AT WHETHER THE EXCHANGE RESPONDED, NOT WHAT THE RESPONSE IS.

    while True:
        data = await queue.get()
        if data is None:  # Shutdown signal
            queue.task_done()
            break

        elif data['kind'] == MessageType.ORDER:
            logger.debug(f"Worker [{exchange_name}] processing order: {data} -> {ccxt_client}")
            try:
                order = await ccxt_client.create_limit_order(symbol=data['pair'], side=data['side'].value, amount=data['amount'], price=data['price'])
                # await asyncio.sleep(0.01)  # Simulate async processing
            
            except ccxt.ExchangeError as error:
                logger.error(f"Order creation seems to have failed")
                await redis_out.publish(data['id'], f"{data['id']} order failed on {exchange_name}")

            else:

                await redis_out.publish(data['id'], f"{data['id']} order response on {exchange_name} is :{order}")

        elif data['kind'] == MessageType.CANCELLATION:
            logger.debug(f"Worker [{exchange_name}] processing cancellation: {data} -> {ccxt_client}")
            # await ccxt_client.cancel_order()
            await asyncio.sleep(0.01)  # Simulate async processing
            await redis_out.publish(data['id'], f"{data['id']} cancellation confirmed on {exchange_name}")
        
        queue.task_done()


async def redis_subscriber(queues):
    """Listens to Redis channel and routes messages to the correct queue."""
    redis = await Redis(host="localhost", port=6379, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe(CHANNEL_NAME)

    logger.debug("Subscribed to Redis channel:", CHANNEL_NAME)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                print(message['data'])
                data = parse_message(message["data"])
                if data is None:
                    logger.error(f"Invalid parsing for message {data}")
                else:

                    exchange = data.get("exchange")

                    if exchange in queues:
                        await queues[exchange].put(data)
                    else:
                        logger.error(
                            f"Warning: Received unknown message type '{exchange}', ignoring..."
                        )
    finally:
        await pubsub.unsubscribe(CHANNEL_NAME)
        await redis.close()


async def main():
    worker_settings = load_worker_settings()
    queues = {exchange: asyncio.Queue() for exchange in worker_settings}

    subscriber_task = asyncio.create_task(redis_subscriber(queues))


    for exchange in worker_settings:
        asyncio.create_task(
            worker(exchange, queues[exchange], fake_clients[exchange])
        )

    await asyncio.gather(subscriber_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
