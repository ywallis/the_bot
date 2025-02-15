import logging
import logging_config 
from enums import OrderSide, MessageType
from structs import CancellationMessage, OrderMessage
from utils import parse_message
from ccxt.base.exchange import Exchange  # pyright: ignore[reportMissingTypeStubs]

import tomllib
import asyncio
from redis.asyncio import Redis
import json

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


def load_worker_settings():
    """Load worker settings from a TOML file."""
    with open(CONFIG_FILE, "rb") as f:
        config = tomllib.load(f)
    return config.get("exchanges", {})


async def worker(exchange, queue, ccxt_client):
    """Processes messages from the queue and sends them to the correct ccxt_client."""
    while True:
        data = await queue.get()
        if data is None:  # Shutdown signal
            queue.task_done()
            break

        parsed_data = parse_message(data)

        if parsed_data is None:
            logger.error(f"Invalid parsing for message {parsed_data}")

        elif parsed_data['kind'] == MessageType.ORDER:
            logger.debug(f"Worker [{exchange}] processing order: {data} -> {ccxt_client}")
            # await ccxt_client.create_order()
            await asyncio.sleep(0.01)  # Simulate async processing
            await redis_out.publish(data['id'], f"{data['id']} order confirmed on {exchange}")

        elif parsed_data['kind'] == MessageType.CANCELLATION:
            logger.debug(f"Worker [{exchange}] processing cancellation: {data} -> {ccxt_client}")
            # await ccxt_client.cancel_order()
            await asyncio.sleep(0.01)  # Simulate async processing
            await redis_out.publish(data['id'], f"{data['id']} cancellation confirmed on {exchange}")
        
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
                data = json.loads(message["data"])
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
