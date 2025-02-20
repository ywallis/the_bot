import logging
import logging_config 
from enums import OrderSide, MessageType
from structs import CancellationMessage, OrderMessage, CustomExchange
from errors import BrokerError
from utils import parse_message
import ccxt.async_support as ccxt
from ccxt_abstractions import create_and_return_order
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

        else:

            logger.debug(f"Worker [{exchange_name}] processing {data['kind'].value}: {data} -> {ccxt_client}")

            try:

                if data['kind'] == MessageType.ORDER:

                    confirmation = await create_and_return_order(data, ccxt_client)

                elif data['kind'] == MessageType.CANCELLATION:
                    # await ccxt_client.cancel_order()
                    await asyncio.sleep(0.01)  # Simulate async processing
                    confirmation = "fun"
            
                else:
                    logger.error(f"Message of unknown type was allowed through: {data}")
                    raise BrokerError(f"Message of unknown type was allowed through: {data}")


            except BrokerError as e:
                
                
                await redis_out.publish(data['id'], f"Error for {data['id']} on {exchange_name} is {e}")
            
            else:

                # Happy flow
                await redis_out.publish(data['id'], f"Reply from broker for {data['id']} on {exchange_name} is {confirmation}")
            
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
