import logging
import logging_config
from enums import MessageType
from structs import CustomExchange
from errors import BrokerError
from utils import parse_message
from ccxt_abstractions import create_and_return_order, cancel_order_return_confirmation
import asyncio
from redis.asyncio import Redis
from exchange_clients import authenticated_clients

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)

redis_in = Redis(host="localhost", port=6379, decode_responses=True)
redis_out = Redis(host="localhost", port=6379, decode_responses=True)

CONFIG_FILE = "config.toml"
CHANNEL_NAME = "broker"


async def process_message(
    message, results_queue: asyncio.Queue, ccxt_client: CustomExchange
):

    try:

        if message["kind"] == MessageType.ORDER:

            logger.debug(
                f"Message with id {message['id']} was identified as {message['kind']}"
            )
            # This needs to change to "add_task asap"
            confirmation = await create_and_return_order(message, ccxt_client)

        elif message["kind"] == MessageType.CANCELLATION:
            logger.debug(
                f"Message with id {message['id']} was identified as {message['kind']}"
            )
            confirmation = await cancel_order_return_confirmation(message, ccxt_client)

        else:
            logger.error(f"Message of unknown type was allowed through: {message}")
            raise BrokerError(f"Message of unknown type was allowed through: {message}")

        # Happy flow
        await results_queue.put((message["id"], message["kind"], confirmation))

    except BrokerError as e:

        # Sad flow
        await results_queue.put((message["id"], MessageType.ERROR, e))


async def worker(
    queue: asyncio.Queue, results_queue: asyncio.Queue, ccxt_client: CustomExchange
):
    """Processes messages from the queue and sends them to the correct ccxt_client."""

    while True:
        message = await queue.get()
        if message is None:  # Shutdown signal
            queue.task_done()
            break

        else:

            logger.debug(
                f"Worker [{ccxt_client.name}] processing {message['kind'].value}: {message} -> {ccxt_client.name}"
            )

            asyncio.create_task(process_message(message, results_queue, ccxt_client))

            queue.task_done()


async def results_worker(results_queue: asyncio.Queue):
    """Processes the results queue and publishes responses via Redis."""

    while True:
        order_id, msg_type, message = await results_queue.get()
        if order_id is None:  # Shutdown signal
            results_queue.task_done()
            break

        logger.debug(f"Publishing result to Redis: {message}")
        await redis_out.publish(order_id, f"{msg_type}|{message}")

        results_queue.task_done()


async def redis_subscriber(queues: dict[str, asyncio.Queue]):
    """Listens to Redis channel and routes messages to the correct queue."""
    redis = await Redis(host="localhost", port=6379, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe(CHANNEL_NAME)

    logger.debug("Subscribed to Redis channel:", CHANNEL_NAME)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                print(message["data"])
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

    worker_queues = {
        exchange: asyncio.Queue() for exchange in authenticated_clients.keys()
    }

    results_queue = asyncio.Queue()
    results_task = asyncio.create_task(results_worker(results_queue))
    subscriber_task = asyncio.create_task(redis_subscriber(worker_queues))

    for exchange in authenticated_clients.keys():
        asyncio.create_task(
            worker(
                worker_queues[exchange], results_queue, authenticated_clients[exchange]
            )
        )

    await asyncio.gather(subscriber_task, results_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
