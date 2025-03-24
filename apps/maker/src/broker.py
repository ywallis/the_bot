import asyncio
import json
import logging
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from apps.maker.src.constants import REDIS_HOSTNAME, REDIS_PORT, BROKER_CHANNEL
import apps.maker.src.logging_config as logging_config
from apps.maker.src.ccxt_abstractions import (
    cancel_order_return_confirmation,
    create_and_return_order,
)
from apps.maker.src.enums import MessageType
from apps.maker.src.errors import BrokerError
from apps.maker.src.exchange_clients import authenticated_clients, symbols
from apps.maker.src.structs import (
    CancellationMessage,
    CustomExchange,
    OrderBatchMessage,
    OrderMessage,
)
from apps.maker.src.utils import (
    is_cancellation_message,
    is_order_message,
    order_from_ccxt,
    parse_message,
)

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def fetch_all_open(clients: dict[str, CustomExchange]) -> OrderBatchMessage:
    all_orders: list[OrderMessage] = []

    for name, client in clients.items():
        for symbol in symbols:
            orders = await client.fetch_open_orders(symbol)
            for order in orders:
                all_orders.append(order_from_ccxt(order, name))

    return OrderBatchMessage(
        kind=MessageType.ORDERBATCH,
        strategy="recollection",
        id="recollection",
        orders=all_orders,
    )


async def process_message(
    message: CancellationMessage | OrderMessage | Any,
    results_queue: asyncio.Queue,
    ccxt_client: CustomExchange,
):
    try:
        if is_order_message(message):
            # if message.get("kind") == MessageType.ORDER:

            logger.debug(
                f"Message with id {message['id']} was identified as {message['kind']}"
            )
            confirmation = await create_and_return_order(message, ccxt_client)

        elif is_cancellation_message(message):
            # elif message.get("kind") == MessageType.CANCELLATION:
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


async def results_worker(redis: Redis, results_queue: asyncio.Queue):
    """Processes the results queue and publishes responses via Redis."""

    while True:
        order_id, msg_type, message = await results_queue.get()
        if order_id is None:  # Shutdown signal
            results_queue.task_done()
            break

        logger.debug(f"Publishing result to Redis: {message}")
        await redis.publish(order_id, f"{msg_type}|{message}")

        results_queue.task_done()


async def redis_subscriber(redis: Redis, pubsub: PubSub, queues: dict[str, asyncio.Queue]):
    """Listens to Redis channel and routes messages to the correct queue."""
    await pubsub.subscribe(BROKER_CHANNEL)

    logger.debug(f"Subscribed to Redis channel: {BROKER_CHANNEL}")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                logger.debug(message["data"])
                data = parse_message(message["data"])
                if data is None:
                    logger.error(f"Invalid parsing for message {data}")
                else:
                    exchange = data.get("exchange")

                    if exchange in queues:
                        await queues[exchange].put(data)
                    # Should only check at message processor initialization
                    elif exchange == "INIT":
                        logger.debug("INIT condition triggered")
                        all_open_orders = await fetch_all_open(authenticated_clients)
                        await redis.publish(
                            "INIT", json.dumps(dict(all_open_orders), default=str)
                        )

                    else:
                        logger.error(
                            f"Warning: Received unknown message type '{exchange}', ignoring..."
                        )
    finally:
        await pubsub.unsubscribe(BROKER_CHANNEL)


async def main():
    redis = await Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, decode_responses=True)
    pubsub: PubSub = redis.pubsub()

    worker_queues = {
        exchange: asyncio.Queue() for exchange in authenticated_clients.keys()
    }

    results_queue = asyncio.Queue()
    results_task = asyncio.create_task(results_worker(redis, results_queue))
    subscriber_task = asyncio.create_task(redis_subscriber(redis, pubsub, worker_queues))

    for exchange in authenticated_clients.keys():
        asyncio.create_task(
            worker(
                worker_queues[exchange], results_queue, authenticated_clients[exchange]
            )
        )

    await asyncio.gather(subscriber_task, results_task, return_exceptions=True)

    await redis.close()


if __name__ == "__main__":
    asyncio.run(main())
