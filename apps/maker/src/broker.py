"""Module for handling broker operations and order execution."""

import asyncio
import json
import logging
import signal
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import PubSub

import apps.shared.src.logging_config as logging_config
from apps.maker.src.ccxt_abstractions import (
    cancel_order_return_confirmation,
    create_and_return_order,
)
from apps.maker.src.constants import BROKER_CHANNEL, REDIS_HOSTNAME, REDIS_PORT
from apps.maker.src.enums import MessageType
from apps.maker.src.errors import BrokerError
from apps.maker.src.structs import (
    CancellationMessage,
    OrderBatchMessage,
    OrderMessage,
)
from apps.maker.src.utils import (
    is_cancellation_message,
    is_order_message,
    order_from_ccxt,
    parse_message,
)
from apps.shared.src.errors import NetworkError
from apps.shared.src.exchange_clients import (
    authenticated_clients,
    load_clients,
    symbols,
)
from apps.shared.src.structs import CustomExchange

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def fetch_all_open(clients: dict[str, CustomExchange]) -> OrderBatchMessage:
    """Fetch all currently open orders on all clients.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        A dict mapping CCXT short id to CCXT client instance

    Returns
    -------
    OrderBatchMessage
        A batch of all retrieved open orders

    """
    all_orders: list[OrderMessage] = []

    for name, client in clients.items():
        for symbol in symbols:
            attempt: int = 1
            while True:
                try:
                    orders = await client.fetch_open_orders(symbol)
                    logger.info(f"Fetched {symbol} orders from {client}: {orders}")
                    for order in orders:
                        all_orders.append(order_from_ccxt(order, name))
                    break
                except NetworkError as e:
                    logger.error(
                        f"Network error when fetching order, attempt {attempt}: {e}"
                    )
                    attempt += 1
                    if attempt > 3:
                        raise Exception(
                            f"Fetching orders failed: client {client}, attempt n. {attempt}"
                        )

    logger.debug(f"Orders were retrieved from exchanges: {all_orders}")

    return OrderBatchMessage(
        kind=MessageType.ORDERBATCH,
        strategy="recollection",
        id="recollection",
        orders=all_orders,
    )


async def process_message(
    message: CancellationMessage | OrderMessage,
    results_queue: asyncio.Queue,
    ccxt_client: CustomExchange,
):
    """Process a message coming from the message processor.

    Parameters
    ----------
    message : CancellationMessage | OrderMessage
        The message to be processed
    results_queue : asyncio.Queue
        The queue in which results will be placed
    ccxt_client : CustomExchange
        A CCXT exchange client instance

    """
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
    """Spawn a worker which will process messages destined to a specific exchange.

    Parameters
    ----------
    queue : asyncio.Queue
        The queue for the worker's exchange
    results_queue : asyncio.Queue
        The queue the results will be placed in
    ccxt_client : CustomExchange
        The CCXT exchange client instance for the exchange

    """
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
    """Spawn a worker which will send processed messages to the processor.

    Parameters
    ----------
    redis : Redis
        A redis client instance
    results_queue : asyncio.Queue
        The results queue the worker will work to empty

    """
    while True:
        order_id, msg_type, message = await results_queue.get()
        if order_id is None:  # Shutdown signal
            results_queue.task_done()
            break

        logger.debug(f"Publishing result to Redis: {message}")
        await redis.publish(order_id, f"{msg_type}|{message}")

        results_queue.task_done()


async def redis_subscriber(
    redis: Redis, pubsub: PubSub, queues: dict[str, asyncio.Queue]
):
    """Listen to a Redis channel and route messages to the right queue.

    Parameters
    ----------
    redis : Redis
        A Redis client instance used to communicate back to processor
    pubsub : PubSub
        The pubsub Redis instance used to receive messages
    queues : dict[str, asyncio.Queue]
        A dict mapping CCXT short ids to corresponding queues

    """
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
                        logger.info("INIT message received")
                        await load_clients()
                        try:
                            all_open_orders = await fetch_all_open(
                                authenticated_clients
                            )
                            logger.info(all_open_orders)

                        except Exception as e:
                            raise Exception(e)

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
    """Initialize the broker and constantly listen.

    Will run until SIGTERM or an unexpected crash happens.
    """
    redis = await Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, decode_responses=True)
    pubsub: PubSub = redis.pubsub()

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, shutdown_event.set)

    worker_queues = {
        exchange: asyncio.Queue() for exchange in authenticated_clients.keys()
    }

    results_queue = asyncio.Queue()
    results_task = asyncio.create_task(results_worker(redis, results_queue))
    subscriber_task = asyncio.create_task(
        redis_subscriber(redis, pubsub, worker_queues)
    )

    for exchange in authenticated_clients.keys():
        asyncio.create_task(
            worker(
                worker_queues[exchange], results_queue, authenticated_clients[exchange]
            )
        )

    # Processing SIGTERM
    await shutdown_event.wait()

    logger.info("Shutting down after waiting for 5 seconds")
    await asyncio.sleep(5)

    await asyncio.gather(subscriber_task, results_task, return_exceptions=False)

    await redis.close()


if __name__ == "__main__":
    asyncio.run(main())
