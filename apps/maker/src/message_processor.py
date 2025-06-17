import ast
import asyncio
import json
import logging
import signal
from collections.abc import Awaitable
from copy import copy
from datetime import datetime
from decimal import Decimal
from typing import cast

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.constants import (
    BROKER_CHANNEL,
    MESSAGE_PROCESSOR_CHANNEL,
    REDIS_HOSTNAME,
    REDIS_PORT,
)
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.errors import BrokerError
from apps.maker.src.structs import (
    CancellationMessage,
    OrderBatchMessage,
    OrderMessage,
    Response,
)
from apps.maker.src.utils import (
    cancellation_from_order,
    identify_response,
    parse_message,
)

logging_config.setup_logging()
logger = logging.getLogger(__name__)


class MessageProcessor:
    def __init__(self):
        self.pool = ConnectionPool(
            host=REDIS_HOSTNAME, port=REDIS_PORT, db=0, max_connections=20
        )
        self.redis = Redis(decode_responses=True, connection_pool=self.pool)
        self.redis_pubsub = Redis(
            connection_pool=self.pool, decode_responses=True
        ).pubsub()
        self.locks = {}  # Dictionary to store locks dynamically
        self.message_queue = {}  # Dictionary to keep track of the next action to execute in case of a lock
        self.open_orders: dict[
            str, OrderMessage
        ] = {}  # Keep track of last open order (could be cleaned up by the websocket watcher?)
        self.tasks = []
        self.shutdown_event = asyncio.Event()

    async def block_and_shutdown(self, tasks_to_cancel: list[asyncio.Task]):
        logger.info("Cancelling all open tasks")
        # Shut down listener and collector
        for task in tasks_to_cancel:
            task.cancel()
        await asyncio.sleep(1)
        for task in self.tasks:
            if not task.done():
                print(task)
                task.cancel()
        logger.info("All open tasks sucessfully closed")

    async def cancel_all_open(self):
        all_cancellation: list[asyncio.Task] = []

        for order in self.open_orders.values():
            cancellation_message = cancellation_from_order(order)
            logger.info(f"Winding down, cancelling order {order}")
            all_cancellation.append(
                asyncio.create_task(self.place_cancellation(cancellation_message))
            )
        await asyncio.gather(*all_cancellation)
        logger.info("All open orders sucessfully cancelled")

    async def get_open_orders(self):
        open_orders = {}
        trigger = OrderMessage(
            kind=MessageType.ORDER,
            strategy="INIT",
            exchange="INIT",
            id="0",
            exchange_id="",
            pair="INIT",
            side=OrderSide.SELL,
            order_type=OrderType.UNIQUE,
            price=Decimal(0),
            amount=Decimal(0),
        )
        async with Redis(connection_pool=self.pool) as redis:
            await redis.publish(BROKER_CHANNEL, json.dumps(dict(trigger), default=str))

            logger.info("Asking broker for all open orders.")
            async with redis.pubsub() as pubsub:
                logger.debug("Waiting for answer on channel INIT")
                await pubsub.subscribe("INIT")
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        response = message["data"].decode()
                        logger.debug(f"Received init reply from broker: {response}")
                        order_batch = cast(OrderBatchMessage, parse_message(response))
                        for order in order_batch["orders"]:
                            # No recollection of unique orders

                            if order.get("order_type") == OrderType.UNIQUE:
                                continue
                            open_orders[order["strategy"]] = order
                        break
                await pubsub.unsubscribe("INIT")

        return open_orders

    def replace_queued_value(
        self, msg: OrderMessage | CancellationMessage | OrderBatchMessage
    ):
        strategy: str = msg["strategy"]
        prior = copy(self.message_queue[strategy])
        logger.debug(f"Replacing {prior['id']} with {msg['id']}")
        self.message_queue[strategy] = msg
        return prior["id"]

    def get_lock(self, strategy: str):
        """Get or create a lock for a strategy/signal type."""
        if strategy not in self.locks:
            self.locks[strategy] = asyncio.Lock()
            logger.debug(f"No lock found, creating one for {strategy}")
        return self.locks[strategy]

    async def send_to_broker(self, msg: OrderMessage | CancellationMessage) -> Response:
        flattened = json.dumps(dict(msg), default=str)

        # Now using context manager to ensure redis instances are dropped.

        response: str = ""

        async with Redis(connection_pool=self.pool) as redis:
            await redis.publish(BROKER_CHANNEL, flattened)

            logger.info(
                f"Sending message with id {msg['id']} and type {msg['kind']} to broker."
            )
            async with redis.pubsub() as pubsub:
                await pubsub.subscribe(msg["id"])
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        response = message["data"].decode()
                        logger.debug(
                            f"Received reply from broker for msg {msg['id']}: {response}"
                        )
                        break
                await pubsub.unsubscribe(msg["id"])

        return identify_response(response)

    async def place_order(self, msg: OrderMessage) -> OrderMessage:
        """Sends an order object to the broker and expects a confirmation."""

        logger.debug(f"Sending {msg['id']} to broker")
        confirmation: Response = await self.send_to_broker(msg)

        if confirmation.get("kind") == MessageType.ORDER:
            exchange_id = ast.literal_eval(confirmation.get("text"))["id"]
            msg["exchange_id"] = exchange_id
            logger.info(
                f"Order with id {msg['id']} was sucessfully placed. Exchange id is {exchange_id}"
            )
            return msg
        else:
            raise BrokerError(
                message=f"Invalid response from broker:{confirmation['text']}"
            )

    async def place_cancellation(self, order: CancellationMessage) -> bool:
        """Sends an order object to the broker and expects a confirmation."""

        logger.debug(f"Cancelling {order['id']} with broker")
        confirmation: Response = await self.send_to_broker(order)

        if confirmation.get("kind") == MessageType.CANCELLATION:
            logger.info(f"Order with id {order['id']} was sucessfully cancelled.")
            return True
        else:
            raise BrokerError(message=f"Invalid response from broker:{confirmation}")

    async def process_message(
        self, msg: OrderMessage | CancellationMessage | OrderBatchMessage | None
    ):
        """Process the message if the lock is available."""

        if msg is None:
            logger.error(f"Invalid parsing for message {msg}")
            return

        strategy: str = msg["strategy"]
        lock: asyncio.Lock = self.get_lock(strategy)

        # Checking for lock (order confirmation pending) first

        if lock.locked():
            logger.debug(
                f"Order already processing, queuing order: {msg['id']} for strategy {strategy}"
            )

            if strategy in self.message_queue:
                replaced_value = self.replace_queued_value(msg)

                return f"{datetime.now():%M:%S:%f} {msg['id']} replaced {replaced_value} in queue"

            else:
                self.message_queue[strategy] = msg

                return f"{datetime.now():%M:%S:%f} {msg['id']} was queued"

        async with lock:
            logger.debug(f"Processing {strategy} message: {msg['id']}")

            # This is where the order / cancellation happens.

            # If an open order exists for the relevant strategy, cancel it. If it doesn't, do nothing but note it.

            if msg["kind"] == MessageType.ORDERBATCH:
                order_batch = cast(OrderBatchMessage, msg)
                logger.debug(f"Processing order batch {msg['id']}")
                order_tasks: list[Awaitable] = []
                for order in order_batch["orders"]:
                    order_tasks.append(self.place_order(order))

                await asyncio.gather(*order_tasks)

            if msg["kind"] == MessageType.CANCELLATION:
                if strategy in self.open_orders:
                    logger.debug(
                        f"Cancelling order {self.open_orders[strategy]['id']} for strategy {strategy}"
                    )

                    cancellation = cancellation_from_order(self.open_orders[strategy])
                    if await self.place_cancellation(cancellation):
                        del self.open_orders[strategy]

                else:
                    logger.warning("Received cancellation but no open order to cancel")

            # If an open order exists, cancel it and replace it with the new one. If it doesn't, create a new one.
            elif msg["kind"] == MessageType.ORDER:
                order_msg: OrderMessage = cast(OrderMessage, msg)

                if strategy in self.open_orders:
                    logger.debug(
                        f"Cancelling order {self.open_orders[strategy]['id']} for strategy {strategy} and replacing with order {msg['id']}"
                    )

                    cancellation = cancellation_from_order(self.open_orders[strategy])
                    if await self.place_cancellation(cancellation):
                        del self.open_orders[strategy]

                else:
                    logger.debug(f"Placing order {msg['id']}")

                confirmed_order = await self.place_order(order_msg)
                if confirmed_order["order_type"] == OrderType.REPLACE:
                    self.open_orders[strategy] = confirmed_order

            logger.debug(f"Finished processing {strategy} message: {msg['id']}")

        # This recursively starts processing for any message pending from the queue

        if strategy in self.message_queue:
            logger.debug(
                f"{self.message_queue[strategy]['id']} in the queue, processing."
            )
            task = asyncio.create_task(
                self.process_message(self.message_queue[strategy])
            )
            self.tasks.append(task)
            del self.message_queue[strategy]

        return f"{datetime.now():%M:%S:%f} {msg['id']} was processed"

    async def listen_to_redis(self):
        """Subscribe to Redis and process messages."""

        pubsub = self.redis.pubsub()
        await pubsub.subscribe(
            MESSAGE_PROCESSOR_CHANNEL
        )  # Subscribe to a Redis channel

        async for message in pubsub.listen():
            if message["type"] == "message":
                data = parse_message(message["data"])

                logger.info(f"Received {data} via redis")
                task = asyncio.create_task(self.process_message(data))
                self.tasks.append(task)

    async def collect_results_periodically(self):
        """Periodically collect finished tasks and remove them from the list."""
        while True:
            if self.tasks:
                # Gather only completed tasks
                done, pending = await asyncio.wait(
                    self.tasks, timeout=1, return_when=asyncio.FIRST_COMPLETED
                )
                self.tasks = list(pending)
                # Collect results
                results: list[str] = [
                    task.result() for task in done if task.result() is not None
                ]
                if results:
                    # logger.info(reversed(results))
                    for result in reversed(results):
                        logger.debug(f"BATCH: {result}")
                    # print(f"Collected results: {sorted(results)}")

            await asyncio.sleep(2)  # Check every 20 second


async def main():
    logger.debug("Message processor starting")
    processor = MessageProcessor()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, processor.shutdown_event.set)

    # Let broker boot and check for open orders

    await asyncio.sleep(2)
    processor.open_orders = await processor.get_open_orders()
    if processor.open_orders:
        logger.info(f"Pre-existing open order were found: {processor.open_orders}")

    # Start message listener. Tasks are used so that no result is immediately expected.
    listener_task = asyncio.create_task(processor.listen_to_redis())

    # Start periodic collection of results
    collector_task = asyncio.create_task(processor.collect_results_periodically())

    await processor.shutdown_event.wait()
    await processor.cancel_all_open()
    await processor.block_and_shutdown([listener_task, collector_task])

    await asyncio.gather(listener_task, collector_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
