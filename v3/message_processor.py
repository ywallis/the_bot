import logging
import json
from pythonjsonlogger.json import JsonFormatter
import asyncio
from typing import cast, Awaitable
from copy import copy
from enums import MessageType
from structs import OrderMessage, CancellationMessage
from redis.asyncio import Redis, ConnectionPool
from datetime import datetime
from utils import parse_message

# This is the draft for my trading system message processor
# TODO:
# - Logging
# - Testing
# - CCXT broker response
# - Cancel / Cancel and replace in order processing dependant on open order

# NOTES:
# The strategy knows when to void it's own signals.
# Returns from tasks will probably just be used for logs.

logger = logging.getLogger()

logHandler = logging.StreamHandler()
formatter = JsonFormatter("{filename}{asctime}{message}{exc_info}", style="{")
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)


class MessageProcessor:

    def __init__(self):

        self.pool = ConnectionPool(host="localhost", port=6379, db=0, max_connections=10)
        self.redis = Redis(host="localhost", port=6379, decode_responses=True, connection_pool=self.pool)
        self.redis_pubsub = Redis(connection_pool=self.pool, decode_responses=True).pubsub()
        self.locks = {}  # Dictionary to store locks dynamically
        self.message_queue = (
            {}
        )  # Dictionary to keep track of the next action to execute in case of a lock
        self.open_orders = (
            {}
        )  # Keep track of last open order (could be cleaned up by the websocket watcher?)
        self.tasks = []

    def replace_queued_value(self, msg: OrderMessage | CancellationMessage):

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

    async def send_to_broker(self, msg: OrderMessage | CancellationMessage):
        flattened = json.dumps(dict(msg), default=str)
        _: Awaitable[int] = await self.redis.rpush("broker", flattened) # type: ignore

        print('seeeend')
        
        return_redis_instance = Redis(host="localhost", port=6379, decode_responses=True, connection_pool=self.pool)
        return_redis_pubsub = return_redis_instance.pubsub()
        await return_redis_pubsub.subscribe(msg['id'])

        print(f"Waiting for message on channel {msg['id']}")
        async for message in return_redis_pubsub.listen():
            if message["type"] == "message":
                print("Received:", message)
                break  # Exit loop once a message is received        # message = await return_redis_pubsub.get_message(ignore_subscribe_messages=True, timeout=None)

        await return_redis_pubsub.unsubscribe(msg["id"])
        await return_redis_instance.close()
        print(f"Unsubscribed from {msg['id']}")
        print("End of send to broker")


    async def place_order(self, msg: OrderMessage) -> bool:
        """Sends an order object to the broker and expects a confirmation."""

        logger.debug(f"Sending {msg['id']} to broker")
        # TODO: Replace with broker connector
        await self.send_to_broker(msg)
        await asyncio.sleep(3)
        order_confirmed = True
        print("This should not print for now")

        return order_confirmed

    # The cancellation actually only requires the "open order" OrderMessage
    async def place_cancellation(self, order: OrderMessage) -> bool:
        """Sends an order object to the broker and expects a confirmation."""

        logger.debug(f"Cancelling {order['id']} with broker")
        # TODO: Replace with broker connector
        await asyncio.sleep(3)
        cancellation_confirmed = True

        return cancellation_confirmed

    async def process_message(self, msg: OrderMessage | CancellationMessage | None):
        """Process the message if the lock is available."""

        if msg is None:
            logger.error("Invalid parsing")
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

                return (
                    f"{datetime.now():%M:%S:%f} {msg['id']} was queued"
                )

        async with lock:
            logger.debug(f"Processing {strategy} message: {msg['id']}")

            # This is where the order / cancellation happens.

            # If an open order exists for the relevant strategy, cancel it. If it doesn't, do nothing but note it.

            if msg["kind"] == MessageType.CANCELLATION:

                if strategy in self.open_orders:
                    logger.debug(
                        f"Cancelling order {self.open_orders[strategy]['id']} for strategy {strategy}"
                    )

                    if await self.place_cancellation(self.open_orders[strategy]):
                        del(self.open_orders[strategy])

                else:
                    logger.warning(f"Received cancellation but no open order to cancel")

            # If an open order exists, cancel it and replace it with the new one. If it doesn't, create a new one.
            elif msg["kind"] == MessageType.ORDER:
                order_msg: OrderMessage = cast(OrderMessage, msg)

                if strategy in self.open_orders:
                    logger.debug(f"Cancelling order {self.open_orders[strategy]['id']} for strategy {strategy} and replacing with order {msg['id']}")

                    if await self.place_cancellation(self.open_orders[strategy]):
                        del(self.open_orders[strategy])

                    if await self.place_order(order_msg):
                        self.open_orders[strategy] = order_msg
                else:
                    logger.debug(f"Placing order {msg['id']}")

                    if await self.place_order(order_msg):
                        self.open_orders[strategy] = order_msg

            logger.debug(f"Finished processing {strategy} message: {msg['id']}")

        # This recursively starts processing for any message pending from the queue

        if strategy in self.message_queue:
            logger.debug(f"{self.message_queue[strategy]['id']} in the queue, processing.")
            task = asyncio.create_task(
                self.process_message(self.message_queue[strategy])
            )
            self.tasks.append(task)
            del self.message_queue[strategy]

        return f"{datetime.now():%M:%S:%f} {msg['id']} was processed"

    async def listen_to_redis(self):
        """Subscribe to Redis and process messages."""

        pubsub = self.redis.pubsub()
        await pubsub.subscribe("testing_ps")  # Subscribe to a Redis channel

        async for message in pubsub.listen():
            if message["type"] == "message":
                data = parse_message(message["data"])

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
                results: list[str] = [task.result() for task in done if task.result() is not None]
                if results:
                    # logger.info(reversed(results))
                    for result in reversed(results):
                        logger.info(f"BATCH: {result}")
                    # print(f"Collected results: {sorted(results)}")

            await asyncio.sleep(5)  # Check every 20 second


async def main():
    logger.debug("Launching main loop")
    processor = MessageProcessor()

    # Start message listener. Tasks are used so that no result is immediately expected.
    listener_task = asyncio.create_task(processor.listen_to_redis())

    # Start periodic collection of results
    collector_task = asyncio.create_task(processor.collect_results_periodically())

    await asyncio.gather(listener_task, collector_task)


asyncio.run(main())
