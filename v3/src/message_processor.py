import logging
import json
import asyncio
import logging
import logging_config
from typing import cast
from copy import copy
from enums import MessageType
from structs import OrderMessage, CancellationMessage, Response
from redis.asyncio import Redis, ConnectionPool
from datetime import datetime
from utils import identify_response, parse_message, cancellation_from_order
from v3.src.errors import BrokerError

# This is the draft for my trading system message processor
# TODO:
# - Testing
# - Define CCXT broker response

# NOTES:
# The strategy knows when to void it's own signals.
# Returns from tasks will probably just be used for logs.

logging_config.setup_logging()
logger = logging.getLogger(__name__)


class MessageProcessor:

    def __init__(self):

        self.pool = ConnectionPool(
            host="localhost", port=6379, db=0, max_connections=20
        )
        self.redis = Redis(decode_responses=True, connection_pool=self.pool)
        self.redis_pubsub = Redis(
            connection_pool=self.pool, decode_responses=True
        ).pubsub()
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

    async def send_to_broker(self, msg: OrderMessage | CancellationMessage) -> Response:
        flattened = json.dumps(dict(msg), default=str)

        # Now using context manager to ensure redis instances are dropped.

        response: str = ""

        async with Redis(connection_pool=self.pool) as redis:
            await redis.publish("broker", flattened)

            logger.info(
                f"Sending message with id {msg['id']} and type {msg['kind']} to broker."
            )
            async with redis.pubsub() as pubsub:
                await pubsub.subscribe(msg["id"])
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        response = message["data"].decode()
                        logger.info(
                            f"Received reply from broker for msg {msg['id']}: {response}"
                        )
                        print("PAYLOAD", response)
                        break
                await pubsub.unsubscribe(msg["id"])

        # This is where parsing and returning the reponse will happen. So far this always returns true.
        # This should be simple: Confirmation, retry mechanism in case of errors, and informing/stopping the strat in case something goes out of bounds.

        return identify_response(response)

    async def place_order(self, msg: OrderMessage) -> OrderMessage:
        """Sends an order object to the broker and expects a confirmation."""

        logger.debug(f"Sending {msg['id']} to broker")
        # TODO: Replace with broker connector
        confirmation: Response = await self.send_to_broker(msg)

        if confirmation.get("kind") == MessageType.ORDER:
            exchange_id = json.loads(confirmation["text"])["id"]
            msg["exchange_id"] = exchange_id
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
            return True
        else:
            raise BrokerError(message=f"Invalid response from broker:{confirmation}")

    async def process_message(self, msg: OrderMessage | CancellationMessage | None):
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

            if msg["kind"] == MessageType.CANCELLATION:

                if strategy in self.open_orders:
                    logger.debug(
                        f"Cancelling order {self.open_orders[strategy]['id']} for strategy {strategy}"
                    )

                    cancellation = cancellation_from_order(self.open_orders[strategy])
                    if await self.place_cancellation(cancellation):
                        del self.open_orders[strategy]

                else:
                    logger.warning(f"Received cancellation but no open order to cancel")

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

                    confirmed_order: OrderMessage = await self.place_order(order_msg)
                    self.open_orders[strategy] = confirmed_order

                else:
                    logger.debug(f"Placing order {msg['id']}")

                    confirmed_order = await self.place_order(order_msg)
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
                results: list[str] = [
                    task.result() for task in done if task.result() is not None
                ]
                if results:
                    # logger.info(reversed(results))
                    for result in reversed(results):
                        logger.info(f"BATCH: {result}")
                    # print(f"Collected results: {sorted(results)}")

            await asyncio.sleep(2)  # Check every 20 second


async def main():
    logger.debug("Launching main loop")
    processor = MessageProcessor()

    # Start message listener. Tasks are used so that no result is immediately expected.
    listener_task = asyncio.create_task(processor.listen_to_redis())

    # Start periodic collection of results
    collector_task = asyncio.create_task(processor.collect_results_periodically())

    await asyncio.gather(listener_task, collector_task)


asyncio.run(main())
