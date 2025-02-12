import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

from ccxt.base.exchange import Exchange # pyright: ignore[reportMissingTypeStubs] 
from ws.ws_clients import all_clients_dict
import time
import redis
from typing import TypedDict
from enum import Enum

# import asyncio


class MessageType(Enum):

    order = "order"
    cancellation = "cancellation"


class Cancellation(TypedDict):
    kind: MessageType
    client: str
    id: str


class Order(TypedDict):
    kind: MessageType
    client: str
    id: str
    price: float
    amount: float
    side: str


r = redis.Redis(host="localhost", port=6379, decode_responses=True)

queue_name = "broker"


def take_action(job: str):
    print(job)


def process_message(order: Cancellation | Order, clients: dict[str, Exchange]):
    # This is where messages will be processed. I currently imagine two types of messages: place order and cancel order. Obviously, more data is needed to cancel an order than to create oe, but I think that can be ignore in a first step. This will be a good place to practice both serialisation and enum/match in python and rust.

    pass


def return_value(queue: str, timeout: int) -> str | None:

    redis_results: tuple[str, str] | None = r.blpop([queue], timeout=timeout)  # pyright: ignore [reportAssignmentType, reportUnknownMemberType]

    print("Result:", redis_results)

    if redis_results:
        _key, message = redis_results
        # print(f"Message is {message}")
        return message
    else:
        # print("Queue is empty")
        return None


if __name__ == "__main__":
    print("Hi from main")
    print("Dict", all_clients_dict)
    # for i in range(4):
    #     start = time.perf_counter()
    #     print(return_value(queue_name, 0))
    #     end = time.perf_counter()
    #     print(f'Total time is {end - start}.')
