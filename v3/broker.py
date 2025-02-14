from enums import OrderSide, MessageType
from structs import CancellationMessage, OrderMessage
from utils import parse_message
from ccxt.base.exchange import Exchange # pyright: ignore[reportMissingTypeStubs] 
# from ws.ws_clients import all_clients_dict
import tomllib
import asyncio
from redis.asyncio import Redis, ConnectionPool

redis_in = Redis(host="localhost", port=6379, decode_responses=True)
redis_out = Redis(host="localhost", port=6379, decode_responses=True)

CONFIG_FILE = "config.toml"
queue_name = "broker"

def load_worker_settings():
    """Load worker settings from a TOML file."""
    with open(CONFIG_FILE, "rb") as f:
        config = tomllib.load(f)
    return config.get("exchanges", {})

def take_action(job: str):
    print(job)


def process_message(order: CancellationMessage | OrderMessage, clients: dict[str, Exchange]):
    # This is where messages will be processed. I currently imagine two types of messages: place order and cancel order. Obviously, more data is needed to cancel an order than to create oe, but I think that can be ignore in a first step. This will be a good place to practice both serialisation and enum/match in python and rust.

    pass


async def pull_from_queue(queue: str, timeout: int) -> str | None:

    redis_results: tuple[str, str] | None = await redis_in.blpop([queue], timeout=timeout)  # pyright: ignore [reportAssignmentType, reportUnknownMemberType]

    print("Result:", redis_results)

    if redis_results:
        _key, message = redis_results
        # print(f"Message is {message}")
        return message
    else:
        print("Queue is empty")
        return None

async def main():

    while True:
        data = await pull_from_queue(queue_name, 0)


if __name__ == "__main__":
    print("Hi from main")
    print(load_worker_settings())
    asyncio.run(main())
