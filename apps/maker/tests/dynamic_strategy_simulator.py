"""Simulator for dynamic strategy order generation."""

from redis import Redis
from random import randint
import time
import json
from datetime import datetime

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL, REDIS_HOSTNAME, REDIS_PORT

def fake_order(strategy:str, exchange: str) -> dict[str, str]:
    """Generate a fake order dictionary."""
    order: dict[str, str] = {
        "kind": "order",
        "strategy": strategy,
        "exchange": exchange,
        "id": f"{exchange}{datetime.now():%M:%S:%f}",
        "pair": "TEST/USDT",
        "side": "sell",
        "price": "3",
        "amount": "10",
    }
    return order
    

def loop(order:int = 0):
    """Infinite loop publishing orders to Redis."""
    while True:

        # result = r.publish(channel, f'Hi! It\'s {datetime.now()}')
        result = r.publish(channel, f'ORDER:Number {order}')
        print(result)
        time.sleep(randint(0,4))
        order += 1

r: Redis = Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, decode_responses=True)

channel: str = MESSAGE_PROCESSOR_CHANNEL 

order_no: int = 0

# time.sleep(1)
# r.publish(channel, 'ORDER:Number 1')
# r.publish(channel, 'ORDER:Number 2')
# r.publish(channel, 'ORDER:Number 3')
# time.sleep(3)
# r.publish(channel, 'ORDER:Number 4')

order1: dict[str, str] = {
    "kind": "order",
    "strategy": "TESTm",
    "exchange": "mexc",
    "id": "abc1",
    "side": "sell",
    "price": "3",
    "amount": "10",
}
order2: dict[str, str] = {
    "kind": "order",
    "strategy": "TESTb",
    "exchange": "exchange_a",
    "id": "abc2",
    "side": "sell",
    "price": "3",
    "amount": "10",
}
order3: dict[str, str] = {
    "kind": "order",
    "strategy": "ALPm",
    "exchange": "mexc",
    "id": "abc3",
    "side": "sell",
    "price": "3",
    "amount": "10",
}
order4: dict[str, str] = {
    "kind": "order",
    "strategy": "TEST",
    "exchange": "mexc",
    "id": "abc4",
    "side": "sell",
    "price": "3",
    "amount": "10",
}
while True:
    r.publish(channel, json.dumps(fake_order("a", "exchange_a")))
    r.publish(channel, json.dumps(fake_order("b", "mexc")))

    r.publish(channel, json.dumps(fake_order("c", "exchange_b")))
    r.publish(channel, json.dumps(fake_order("d", "exchange_a")))
    r.publish(channel, json.dumps(fake_order("e", "mexc")))

    r.publish(channel, json.dumps(fake_order("f", "exchange_b")))
    # break
    time.sleep(0.03)
    # r.publish(channel, json.dumps(order1))
    # r.publish(channel, json.dumps(order2))
    # r.publish(channel, json.dumps(order3))
    # time.sleep(4)
    # r.publish(channel, json.dumps(order4))
