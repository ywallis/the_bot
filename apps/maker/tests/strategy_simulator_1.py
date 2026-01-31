"""Simulator script 1 for strategy order generation."""

from decimal import Decimal
import time
from redis import Redis
import json
from datetime import datetime

from apps.maker.src.constants import (
    MESSAGE_PROCESSOR_CHANNEL,
    REDIS_HOSTNAME,
    REDIS_PORT,
)
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.structs import CancellationMessage, OrderMessage

r: Redis = Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, decode_responses=True)

channel: str = MESSAGE_PROCESSOR_CHANNEL

order_no: int = 0

order_1: dict[str, str] = {
    "kind": "order",
    "strategy": "TEST",
    "exchange": "exchange_a",
    "id": f"exchange_b_{datetime.now():%M:%S:%f}",
    "exchange_id": "_",
    "pair": "TEST/USDT",
    "side": "sell",
    "order_type": "replace",
    "price": "100",
    "amount": "10",
}

cancellation_1: dict[str, str] = {
    "kind": "cancellation",
    "strategy": "TEST",
    "exchange": "exchange_a",
    "id": order_1["id"],
    "pair": "TEST/USDT",
}
order_2: dict[str, str] = {
    "kind": "order",
    "strategy": "TEST2",
    "exchange": "exchange_a",
    "id": f"exchange_b_{datetime.now():%M:%S:%f}",
    "exchange_id": "_",
    "pair": "TEST/USDT",
    "side": "buy",
    "order_type": "replace",
    "price": "0.01",
    "amount": "100",
}

cancellation_2: dict[str, str] = {
    "kind": "cancellation",
    "strategy": "TEST2",
    "exchange": "exchange_a",
    "id": order_2["id"],
    "pair": "TEST/USDT",
}

order_3 = OrderMessage(
    kind=MessageType.ORDER,
    strategy="TEST_es",
    exchange="exchange_a",
    id=f"t-{datetime.now()::%M:%S:%f}_TEST_es",
    exchange_id="_",
    pair="TEST/USDT",
    side=OrderSide.SELL,
    order_type=OrderType.REPLACE,
    price=Decimal(100),
    amount=Decimal(10),
)

cancellation_3 = CancellationMessage(
    kind=MessageType.CANCELLATION,
    strategy="TEST_eb",
    exchange="exchange_a",
    id=order_3["id"],
    pair="TEST/USDT",
)

order_4 = OrderMessage(
    kind=MessageType.ORDER,
    strategy="TEST_es",
    exchange="exchange_a",
    id=f"t-{datetime.now()::%M:%S:%f}_TEST_es",
    exchange_id="_",
    pair="TEST/USDT",
    side=OrderSide.SELL,
    order_type=OrderType.REPLACE,
    price=Decimal(100.00001),
    amount=Decimal(10.00001),
)

cancellation_4 = CancellationMessage(
    kind=MessageType.CANCELLATION,
    strategy="TEST_es",
    exchange="exchange_a",
    id=order_3["id"],
    pair="TEST/USDT",
)

r.publish(channel, json.dumps(dict(order_1), default=str))
r.publish(channel, json.dumps(dict(cancellation_1), default=str))
time.sleep(3)
r.publish(channel, json.dumps(dict(order_2), default=str))
r.publish(channel, json.dumps(dict(cancellation_2), default=str))
time.sleep(3)
r.publish(channel, json.dumps(dict(order_3), default=str))
r.publish(channel, json.dumps(dict(cancellation_3), default=str))
time.sleep(3)
r.publish(channel, json.dumps(dict(order_4), default=str))
r.publish(channel, json.dumps(dict(cancellation_4), default=str))
