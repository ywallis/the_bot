"""Simulator script 3 for strategy order generation."""

from decimal import Decimal
from redis import Redis
import json
from datetime import datetime

from apps.maker.src.constants import (
    MESSAGE_PROCESSOR_CHANNEL,
    REDIS_HOSTNAME,
    REDIS_PORT,
)
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.structs import OrderMessage

r: Redis = Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, decode_responses=True)

channel: str = MESSAGE_PROCESSOR_CHANNEL

order_1 = OrderMessage(
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

r.publish(channel, json.dumps(dict(order_1), default=str))
