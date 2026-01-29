"""Simulator script 4 for strategy order generation."""

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

r: Redis = Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, decode_responses=True)

channel: str = MESSAGE_PROCESSOR_CHANNEL

order_1 = {
    "kind": MessageType.ORDERBATCH,
    "strategy": "test_strategy_tt",
    "id": f"t-{datetime.now().strftime("%y%m%d%H%M%S%f")}_ALPH_tt",
    "orders": [
        {
            "kind": MessageType.ORDER,
            "strategy": "test_strategy_tt",
            "exchange": "gate",
            "id": f"t-{datetime.now().strftime("%y%m%d%H%M%S%f")}_ALPH_tt",
            # "id": "t-1234",
            "exchange_id": "_",
            "pair": "ALPH/USDT",
            "side": OrderSide.BUY,
            "order_type": OrderType.UNIQUE,
            "price": Decimal(0.01),
            "amount": Decimal(1000),
        },
        {
            "kind": MessageType.ORDER,
            "strategy": "test_strategy_tt",
            "exchange": "gate",
            "id": f"t-{datetime.now().strftime("%y%m%d%H%M%S%f")}_ALPH_tt",
            # "id": "t-2345",
            "exchange_id": "_",
            "pair": "ALPH/USDT",
            "side": OrderSide.SELL,
            "order_type": OrderType.UNIQUE,
            "price": Decimal(59900),
            "amount": Decimal(10),
        },
    ],
}


r.publish(channel, json.dumps(dict(order_1), default=str))
