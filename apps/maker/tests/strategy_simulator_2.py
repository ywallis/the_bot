from redis import Redis
import json
import time
from datetime import datetime

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL, REDIS_HOSTNAME, REDIS_PORT

r: Redis = Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, decode_responses=True)

channel: str = MESSAGE_PROCESSOR_CHANNEL 

order_no: int = 0

order_1: dict[str, str] = {
    "kind": "order",
    "strategy": "ALPH_gate",
    "exchange": "gate",
    "id": f"gate_{datetime.now():%M:%S:%f}",
    "exchange_id": "_",
    "pair": "ALPH/USDT",
    "side": "sell",
    "order_type": "replace",
    "price": "100",
    "amount": "10",
}

order_2: dict[str, str] = {
    "kind": "order",
    "strategy": "ALPH_gate",
    "exchange": "gate",
    "id": f"gate_{datetime.now():%M:%S:%f}",
    "exchange_id": "_",
    "pair": "ALPH/USDT",
    "side": "sell",
    "order_type": "replace",
    "price": "100",
    "amount": "10",
}

cancellation_1: dict[str, str] = {
    "kind": "cancellation",
    "strategy": "ALPH_gate",
    "exchange": "gate",
    "id": order_1["id"],
    "pair": "ALPH/USDT",
}

r.publish(channel, json.dumps(order_1))
r.publish(channel, json.dumps(order_2))
time.sleep(5)
r.publish(channel, json.dumps(cancellation_1))
