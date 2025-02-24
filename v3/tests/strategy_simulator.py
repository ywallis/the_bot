from redis import Redis
import json
from datetime import datetime

r: Redis = Redis(host="localhost", port=6379, decode_responses=True)

channel: str = "testing_ps"

order_no: int = 0

order_1: dict[str, str] = {
    "kind": "order",
    "strategy": "ALPH_gate",
    "exchange": "gate",
    "id": f"gate_{datetime.now():%M:%S:%f}",
    "exchange_id": "_",
    "pair": "ALPH/USDT",
    "side": "sell",
    "price": "100",
    "amount": "10",
}

cancellation_1: dict[str, str] = {
    "kind": "cancellation",
    "strategy": "ALPH_gate",
    "exchange": "gate",
    "id": order_1['id'],
    "pair": "ALPH/USDT",
}

r.publish(channel, json.dumps(order_1))
r.publish(channel, json.dumps(cancellation_1))
