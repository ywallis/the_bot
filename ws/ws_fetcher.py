import sys

# Adding directory to PATH
sys.path.append(".")
sys.path.append("..")

import redis
import json
from datetime import datetime
import time
from ws_clients import all_clients, pair


def retrieve_ob_redis(key):
    # Get the JSON string from Redis
    serialized_ob = r.get(key)
    if serialized_ob is None:
        return None  # Key not found
    # Deserialize the JSON string back to a CCXT ob
    return json.loads(serialized_ob)

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

while True:
    print(datetime.now())

    for client in all_clients:
        ob = retrieve_ob_redis(f'{pair}-{client.name}')
        print(f'{datetime.now()} Bid {ob["bids"][0][0]} and ask {ob["asks"][0][0]} at {ob['datetime']} on {client.name}.')

    time.sleep(1)
