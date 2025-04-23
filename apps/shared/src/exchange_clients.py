import os

import ccxt.pro as ccxt
from dotenv import load_dotenv

from apps.shared.src.structs import CustomExchange
from apps.shared.src.utils import load_config

load_dotenv()
# Load TOML file

config = load_config()
# Extract exchange items
exchanges = config.get("exchanges", [])
strategies = config.get("strategies", [])
symbols = set([strategy["symbol"] for strategy in strategies])

authenticated_clients: dict[str, CustomExchange] = {}

for exchange in exchanges:
    id = exchange["id"]
    exchange_key = os.getenv(f"{id.upper()}_KEY")
    exchange_secret = os.getenv(f"{id.upper()}_SECRET")
    exchange_password = os.getenv(f"{id.upper()}_PASSWORD")

    client = getattr(ccxt, id)()

    if client.requiredCredentials["password"]:
        auth_client = getattr(ccxt, id)(
            {
                "apiKey": exchange_key,
                "secret": exchange_secret,
                "password": exchange_password,
            }
        )
    else:
        auth_client = getattr(ccxt, id)(
            {"apiKey": exchange_key, "secret": exchange_secret}
        )
        auth_client.options["maxRetriesOnFailure"] = 1
    authenticated_clients[id] = auth_client
