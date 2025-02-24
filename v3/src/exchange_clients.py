import os
from dotenv import load_dotenv
import tomllib
import ccxt.async_support as ccxt

load_dotenv("../.env")
# Load TOML file
with open("../config/config.toml", "rb") as f:
    config = tomllib.load(f)

# Extract exchange items
exchanges = config.get("exchanges", [])

authenticated_clients = {}

print(exchanges)

for exchange in exchanges:
    id = exchange["id"]
    print(id)
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
    authenticated_clients[id] = auth_client

print(authenticated_clients)
