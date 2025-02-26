import os
from dotenv import load_dotenv
from apps.maker.src.utils import load_config
import ccxt.async_support as ccxt

load_dotenv()
# Load TOML file

config = load_config()
# Extract exchange items
exchanges = config.get("exchanges", [])

authenticated_clients = {}

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
    authenticated_clients[id] = auth_client

