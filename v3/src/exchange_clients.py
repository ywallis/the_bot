import os
import tomllib
import ccxt.async_support as ccxt

# Load TOML file
with open("config.toml", "rb") as f:
    config = tomllib.load(f)

# Extract object names
exchanges = config.get("exchanges", {})


authenticated_clients = {}


# This does not account for clients that need a password in addition to key/secret
for key, name in exchanges.items():
    print(key, name)
    env1 = os.getenv(f"{name.upper()}_KEY", "default_env1")
    env2 = os.getenv(f"{name.upper()}_SECRET", "default_env2")
    authenticated_clients[key] = getattr(ccxt, name)({"apiKey": env1, "secret": env2})
