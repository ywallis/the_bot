"""
This module initializes and manages authenticated exchange clients using CCXT.
It loads configuration and credentials, and provides a collection of authenticated clients.
"""

import logging
import os

import ccxt.pro as ccxt  # pyright: ignore[reportMissingTypeStubs]
from dotenv import load_dotenv

import apps.shared.src.logging_config as logging_config
from apps.shared.src.errors import RequestTimeout
from apps.shared.src.structs import CustomExchange
from apps.shared.src.utils import load_config

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def load_clients():
    """
    Load and verify authenticated exchange clients.

    Iterates through the authenticated clients and attempts to load their markets.
    Retries up to 5 times in case of a RequestTimeout.

    Raises
    ------
    RequestTimeout
        If a client fails to load markets after 5 attempts.
    """
    for client in authenticated_clients.values():
        attempt: int = 1
        while attempt <= 5:
            try:
                await client.load_markets()
                logger.info(f"Client {client.name} loaded successfully.")
                break
            except RequestTimeout as e:
                logger.error(
                    f"Client {client.name} has timed out on attempt n. {attempt}, retrying. {e}"
                )
                attempt += 1


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
        auth_client.timeout = 30000
        # auth_client.verbose = True
    authenticated_clients[id] = auth_client
