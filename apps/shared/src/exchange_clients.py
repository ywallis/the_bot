"""Module for managing exchange client connections."""

import logging
import os

import ccxt.pro as ccxt  # pyright: ignore[reportMissingTypeStubs]
from dotenv import load_dotenv

import apps.shared.src.logging_config as logging_config
from apps.shared.src.errors import RequestTimeout
from apps.shared.src.structs import CustomExchange
from apps.shared.src.config import load_app_config

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def load_clients():
    """
    Load markets for all authenticated clients with retry logic.

    Attempts to load markets for each client up to 5 times.
    Logs success or failure.
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
# Load typed config. Symbols cover every strategy, production or not, so the
# broker can recollect open orders regardless of the mode it runs in.

config = load_app_config()
symbols: set[str] = config.symbols()

authenticated_clients: dict[str, CustomExchange] = {}

for venue in config.venues:
    id = venue.id
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
