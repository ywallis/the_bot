"""Module for managing exchange client connections."""

import logging
import os

import ccxt.pro as ccxt  # pyright: ignore[reportMissingTypeStubs]
from dotenv import load_dotenv

import apps.shared.src.logging_config as logging_config
from apps.shared.src.errors import RequestTimeout
from apps.shared.src.config import VenueConfig, load_app_config
from apps.shared.src.structs import CustomExchange

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


def ccxt_params(
    venue: VenueConfig,
    key: str | None,
    secret: str | None,
    password: str | None,
    requires_password: bool,
) -> dict[str, object]:
    """
    Build the constructor arguments for a CCXT client.

    Parameters
    ----------
    venue : VenueConfig
        The venue, whose ``options`` are forwarded to CCXT.
    key : str | None
        API key.
    secret : str | None
        API secret.
    password : str | None
        API passphrase, used only if the venue requires one.
    requires_password : bool
        Whether the venue's ``requiredCredentials`` lists a password.

    Returns
    -------
    dict[str, object]
        Keyword arguments for the CCXT exchange class.
    """
    params: dict[str, object] = {"apiKey": key, "secret": secret}
    if requires_password:
        params["password"] = password
    if venue.options:
        params["options"] = dict(venue.options)
    return params


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
    requires_password = bool(client.requiredCredentials["password"])

    auth_client = getattr(ccxt, id)(
        ccxt_params(
            venue, exchange_key, exchange_secret, exchange_password, requires_password
        )
    )
    if not requires_password:
        auth_client.options["maxRetriesOnFailure"] = 1
        auth_client.timeout = 30000
    authenticated_clients[id] = auth_client
