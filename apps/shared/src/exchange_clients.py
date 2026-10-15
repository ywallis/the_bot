"""Module for managing exchange client connections.

One authenticated CCXT client per declared venue, keyed by venue id. A venue
is an account rather than an exchange: the same exchange appears twice when
its spot and futures wallets are separate, once per wallet, and the two
entries differ in ``market_type`` and share a CCXT class through
``ccxt_id``. See ``docs/design/inventory-hedging.md`` section 3.
"""

import asyncio
import logging
import os

import ccxt.pro as ccxt  # pyright: ignore[reportMissingTypeStubs]
from dotenv import load_dotenv

import apps.shared.src.logging_config as logging_config
from apps.shared.src.client_limits import (
    CLIENT_RETRIES_ON_FAILURE,
    CLIENT_TIMEOUT_MS,
    RATE_LIMITER,
    RATE_WINDOW_MS,
)
from apps.shared.src.config import VenueConfig, load_app_config
from apps.shared.src.errors import NetworkError
from apps.shared.src.structs import CustomExchange

# Initializing centralized logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


# Attempts at loading a client's markets, and the pause before the n-th
# retry in seconds is n times this.
LOAD_ATTEMPTS = 5
LOAD_BACKOFF_S = 2.0


async def load_clients():
    """
    Load markets for all authenticated clients with retry logic.

    A timeout or any other network error, a rate limit refusal included, is
    retried with a growing pause: every process loads markets at start, and
    two venue entries sharing one key make that burst hit the limit.

    Raises
    ------
    NetworkError
        If a client still fails after ``LOAD_ATTEMPTS`` attempts.
    """
    for client in authenticated_clients.values():
        for attempt in range(1, LOAD_ATTEMPTS + 1):
            try:
                await client.load_markets()
                logger.info(f"Client {client.name} loaded successfully.")
                break
            except NetworkError as e:
                logger.error(
                    f"Client {client.name} failed to load markets on attempt "
                    f"n. {attempt}: {e}"
                )
                if attempt == LOAD_ATTEMPTS:
                    raise
                await asyncio.sleep(LOAD_BACKOFF_S * attempt)


def ccxt_params(
    venue: VenueConfig,
    key: str | None,
    secret: str | None,
    password: str | None,
    requires_password: bool,
) -> dict[str, object]:
    """
    Build the constructor arguments for a CCXT client.

    Every client throttles with a rolling window rather than CCXT's default
    leaky bucket; see ``client_limits.RATE_LIMITER``.

    Parameters
    ----------
    venue : VenueConfig
        The venue, whose ``options`` are forwarded to CCXT. A ``swap``
        venue also sets ``defaultType`` so that every call on the client
        addresses the futures account; an explicit ``defaultType`` in the
        venue's options wins over that.
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
    params: dict[str, object] = {
        "apiKey": key,
        "secret": secret,
        "rateLimiterAlgorithm": RATE_LIMITER,
        "rollingWindowSize": RATE_WINDOW_MS,
    }
    if requires_password:
        params["password"] = password
    options = venue.client_options
    if options:
        params["options"] = options
    return params


def build_client(venue: VenueConfig) -> CustomExchange:
    """
    Construct the authenticated CCXT client of one venue.

    Credentials are read from ``{PREFIX}_KEY``, ``{PREFIX}_SECRET`` and
    ``{PREFIX}_PASSWORD`` in the environment, the prefix being the venue's
    ``env_prefix``. The client is the CCXT class named by the venue's
    ``exchange``, so two venue entries may share one exchange.

    Parameters
    ----------
    venue : VenueConfig
        The venue.

    Returns
    -------
    CustomExchange
        The client, with ``id`` overridden to the venue id so that every
        feed handler keys its streams on the venue and not the exchange.
    """
    prefix = venue.env_prefix
    exchange_key = os.getenv(f"{prefix}_KEY")
    exchange_secret = os.getenv(f"{prefix}_SECRET")
    exchange_password = os.getenv(f"{prefix}_PASSWORD")

    exchange_class = getattr(ccxt, venue.exchange)
    requires_password = bool(exchange_class().requiredCredentials["password"])

    auth_client = exchange_class(
        ccxt_params(
            venue, exchange_key, exchange_secret, exchange_password, requires_password
        )
    )
    if not requires_password:
        auth_client.options["maxRetriesOnFailure"] = CLIENT_RETRIES_ON_FAILURE
        auth_client.timeout = CLIENT_TIMEOUT_MS
    # The feed handlers publish on ``client.id``; a second entry for one
    # exchange must publish under its own venue id or the two wallets'
    # balances and orders land on one stream.
    auth_client.id = venue.id
    return auth_client


load_dotenv()
# Load typed config. Symbols cover every strategy, production or not, so the
# broker can recollect open orders regardless of the mode it runs in.

config = load_app_config()
symbols: set[str] = config.symbols()

# The symbols each venue trades, so a process that asks a venue about its
# orders asks only about markets that exist on it: a futures wallet has no
# spot symbols and a spot wallet no contracts.
symbols_per_venue: dict[str, set[str]] = {}
for _venue_id, _symbol in config.venue_symbol_pairs():
    symbols_per_venue.setdefault(_venue_id, set()).add(_symbol)

authenticated_clients: dict[str, CustomExchange] = {}

for venue in config.venues:
    authenticated_clients[venue.id] = build_client(venue)
