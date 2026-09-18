"""Feed handler for trading fees.

Fetches each venue's fee schedule at startup and then on a fixed interval
(``[fees] refresh_s``): fee levels follow the account's rolling volume and
balance, so the rates the matcher and strategies size with must not be
frozen at startup. Every fetch is published as a ``FeeScheduleEvent`` on
``acct:fees:{venue}`` and written to the snapshot key ``fees-{venue}`` for
polling consumers.
"""

import asyncio
import logging
from typing import Any

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.config import AppConfig, VenueConfig, load_app_config
from apps.shared.src.events import FeeScheduleEvent, encode, now_ns
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.fees import fee_schedule_from_client, fees_snapshot_key
from apps.shared.src.streams import StreamPublisher
from apps.shared.src.structs import CustomExchange
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def publish_fees(
    redis: Redis, publisher: StreamPublisher, event: FeeScheduleEvent
) -> None:
    """
    Publish one fee schedule as an event and a snapshot key.

    The snapshot carries the encoded event, so a consumer can decode it
    with the shared codec without a second schema to maintain.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher holding sequence numbers and trimming settings.
    event : FeeScheduleEvent
        The schedule of one venue.
    """
    async with redis.pipeline(transaction=False) as pipe:
        pipe.set(fees_snapshot_key(event.venue), encode(event))
        publisher.xadd(pipe, event)
        await pipe.execute()


async def watch_fees(
    client: CustomExchange,
    venue: VenueConfig,
    symbols: set[str],
    refresh_s: int,
    redis: Redis,
    publisher: StreamPublisher,
) -> None:
    """
    Fetch and publish a venue's fee schedule forever.

    The venue's markets are loaded on every pass: the first pass primes
    the client's market dict, later passes pick up listing changes and
    updated default rates. A failed pass is logged and retried on the next
    interval; nothing else in this process must go down because a REST
    call did.

    Parameters
    ----------
    client : CustomExchange
        The exchange client.
    venue : VenueConfig
        The venue, whose fee policy and static overrides apply.
    symbols : set[str]
        The symbols the schedule covers.
    refresh_s : int
        Seconds between fetches.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``acct:fees:{venue}``.
    """
    while True:
        try:
            await client.load_markets()
            event = await fee_schedule_from_client(client, venue, symbols, now_ns())
            await publish_fees(redis, publisher, event)
            logger.info(
                f"Published fee schedule for {event.venue} "
                f"({event.source.value}, {len(event.symbols)} symbols)"
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001, the feed must survive
            logger.error(f"Error in fee schedule for {client.id}: {error}")
        await asyncio.sleep(refresh_s)


def build_tasks(
    config: AppConfig,
    clients: dict[str, CustomExchange],
    redis: Redis,
    publisher: StreamPublisher,
    production_mode: bool,
) -> list[Any]:
    """
    Create one fee watch coroutine per venue we trade on.

    Only venues with an authenticated client are polled, since the account
    tier is what the schedule reports; a venue declared for market data
    alone has no schedule to fetch.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    clients : dict[str, CustomExchange]
        Authenticated exchange clients keyed by venue id.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``acct:fees:{venue}``.
    production_mode : bool
        Which strategies' subscriptions to serve.

    Returns
    -------
    list[Any]
        Coroutines ready to be gathered.
    """
    symbols_per_venue: dict[str, set[str]] = {}
    for venue_id, symbol in config.venue_symbol_pairs(production_mode):
        symbols_per_venue.setdefault(venue_id, set()).add(symbol)

    tasks: list[Any] = []
    for venue_id, symbols in sorted(symbols_per_venue.items()):
        client = clients.get(venue_id)
        if client is None:
            logger.warning(f"No authenticated client for {venue_id}, not watching fees")
            continue
        venue = next(v for v in config.venues if v.id == venue_id)
        tasks.append(
            watch_fees(client, venue, symbols, config.fees.refresh_s, redis, publisher)
        )
    return tasks


async def main(config: AppConfig, clients: dict[str, CustomExchange]) -> None:
    """
    Publish every venue's fee schedule until cancelled.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    clients : dict[str, CustomExchange]
        A dict mapping CCXT short id to a CCXT exchange client.
    """
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    publisher = StreamPublisher(maxlen=config.market_data.stream_maxlen)

    tasks = build_tasks(config, clients, redis, publisher, production)
    logger.info(f"Starting {len(tasks)} fee watchers")
    try:
        await asyncio.gather(*tasks)
    finally:
        for client in clients.values():
            await client.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main(load_app_config(), authenticated_clients))
