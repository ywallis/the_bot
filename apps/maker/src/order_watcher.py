"""Feed handler for our own orders.

One ``watch_orders`` loop per venue and symbol we can trade. Every CCXT order
update becomes an ``OrderEvent`` on ``oms:events``, which strategies, the
matcher and the recorder all read. This is a feed handler like the book and
trade watchers: it holds no order state machine and makes no trading
decision, it only reports what the venue says about an order.

It was the first half of ``matcher.py`` until phase 3. Splitting it out is
what lets the hedging logic become an ordinary consumer of the bus rather
than the only component that can see a fill. See
``docs/design/event-driven-framework.md`` section 6.
"""

import asyncio
import logging
import time
from collections import OrderedDict
from decimal import Decimal
from typing import Any

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.enums import OidComponent
from apps.maker.src.structs import LimitedSet
from apps.maker.src.utils import info_from_oid
from apps.maker.src.watcher import handle_feed_error
from apps.shared.src.ccxt_events import order_event_from_ccxt
from apps.shared.src.config import AppConfig, load_app_config
from apps.shared.src.events import OrderEvent, now_ns
from apps.shared.src.exchange_clients import authenticated_clients
from apps.shared.src.streams import StreamPublisher
from apps.shared.src.structs import CustomExchange
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# Tags carrying the two halves of our order id convention, so a consumer can
# route on the strategy without re-parsing the client order id.
STRATEGY_TAG = "strategy_id"
ORDER_TAG = "order_id"

# How far before the last websocket delivery a reconciliation fetches from.
# Venue timestamps and ours disagree by a little, and an update can be in
# flight when the socket drops; overlap is harmless, a gap is not.
RECONCILE_MARGIN_MS = 5_000

# How often the venue is asked over REST regardless of what the socket is
# doing. Live, a terminal fill update went unreported for 23 minutes with no
# error raised and the subscription still up, so waiting for a drop is not
# enough. The schedule is unconditional rather than "only while the socket
# is quiet", because a socket can keep delivering other orders' updates
# while silently dropping one, and then no measure of silence ever fires.
# This is what bounds how long a fill can sit unseen, and an unseen fill is
# an unhedged one. Costs one fetch per interval per venue and symbol, which
# publishes nothing when nothing was missed.
RECONCILE_INTERVAL_S = 30.0

# How far back a scheduled reconciliation looks. The error path knows when
# the socket died and fetches from there; a scheduled one has no such
# anchor, since a silently dropped update leaves no trace of its own age.
# This is therefore the oldest a missed update can be and still be
# recovered, and it wants to be comfortably larger than the interval.
RECONCILE_LOOKBACK_MS = 600_000

# Orders remembered per loop, for deduplication and fill deltas. Large enough
# to cover any order still open, small enough to stay bounded in a process
# that runs for weeks.
ORDER_MEMORY = 2000


class BoundedDict:
    """
    A mapping that forgets its oldest key once it is full.

    Attributes
    ----------
    max_size : int
        Maximum number of keys held.
    """

    def __init__(self, max_size: int) -> None:
        """
        Initialize the mapping.

        Parameters
        ----------
        max_size : int
            Maximum number of keys held.
        """
        self.max_size = max_size
        self._items: OrderedDict[str, Decimal] = OrderedDict()

    def get(self, key: str, default: Decimal) -> Decimal:
        """
        Return the value for a key.

        Parameters
        ----------
        key : str
            The key.
        default : Decimal
            Returned when the key is absent.

        Returns
        -------
        Decimal
            The value.
        """
        return self._items.get(key, default)

    def set(self, key: str, value: Decimal) -> None:
        """
        Store a value, evicting the oldest key if the mapping is full.

        Parameters
        ----------
        key : str
            The key.
        value : Decimal
            The value.
        """
        if key in self._items:
            self._items.move_to_end(key)
        elif len(self._items) >= self.max_size:
            self._items.popitem(last=False)
        self._items[key] = value

    def __len__(self) -> int:
        """Return the number of keys held."""
        return len(self._items)


def oid_components(client_order_id: Any) -> tuple[str, str]:
    """
    Split our client order id into its strategy and order identifiers.

    Parameters
    ----------
    client_order_id : Any
        Value of the CCXT ``clientOrderId`` field.

    Returns
    -------
    tuple[str, str]
        Strategy identifier and order identifier, both empty for an id that
        does not follow our convention. Orders placed outside this system,
        by hand or by an older build, reach the same websocket and must not
        take the loop down.
    """
    if not client_order_id:
        return ("", "")
    oid = str(client_order_id)
    try:
        return (
            info_from_oid(oid, OidComponent.STRATEGY),
            info_from_oid(oid, OidComponent.ORDER),
        )
    except Exception:
        logger.debug(f"Client order id {oid!r} is not ours, reporting it unattributed")
        return ("", "")


def strategy_key(strategy_identifier: str, order_identifier: str) -> str:
    """
    Rebuild the strategy key an order belongs to.

    Strategies name themselves ``<identifier>_<order identifier>`` when they
    submit, so events carry the same key their intents did and a strategy can
    filter ``oms:events`` down to its own orders.

    Parameters
    ----------
    strategy_identifier : str
        Strategy identifier from the order id.
    order_identifier : str
        Order identifier from the order id.

    Returns
    -------
    str
        The strategy key, empty if the order is not attributable.
    """
    if not strategy_identifier:
        return ""
    return f"{strategy_identifier}_{order_identifier}"


def update_key(order: dict[str, Any]) -> tuple[Any, ...]:
    """
    Return the identity of an order update for deduplication.

    Venues replay their cached orders after a resubscription, so the same
    update arrives twice. An update is identified by the order it concerns
    and the state that update reports, which means a genuine transition is
    always published and a replay of a transition already seen is not.

    Parameters
    ----------
    order : dict[str, Any]
        CCXT unified order.

    Returns
    -------
    tuple[Any, ...]
        The identity.
    """
    return (
        order.get("clientOrderId") or order.get("id"),
        order.get("status"),
        str(order.get("filled")),
    )


def event_from_order(
    venue: str,
    order: dict[str, Any],
    ts_recv: int,
    filled_so_far: BoundedDict,
) -> OrderEvent:
    """
    Build the ``OrderEvent`` for one CCXT order update.

    Parameters
    ----------
    venue : str
        CCXT short id.
    order : dict[str, Any]
        CCXT unified order.
    ts_recv : int
        Local receive time in nanoseconds.
    filled_so_far : BoundedDict
        Cumulative filled size per order id, updated in place so the next
        update of the same order can report the fill that caused it.

    Returns
    -------
    OrderEvent
        The event.
    """
    strategy_identifier, order_identifier = oid_components(order.get("clientOrderId"))
    order_id = str(order.get("clientOrderId") or order.get("id") or "")
    previous_filled = filled_so_far.get(order_id, Decimal(0))
    event = order_event_from_ccxt(
        venue,
        order,
        strategy=strategy_key(strategy_identifier, order_identifier),
        ts_recv=ts_recv,
        previous_filled=previous_filled,
        tags={STRATEGY_TAG: strategy_identifier, ORDER_TAG: order_identifier},
    )
    filled_so_far.set(order_id, event.filled)
    return event


async def publish_updates(
    client: CustomExchange,
    orders: list[dict[str, Any]],
    ts_recv: int,
    redis: Redis,
    publisher: StreamPublisher,
    seen: LimitedSet,
    filled_so_far: BoundedDict,
) -> int:
    """
    Publish every order update not already published.

    Parameters
    ----------
    client : CustomExchange
        The exchange client the updates came from.
    orders : list[dict[str, Any]]
        CCXT unified orders, from the websocket or from a REST fetch.
    ts_recv : int
        Local receive time in nanoseconds.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``oms:events``.
    seen : LimitedSet
        Update keys already published, updated in place.
    filled_so_far : BoundedDict
        Cumulative filled size per order id, updated in place.

    Returns
    -------
    int
        Number of events published.
    """
    published = 0
    for order in list(orders):
        key = update_key(order)
        if key in seen:
            logger.debug(f"Dropping replayed order update {key}")
            continue
        seen.add(key)
        event = event_from_order(client.id, order, ts_recv, filled_so_far)
        await publisher.publish(redis, event)
        published += 1
        logger.info(
            f"{event.intent_id} on {client.id} is {event.state.value}, "
            f"filled {event.filled}/{event.filled + event.remaining}"
        )
    return published


async def fetch_orders_since(
    client: Any, ticker: str, since: int
) -> list[dict[str, Any]]:
    """
    Fetch every order of ours that changed since a time, over REST.

    Venues differ in what they expose. One call to ``fetch_orders`` where
    the venue has it; otherwise the open orders plus whatever closed or
    cancelled ones the venue can list, which between them cover every state
    an order can be in.

    Parameters
    ----------
    client : Any
        The exchange client. Typed loosely because the optional endpoints
        are looked up on the client's ``has`` map.
    ticker : str
        The trading pair symbol.
    since : int
        Milliseconds since the epoch.

    Returns
    -------
    list[dict[str, Any]]
        CCXT unified orders.
    """
    has = getattr(client, "has", {}) or {}
    if has.get("fetchOrders"):
        return list(await client.fetch_orders(ticker, since=since))
    orders: list[dict[str, Any]] = list(await client.fetch_open_orders(ticker))
    if has.get("fetchCanceledAndClosedOrders"):
        orders += await client.fetch_canceled_and_closed_orders(ticker, since=since)
    else:
        if has.get("fetchClosedOrders"):
            orders += await client.fetch_closed_orders(ticker, since=since)
        if has.get("fetchCanceledOrders"):
            orders += await client.fetch_canceled_orders(ticker, since=since)
    return orders


async def reconcile(
    client: CustomExchange,
    ticker: str,
    since: int,
    redis: Redis,
    publisher: StreamPublisher,
    seen: LimitedSet,
    filled_so_far: BoundedDict,
) -> int:
    """
    Publish what the websocket missed while it was down.

    A venue replays its cache after a resubscription, but the cache holds
    open orders, so an order that was placed and finished during the gap,
    including one that filled, never comes back over the socket. This asks
    the venue over REST for everything that changed since shortly before
    the last update the socket delivered, and publishes whatever has not
    been published. Overlap with what the socket already delivered, or
    delivers on resubscription, is dropped by the same update keys.

    A failure here is logged and swallowed: the socket is back, and the next
    drop will reconcile again. Nothing else in this process must go down
    because a REST call did.

    Parameters
    ----------
    client : CustomExchange
        The exchange client.
    ticker : str
        The trading pair symbol.
    since : int
        Milliseconds since the epoch to fetch from.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``oms:events``.
    seen : LimitedSet
        Update keys already published, updated in place.
    filled_so_far : BoundedDict
        Cumulative filled size per order id, updated in place.

    Returns
    -------
    int
        Number of events published, 0 on failure.
    """
    try:
        orders = await fetch_orders_since(client, ticker, since)
    except asyncio.CancelledError:
        raise
    except Exception as error:  # noqa: BLE001, the feed must survive
        logger.error(f"Could not reconcile orders on {client.id} {ticker}: {error}")
        return 0
    published = await publish_updates(
        client, orders, now_ns(), redis, publisher, seen, filled_so_far
    )
    logger.info(
        f"Reconciled {client.id} {ticker} from {since}: {len(orders)} orders "
        f"fetched, {published} updates the socket had missed"
    )
    return published


async def watch_orders(
    client: CustomExchange,
    ticker: str,
    redis: Redis,
    publisher: StreamPublisher,
) -> None:
    """
    Watch our orders on one venue and symbol and publish every update.

    The venue is asked over REST for what the socket has not reported
    (``reconcile``) on two triggers. After every feed error the loop
    survives: venues dropped the socket about twice an hour, and an order
    placed and finished inside one of those gaps is otherwise never
    reported. And every ``RECONCILE_INTERVAL_S`` unconditionally, because a
    subscription can stop delivering an order's updates without ever
    raising, which no error path can see.

    The pending watch is deliberately left running across a scheduled
    reconciliation rather than cancelled and restarted. CCXT resolves a
    watch from a cache of what it has received; cancelling the await can
    drop an update that is already buffered, which is precisely the
    failure this reconciliation exists to catch.

    Parameters
    ----------
    client : CustomExchange
        The exchange client.
    ticker : str
        The trading pair symbol.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``oms:events``.
    """
    seen = LimitedSet(ORDER_MEMORY)
    filled_so_far = BoundedDict(ORDER_MEMORY)
    # Orders that predate this process are the order manager's business at
    # startup, not this loop's, so the venue is asked only for what happens
    # from now on. Anything the venue replays anyway is dropped by `seen`.
    since = int(time.time() * 1000)
    last_delivery_ms = since
    due_at = time.monotonic() + RECONCILE_INTERVAL_S
    watching: asyncio.Task[list[dict[str, Any]]] | None = None
    try:
        while True:
            try:
                if watching is None:
                    watching = asyncio.ensure_future(
                        client.watch_orders(ticker, since=since)
                    )
                # Checked before racing the socket, so a venue delivering
                # faster than the interval cannot starve the schedule.
                if time.monotonic() >= due_at:
                    await reconcile(
                        client,
                        ticker,
                        int(time.time() * 1000) - RECONCILE_LOOKBACK_MS,
                        redis,
                        publisher,
                        seen,
                        filled_so_far,
                    )
                    due_at = time.monotonic() + RECONCILE_INTERVAL_S
                    continue
                timer = asyncio.ensure_future(
                    asyncio.sleep(max(0.0, due_at - time.monotonic()))
                )
                try:
                    done, _pending = await asyncio.wait(
                        {watching, timer}, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    timer.cancel()
                if watching not in done:
                    continue
                orders: list[dict[str, Any]] = watching.result()
                watching = None
                ts_recv = now_ns()
                last_delivery_ms = ts_recv // 1_000_000
                await publish_updates(
                    client, orders, ts_recv, redis, publisher, seen, filled_so_far
                )
            except BaseException as e:  # noqa: B036, narrowed in handle_feed_error
                if watching is not None and not watching.done():
                    watching.cancel()
                watching = None
                await handle_feed_error(e, client, "watch_orders", ticker)
                await reconcile(
                    client,
                    ticker,
                    last_delivery_ms - RECONCILE_MARGIN_MS,
                    redis,
                    publisher,
                    seen,
                    filled_so_far,
                )
    finally:
        if watching is not None:
            watching.cancel()


def build_tasks(
    config: AppConfig,
    clients: dict[str, CustomExchange],
    redis: Redis,
    publisher: StreamPublisher,
    production_mode: bool,
) -> list[Any]:
    """
    Create one watch coroutine per venue and symbol we can trade.

    Subscriptions declare where a strategy looks, not where it trades, so
    this is a superset of the venues we place orders on. Pairs whose venue
    has no authenticated client are skipped rather than started and left to
    fail, since a venue can be declared for market data alone.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    clients : dict[str, CustomExchange]
        Authenticated exchange clients keyed by venue id.
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``oms:events``.
    production_mode : bool
        Which strategies' subscriptions to serve.

    Returns
    -------
    list[Any]
        Coroutines ready to be gathered.
    """
    tasks: list[Any] = []
    for venue, symbol in sorted(config.venue_symbol_pairs(production_mode)):
        client = clients.get(venue)
        if client is None:
            logger.warning(f"No authenticated client for {venue}, not watching orders")
            continue
        tasks.append(watch_orders(client, symbol, redis, publisher))
    return tasks


async def main(config: AppConfig, clients: dict[str, CustomExchange]) -> None:
    """
    Run all order watch loops until one fails.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    clients : dict[str, CustomExchange]
        Authenticated exchange clients keyed by venue id.
    """
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    publisher = StreamPublisher(maxlen=config.oms.stream_maxlen)

    tasks = build_tasks(config, clients, redis, publisher, production)
    logger.info(f"Starting {len(tasks)} order watchers")
    try:
        await asyncio.gather(*tasks)
    finally:
        for client in clients.values():
            await client.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main(load_app_config(), authenticated_clients))
