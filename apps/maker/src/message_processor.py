"""Order manager: the single consumer of ``oms:intents``.

Strategies publish ``OrderIntent`` and ``CancelIntent`` events; this process
reads them through the ``oms`` consumer group, drives the venue through the
broker, and publishes every state transition to ``oms:events`` and the timing
of every intent to ``oms:latency``.

Three things follow from the consumer group. Exactly one process acts on each
intent, so there is no second order manager racing this one. An intent is
acknowledged only once it can no longer be acted on, so a crash mid-flight
replays it rather than losing it; ``max_intent_age_s`` bounds what a replay
can do by rejecting intents that have gone stale. And the position in the
stream is Redis' to keep, so a restart resumes where this process stopped.

Order state is per intent, keyed by venue and intent id, because the matcher
reuses an order's client id on the other venue when it hedges. It is fed both
by what the broker confirms and by what the order watcher reports on
``oms:events``, so an order that fills or is cancelled at the venue leaves
this process's book without anyone asking.

Legacy strategies still publish dicts on a pubsub channel. ``legacy_bridge``
runs inside this process, translates them into intents and publishes them to
``oms:intents`` like any other producer, so there is exactly one input path
here. See ``docs/design/event-driven-framework.md`` section 6.
"""

import ast
import asyncio
import json
import logging
import signal
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import ResponseError

import apps.shared.src.logging_config as logging_config
from apps.maker.src import legacy_bridge
from apps.maker.src.constants import BROKER_CHANNEL
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.errors import BrokerError
from apps.maker.src.legacy_bridge import LEGACY_ORDER_TYPE_TAG
from apps.maker.src.structs import (
    CancellationMessage,
    OrderBatchMessage,
    OrderMessage,
    Response,
)
from apps.maker.src.utils import identify_response, parse_message
from apps.shared.src.config import AppConfig, load_app_config
from apps.shared.src.events import (
    INTENTS_STREAM,
    OMS_CONSUMER_GROUP,
    ORDER_EVENTS_STREAM,
    AnyEvent,
    CancelIntent,
    LatencyRecord,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    from_stream_fields,
    now_ns,
)
from apps.shared.src.streams import StreamPublisher, entry_id_str, stream_tail

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# An order is identified by its venue as well as its intent id: the matcher
# hedges a fill by placing an order with the same client id on the other
# venue, so intent ids are only unique per venue.
OrderKey = tuple[str, str]

# States after which an order can no longer be acted on and is dropped from
# the book.
TERMINAL_STATES: frozenset[OrderState] = frozenset(
    {
        OrderState.FILLED,
        OrderState.CANCELLED,
        OrderState.EXPIRED,
        OrderState.REJECTED,
    }
)

# Start position for the first read of a consumer's own pending entries.
PENDING = "0"
# Start position for entries no consumer in the group has read yet.
NEW = ">"


@dataclass
class TrackedOrder:
    """
    An order this process placed and has not yet seen finish.

    Attributes
    ----------
    intent : OrderIntent
        The intent that created the order.
    state : OrderState
        Last state published for it.
    venue_order_id : str | None
        Venue-assigned id, known once the broker confirms the placement. It
        is what a cancellation has to name.
    filled : Decimal
        Cumulative filled size, as last reported by the order watcher.
    remaining : Decimal
        Remaining size.
    avg_price : Decimal | None
        Average fill price, if anything filled.
    """

    intent: OrderIntent
    state: OrderState
    venue_order_id: str | None = None
    filled: Decimal = Decimal(0)
    remaining: Decimal = Decimal(0)
    avg_price: Decimal | None = None

    @property
    def key(self) -> OrderKey:
        """
        Return the book key of this order.

        Returns
        -------
        OrderKey
            Venue and intent id.
        """
        return (self.intent.venue, self.intent.intent_id)


@dataclass
class QueuedIntent:
    """
    An intent held back because its strategy is busy at the broker.

    Attributes
    ----------
    entry_id : str
        Redis entry id, still unacknowledged so a crash replays the intent.
    intent : OrderIntent
        The intent.
    ts_oms_recv : int
        When this process read the intent from the stream. Carried through
        the queue so the latency record keeps meaning what it says: the
        wait here belongs to the order manager, not to the wire.
    """

    entry_id: str
    intent: OrderIntent
    ts_oms_recv: int


def legacy_order_type(intent: OrderIntent) -> OrderType:
    """
    Return the order type the broker's message format expects.

    The broker only distinguishes market orders from limit orders. The
    replace and unique distinction is the order manager's own, and survives
    here so that an order recollected from the venue is classified the same
    way it was when it was placed.

    Parameters
    ----------
    intent : OrderIntent
        The intent.

    Returns
    -------
    OrderType
        The legacy order type.
    """
    tag = intent.tags.get(LEGACY_ORDER_TYPE_TAG)
    if tag is not None:
        return OrderType(tag)
    if intent.order_type is OrderKind.MARKET:
        return OrderType.MARKET
    if intent.replace_of is not None:
        return OrderType.REPLACE
    return OrderType.UNIQUE


def replaces(intent: OrderIntent) -> bool:
    """
    Return whether an intent supersedes the strategy's resting order.

    This is the opt-in that selects the cancel-then-place behaviour and the
    latest-wins coalescing: exactly what a quote-replacement strategy wants
    and wrong for a strategy whose orders are all meant to reach the venue.

    Parameters
    ----------
    intent : OrderIntent
        The intent.

    Returns
    -------
    bool
        True if the intent replaces an earlier order.
    """
    if intent.replace_of is not None:
        return True
    return intent.tags.get(LEGACY_ORDER_TYPE_TAG) == OrderType.REPLACE.value


def broker_order_from_intent(intent: OrderIntent) -> OrderMessage:
    """
    Build the broker message that places an intent.

    Parameters
    ----------
    intent : OrderIntent
        The intent.

    Returns
    -------
    OrderMessage
        The message to publish on the broker channel.
    """
    return OrderMessage(
        kind=MessageType.ORDER,
        strategy=intent.strategy,
        exchange=intent.venue,
        id=intent.intent_id,
        exchange_id="_",
        pair=intent.symbol,
        side=OrderSide(intent.side.value),
        order_type=legacy_order_type(intent),
        price=intent.price if intent.price is not None else Decimal(0),
        amount=intent.amount,
    )


def broker_cancellation_for(tracked: TrackedOrder) -> CancellationMessage:
    """
    Build the broker message that cancels a tracked order.

    Parameters
    ----------
    tracked : TrackedOrder
        The order to cancel.

    Returns
    -------
    CancellationMessage
        The message to publish on the broker channel. It names the venue
        order id, falling back to the client id for an order whose placement
        was confirmed without one.
    """
    return CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=tracked.intent.strategy,
        exchange=tracked.intent.venue,
        id=tracked.venue_order_id or tracked.intent.intent_id,
        pair=tracked.intent.symbol,
    )


def tracked_from_legacy_order(order: OrderMessage) -> TrackedOrder:
    """
    Rebuild a tracked order from an order recollected at the venue.

    Parameters
    ----------
    order : OrderMessage
        An order the broker found open on a venue at startup.

    Returns
    -------
    TrackedOrder
        The order, as if this process had placed it. Its intent carries the
        legacy order type so that it is cancelled and superseded exactly
        like the order it stands for.
    """
    intent = OrderIntent(
        ts_recv=now_ns(),
        intent_id=order["id"],
        strategy=order["strategy"],
        venue=order["exchange"],
        symbol=order["pair"],
        side=Side(order["side"].value),
        order_type=(
            OrderKind.MARKET
            if order["order_type"] == OrderType.MARKET
            else OrderKind.LIMIT
        ),
        amount=Decimal(order["amount"]),
        price=Decimal(order["price"]),
        tags={LEGACY_ORDER_TYPE_TAG: order["order_type"].value},
    )
    return TrackedOrder(
        intent=intent,
        state=OrderState.OPEN,
        venue_order_id=order["exchange_id"],
        remaining=Decimal(order["amount"]),
    )


def venue_ack_ns(confirmation: dict[str, Any]) -> int | None:
    """
    Return the venue's acceptance time from a CCXT order, in nanoseconds.

    Parameters
    ----------
    confirmation : dict[str, Any]
        CCXT order returned by the broker.

    Returns
    -------
    int | None
        Nanoseconds since the epoch, or None if the venue reported no
        timestamp.
    """
    timestamp = confirmation.get("timestamp")
    if timestamp is None:
        return None
    try:
        return int(timestamp) * 1_000_000
    except (TypeError, ValueError):
        return None


class OrderManager:
    """
    Consume order intents, drive the broker and publish order events.

    Attributes
    ----------
    config : AppConfig
        The application configuration.
    redis : Redis
        Redis client used for the consumer group, the broker channel and
        event publication.
    publisher : StreamPublisher
        Publisher for ``oms:events`` and ``oms:latency``.
    orders : dict[OrderKey, TrackedOrder]
        Orders placed and not yet finished.
    resting : dict[str, OrderKey]
        The order each strategy currently has resting, for intents that
        supersede without naming what they supersede.
    locks : dict[str, asyncio.Lock]
        One lock per strategy, serialising its cancel-then-place sequences.
    queued : dict[str, QueuedIntent]
        The one intent held per busy strategy, latest wins.
    tasks : list[asyncio.Task]
        In-flight intent processing.
    shutdown_event : asyncio.Event
        Set by SIGTERM.
    """

    def __init__(
        self,
        config: AppConfig,
        redis: Redis,
        pool: ConnectionPool | None = None,
    ) -> None:
        """
        Initialize the order manager.

        Parameters
        ----------
        config : AppConfig
            The application configuration.
        redis : Redis
            Redis client created with ``decode_responses=True``.
        pool : ConnectionPool | None
            Connection pool used for the one-off client that recollects open
            orders at startup.
        """
        self.config = config
        self.redis = redis
        self.pool = pool
        self.publisher = StreamPublisher(maxlen=config.oms.stream_maxlen)
        self.orders: dict[OrderKey, TrackedOrder] = {}
        self.resting: dict[str, OrderKey] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self.queued: dict[str, QueuedIntent] = {}
        self.tasks: list[asyncio.Task] = []
        self.shutdown_event = asyncio.Event()
        self.cursor = PENDING
        self.intents_handled = 0

    # Intent intake ---------------------------------------------------------

    async def ensure_group(self) -> None:
        """
        Create the consumer group, tolerating one that already exists.

        The group starts at the end of the stream, so a first start never
        replays intents that were published while no order manager existed.
        """
        try:
            await self.redis.xgroup_create(
                INTENTS_STREAM, OMS_CONSUMER_GROUP, id="$", mkstream=True
            )
            logger.info(
                f"Created group {OMS_CONSUMER_GROUP} on {INTENTS_STREAM}"
            )
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise
            logger.debug(f"Consumer group {OMS_CONSUMER_GROUP} already exists")

    async def consume_once(self) -> int:
        """
        Read one batch of intents and start processing each of them.

        The first reads use the consumer's pending list, which holds the
        entries a previous incarnation of this process read but never
        acknowledged. Once that is empty the cursor moves to new entries and
        stays there.

        While draining the pending list the cursor advances past each entry
        as it is read. Processing is asynchronous, so an entry is still
        pending when the next read goes out, and a cursor that stayed at the
        start of the list would hand the same entries back and act on them
        again on every pass. The pending list itself remains the record of
        what is unfinished, so advancing here loses nothing: a crash
        mid-drain leaves the entries in it and the next process starts over
        from the beginning.

        Returns
        -------
        int
            Number of intents read.
        """
        settings = self.config.oms
        response = await self.redis.xreadgroup(
            OMS_CONSUMER_GROUP,
            settings.consumer,
            {INTENTS_STREAM: self.cursor},
            count=settings.batch,
            block=settings.block_ms,
        )
        read = 0
        # redis-py types the reply as list or dict (RESP3); the client is
        # RESP2 here so it is always the list form.
        for _stream, entries in cast(list[Any], response or []):
            if not entries and self.cursor == PENDING:
                logger.info("No intents left pending, following new entries")
                self.cursor = NEW
                continue
            for entry_id, fields in entries:
                read += 1
                entry = entry_id_str(entry_id)
                if self.cursor != NEW:
                    self.cursor = entry
                self.start(entry, fields)
        return read

    def start(self, entry_id: str, fields: dict[Any, Any]) -> None:
        """
        Decode one entry and start the task that acts on it.

        An entry that cannot be decoded is acknowledged and dropped: it is
        already in the recording, and leaving it pending would make every
        restart replay it forever.

        Parameters
        ----------
        entry_id : str
            Redis entry id.
        fields : dict[Any, Any]
            Field map of the entry.
        """
        try:
            event = from_stream_fields(fields)
        except Exception as error:  # noqa: BLE001, a bad entry must not wedge us
            logger.error(f"Undecodable intent {entry_id}, dropping: {error}")
            self.tasks.append(asyncio.create_task(self.ack(entry_id)))
            return
        self.intents_handled += 1
        self.tasks.append(asyncio.create_task(self.process(entry_id, event)))

    async def process(self, entry_id: str, event: AnyEvent) -> None:
        """
        Act on one decoded intent.

        Parameters
        ----------
        entry_id : str
            Redis entry id.
        event : AnyEvent
            The decoded event.
        """
        ts_oms_recv = now_ns()
        try:
            match event:
                case OrderIntent():
                    await self.process_order_intent(entry_id, event, ts_oms_recv)
                case CancelIntent():
                    await self.process_cancel_intent(entry_id, event)
                case _:
                    logger.error(
                        f"{type(event).__name__} is not an intent, dropping {entry_id}"
                    )
                    await self.ack(entry_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001, one bad intent is not fatal
            logger.exception(f"Failed to process intent {entry_id}: {error}")
            await self.ack(entry_id)

    def is_stale(self, intent: OrderIntent | CancelIntent, now: int) -> bool:
        """
        Return whether an intent is too old to act on.

        Parameters
        ----------
        intent : OrderIntent | CancelIntent
            The intent.
        now : int
            Current time in nanoseconds.

        Returns
        -------
        bool
            True if the intent should be rejected rather than sent to a
            venue.
        """
        max_age_ns = int(self.config.oms.max_intent_age_s * 1_000_000_000)
        return now - intent.ts_recv > max_age_ns

    async def process_order_intent(
        self, entry_id: str, intent: OrderIntent, ts_oms_recv: int
    ) -> None:
        """
        Place an order intent, serialising it if its strategy is busy.

        An intent that supersedes takes the strategy's lock, so the cancel
        and the placement it implies cannot interleave with another of the
        strategy's quotes. Intents that do not supersede are independent
        orders with nothing to serialise, so they go straight to the broker.

        Parameters
        ----------
        entry_id : str
            Redis entry id.
        intent : OrderIntent
            The intent.
        ts_oms_recv : int
            When this process read the intent, nanoseconds. Used for the
            latency record only; staleness is judged against the clock now,
            because an intent drained from the queue carries its original
            read time and may have aged a great deal since.
        """
        if self.is_stale(intent, now_ns()):
            await self.reject(intent, "intent is older than max_intent_age_s")
            await self.ack(entry_id)
            return

        if not replaces(intent):
            await self.place(intent, ts_oms_recv)
            await self.ack(entry_id)
            return

        strategy = intent.strategy
        lock = self.get_lock(strategy)
        if lock.locked():
            superseded = self.queued.get(strategy)
            self.queued[strategy] = QueuedIntent(entry_id, intent, ts_oms_recv)
            if superseded is not None:
                logger.debug(
                    f"{intent.intent_id} replaced {superseded.intent.intent_id} "
                    f"in the queue for {strategy}"
                )
                await self.reject(
                    superseded.intent, f"superseded by {intent.intent_id}"
                )
                await self.ack(superseded.entry_id)
            return

        async with lock:
            await self.cancel_superseded(intent)
            await self.place(intent, ts_oms_recv)
        await self.ack(entry_id)
        self.drain(strategy)

    def drain(self, strategy: str) -> None:
        """
        Start processing the intent a strategy had queued, if any.

        The intent keeps the receive time it was read with rather than being
        stamped afresh. ``ts_oms_recv`` means "read from the stream", so the
        time an intent spent waiting for its strategy's lock has to show up
        between ``ts_oms_recv`` and ``ts_broker_send``, which is the pair
        that measures time inside this process. Re-stamping moved that wait
        onto the leg from the strategy instead, where a latency model built
        from ``oms:latency`` would read it as transport it cannot avoid. A
        13 minute live run put up to a second of it there.

        Parameters
        ----------
        strategy : str
            The strategy key.
        """
        queued = self.queued.pop(strategy, None)
        if queued is None:
            return
        logger.debug(f"Processing queued intent {queued.intent.intent_id}")
        self.tasks.append(
            asyncio.create_task(
                self.process_order_intent(
                    queued.entry_id, queued.intent, queued.ts_oms_recv
                )
            )
        )

    async def process_cancel_intent(self, entry_id: str, intent: CancelIntent) -> None:
        """
        Cancel the order a cancel intent names.

        An empty ``target_intent_id`` means "whatever this strategy has
        resting", which is what legacy strategies send.

        A cancellation is never rejected for age. Cancelling late still
        closes a position the strategy no longer wants, where placing late
        opens one it no longer wants.

        Parameters
        ----------
        entry_id : str
            Redis entry id.
        intent : CancelIntent
            The intent.
        """
        async with self.get_lock(intent.strategy):
            if intent.target_intent_id:
                key: OrderKey | None = (intent.venue, intent.target_intent_id)
            else:
                key = self.resting.get(intent.strategy)
            if key is None or key not in self.orders:
                logger.warning(
                    f"Cancellation for {intent.strategy} has no open order to cancel"
                )
            else:
                await self.cancel_order(key, "cancelled on request")
        await self.ack(entry_id)
        # A quote can have queued behind this cancellation, and only whoever
        # releases the lock will ever start it.
        self.drain(intent.strategy)

    async def ack(self, entry_id: str) -> None:
        """
        Acknowledge an intent that can no longer be acted on.

        Parameters
        ----------
        entry_id : str
            Redis entry id.
        """
        await self.redis.xack(INTENTS_STREAM, OMS_CONSUMER_GROUP, entry_id)

    def get_lock(self, strategy: str) -> asyncio.Lock:
        """
        Get or create the lock for a strategy.

        Parameters
        ----------
        strategy : str
            The strategy key.

        Returns
        -------
        asyncio.Lock
            The lock.
        """
        if strategy not in self.locks:
            self.locks[strategy] = asyncio.Lock()
            logger.debug(f"No lock found, creating one for {strategy}")
        return self.locks[strategy]

    # Order lifecycle -------------------------------------------------------

    async def cancel_superseded(self, intent: OrderIntent) -> None:
        """
        Cancel the order a superseding intent replaces, if it is still open.

        Parameters
        ----------
        intent : OrderIntent
            The superseding intent.
        """
        if intent.replace_of is not None:
            key: OrderKey | None = (intent.venue, intent.replace_of)
        else:
            key = self.resting.get(intent.strategy)
        if key is None or key not in self.orders:
            logger.debug(f"Nothing to supersede for {intent.strategy}")
            return
        await self.cancel_order(key, f"replaced by {intent.intent_id}")

    async def cancel_order(self, key: OrderKey, reason: str) -> bool:
        """
        Cancel one tracked order and publish the transition.

        Parameters
        ----------
        key : OrderKey
            Venue and intent id of the order.
        reason : str
            Why it is being cancelled, carried on the event.

        Returns
        -------
        bool
            True if the broker confirmed the cancellation.
        """
        tracked = self.orders.get(key)
        if tracked is None:
            return False
        try:
            await self.place_cancellation(broker_cancellation_for(tracked))
        except BrokerError as error:
            logger.error(f"Could not cancel {key}: {error}")
            return False
        self.forget(key)
        await self.publish_order_event(tracked, OrderState.CANCELLED, reason=reason)
        return True

    async def place(self, intent: OrderIntent, ts_oms_recv: int) -> bool:
        """
        Send an intent to the broker and publish what came back.

        Parameters
        ----------
        intent : OrderIntent
            The intent to place.
        ts_oms_recv : int
            When this process read the intent, nanoseconds.

        Returns
        -------
        bool
            True if the venue accepted the order.
        """
        tracked = TrackedOrder(
            intent=intent, state=OrderState.ACCEPTED, remaining=intent.amount
        )
        self.orders[tracked.key] = tracked
        await self.publish_order_event(tracked, OrderState.ACCEPTED)

        ts_broker_send = now_ns()
        try:
            confirmation = await self.place_order(broker_order_from_intent(intent))
        except BrokerError as error:
            ts_broker_ack = now_ns()
            self.forget(tracked.key)
            await self.publish_order_event(
                tracked, OrderState.REJECTED, reason=str(error)
            )
            await self.publish_latency(
                intent, ts_oms_recv, ts_broker_send, ts_broker_ack, None
            )
            return False

        ts_broker_ack = now_ns()
        tracked.venue_order_id = str(confirmation.get("id", ""))
        await self.publish_order_event(tracked, OrderState.OPEN)
        if replaces(intent):
            self.resting[intent.strategy] = tracked.key
        elif intent.order_type is OrderKind.MARKET:
            # A market order never rests and cannot be cancelled, so keeping
            # it in the book would only grow it. Whether it filled is the
            # order watcher's report on ``oms:events``, not ours.
            self.forget(tracked.key)
        await self.publish_latency(
            intent,
            ts_oms_recv,
            ts_broker_send,
            ts_broker_ack,
            venue_ack_ns(confirmation),
        )
        return True

    async def reject(self, intent: OrderIntent, reason: str) -> None:
        """
        Publish a rejection for an intent that never reached a venue.

        Parameters
        ----------
        intent : OrderIntent
            The intent.
        reason : str
            Why it was rejected.
        """
        logger.warning(f"Rejecting {intent.intent_id}: {reason}")
        await self.publish_order_event(
            TrackedOrder(
                intent=intent, state=OrderState.REJECTED, remaining=intent.amount
            ),
            OrderState.REJECTED,
            reason=reason,
        )

    def forget(self, key: OrderKey) -> None:
        """
        Drop an order from the book and from its strategy's resting slot.

        Parameters
        ----------
        key : OrderKey
            Venue and intent id of the order.
        """
        tracked = self.orders.pop(key, None)
        if tracked is not None and self.resting.get(tracked.intent.strategy) == key:
            del self.resting[tracked.intent.strategy]

    # Order events ----------------------------------------------------------

    async def publish_order_event(
        self, tracked: TrackedOrder, state: OrderState, reason: str | None = None
    ) -> None:
        """
        Publish a state transition of an order.

        Parameters
        ----------
        tracked : TrackedOrder
            The order.
        state : OrderState
            The new state.
        reason : str | None
            Rejection or cancellation reason.
        """
        tracked.state = state
        intent = tracked.intent
        await self.publisher.publish(
            self.redis,
            OrderEvent(
                ts_recv=now_ns(),
                intent_id=intent.intent_id,
                strategy=intent.strategy,
                venue=intent.venue,
                symbol=intent.symbol,
                state=state,
                side=intent.side,
                venue_order_id=tracked.venue_order_id,
                filled=tracked.filled,
                remaining=tracked.remaining,
                avg_price=tracked.avg_price,
                reason=reason,
                tags=dict(intent.tags),
            ),
        )

    async def publish_latency(
        self,
        intent: OrderIntent,
        ts_oms_recv: int,
        ts_broker_send: int,
        ts_broker_ack: int,
        ts_venue_ack: int | None,
    ) -> None:
        """
        Publish the timing of one intent.

        ``ts_broker_send`` is when the request left this process for the
        broker rather than when the broker reached the venue: the broker
        speaks a request-response pubsub protocol that carries no timing
        back. The difference is the broker's own queueing, which shows up
        inside ``ts_broker_ack`` instead.

        Parameters
        ----------
        intent : OrderIntent
            The intent.
        ts_oms_recv : int
            When this process read the intent.
        ts_broker_send : int
            When the request was published to the broker.
        ts_broker_ack : int
            When the broker's reply arrived.
        ts_venue_ack : int | None
            Venue-reported acceptance time, if any.
        """
        await self.publisher.publish(
            self.redis,
            LatencyRecord(
                ts_recv=ts_broker_ack,
                intent_id=intent.intent_id,
                venue=intent.venue,
                ts_created=intent.ts_recv,
                ts_oms_recv=ts_oms_recv,
                ts_broker_send=ts_broker_send,
                ts_broker_ack=ts_broker_ack,
                ts_venue_ack=ts_venue_ack,
            ),
        )

    def apply_order_event(self, event: OrderEvent) -> None:
        """
        Update the book from an order event the venue produced.

        The order watcher is the only source of truth for what happened to
        an order after it was placed. Without this, an order that filled
        would stay in the book and be cancelled at shutdown, and a strategy's
        resting slot would point at an order that no longer exists.

        Parameters
        ----------
        event : OrderEvent
            The event.
        """
        key: OrderKey = (event.venue, event.intent_id)
        tracked = self.orders.get(key)
        if tracked is None:
            return
        tracked.state = event.state
        tracked.filled = event.filled
        tracked.remaining = event.remaining
        if event.avg_price is not None:
            tracked.avg_price = event.avg_price
        if event.venue_order_id is not None:
            tracked.venue_order_id = event.venue_order_id
        if event.state in TERMINAL_STATES:
            logger.info(f"{event.intent_id} on {event.venue} is {event.state.value}")
            self.forget(key)

    async def follow_order_events(self) -> None:
        """
        Update the book from ``oms:events`` until cancelled.

        Reading starts at the tail as it stands now rather than at ``$``,
        which would re-resolve on every read and drop an event published
        between two of them.
        """
        cursor = await stream_tail(self.redis, ORDER_EVENTS_STREAM)
        while True:
            response = await self.redis.xread(
                {ORDER_EVENTS_STREAM: cursor},
                count=self.config.oms.batch,
                block=self.config.oms.block_ms,
            )
            for _stream, entries in cast(list[Any], response or []):
                for entry_id, fields in entries:
                    cursor = entry_id_str(entry_id)
                    try:
                        event = from_stream_fields(fields)
                    except Exception as error:  # noqa: BLE001, keep following
                        logger.error(f"Undecodable order event {entry_id}: {error}")
                        continue
                    if isinstance(event, OrderEvent):
                        self.apply_order_event(event)

    # Broker ----------------------------------------------------------------

    async def send_to_broker(self, msg: OrderMessage | CancellationMessage) -> Response:
        """
        Send a message to the broker and wait for its reply.

        Parameters
        ----------
        msg : OrderMessage | CancellationMessage
            The message to send.

        Returns
        -------
        Response
            The broker's reply.
        """
        flattened = json.dumps(dict(msg), default=str)
        response: str = ""

        await self.redis.publish(BROKER_CHANNEL, flattened)
        logger.info(
            f"Sending message with id {msg['id']} and type {msg['kind']} to broker."
        )
        async with self.redis.pubsub() as pubsub:
            await pubsub.subscribe(msg["id"])
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = message["data"]
                    response = data.decode() if isinstance(data, bytes) else data
                    logger.debug(
                        f"Received reply from broker for msg {msg['id']}: {response}"
                    )
                    break
            await pubsub.unsubscribe(msg["id"])

        return identify_response(response)

    async def place_order(self, msg: OrderMessage) -> dict[str, Any]:
        """
        Place an order through the broker.

        Parameters
        ----------
        msg : OrderMessage
            The order message.

        Returns
        -------
        dict[str, Any]
            The CCXT order the venue returned.

        Raises
        ------
        BrokerError
            If the broker reports an error or returns something unreadable.
        """
        logger.debug(f"Sending {msg['id']} to broker")
        confirmation: Response = await self.send_to_broker(msg)

        if confirmation.get("kind") != MessageType.ORDER:
            raise BrokerError(
                message=f"Invalid response from broker:{confirmation.get('text')}"
            )
        try:
            order = ast.literal_eval(confirmation["text"])
        except (ValueError, SyntaxError) as error:
            raise BrokerError(
                message=f"Unreadable broker confirmation for {msg['id']}: {error}"
            ) from error
        if not isinstance(order, dict) or "id" not in order:
            raise BrokerError(
                message=f"Broker confirmation for {msg['id']} carries no order id"
            )
        logger.info(
            f"Order with id {msg['id']} was sucessfully placed. "
            f"Exchange id is {order['id']}"
        )
        return cast(dict[str, Any], order)

    async def place_cancellation(self, cancellation: CancellationMessage) -> bool:
        """
        Cancel an order through the broker.

        Parameters
        ----------
        cancellation : CancellationMessage
            The cancellation message.

        Returns
        -------
        bool
            True if the broker confirmed.

        Raises
        ------
        BrokerError
            If the broker reports an error.
        """
        logger.debug(f"Cancelling {cancellation['id']} with broker")
        confirmation: Response = await self.send_to_broker(cancellation)

        if confirmation.get("kind") == MessageType.CANCELLATION:
            logger.info(
                f"Order with id {cancellation['id']} was sucessfully cancelled."
            )
            return True
        raise BrokerError(message=f"Invalid response from broker:{confirmation}")

    # Startup and shutdown --------------------------------------------------

    async def get_open_orders(self) -> dict[str, OrderMessage]:
        """
        Ask the broker for every order still open at the venues.

        Returns
        -------
        dict[str, OrderMessage]
            Open orders keyed by strategy. Unique orders are left out: they
            are placed to execute once and were never tracked.
        """
        open_orders: dict[str, OrderMessage] = {}
        trigger = OrderMessage(
            kind=MessageType.ORDER,
            strategy="INIT",
            exchange="INIT",
            id="0",
            exchange_id="",
            pair="INIT",
            side=OrderSide.SELL,
            order_type=OrderType.UNIQUE,
            price=Decimal(0),
            amount=Decimal(0),
        )
        async with Redis(connection_pool=self.pool) as redis:
            await redis.publish(BROKER_CHANNEL, json.dumps(dict(trigger), default=str))

            logger.info("Asking broker for all open orders.")
            async with redis.pubsub() as pubsub:
                logger.debug("Waiting for answer on channel INIT")
                await pubsub.subscribe("INIT")
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        data = message["data"]
                        response = data.decode() if isinstance(data, bytes) else data
                        logger.debug(f"Received init reply from broker: {response}")
                        order_batch = cast(OrderBatchMessage, parse_message(response))
                        for order in order_batch["orders"]:
                            if order.get("order_type") == OrderType.UNIQUE:
                                continue
                            open_orders[order["strategy"]] = order
                        break
                await pubsub.unsubscribe("INIT")

        return open_orders

    def adopt(self, open_orders: dict[str, OrderMessage]) -> None:
        """
        Put orders recollected at startup into the book.

        No order event is published for them: they were placed before this
        process existed, and announcing them as new transitions would make
        consumers act on orders that have not changed.

        Parameters
        ----------
        open_orders : dict[str, OrderMessage]
            Open orders keyed by strategy.
        """
        for order in open_orders.values():
            tracked = tracked_from_legacy_order(order)
            self.orders[tracked.key] = tracked
            if replaces(tracked.intent):
                self.resting[tracked.intent.strategy] = tracked.key
            logger.info(f"Adopted pre-existing order {tracked.key}")

    async def cancel_all_open(self) -> None:
        """
        Cancel every resting order on the way down.

        Only orders that rest are cancelled. A unique order is placed to
        execute once and is deliberately left alone, which is what the
        engine did before intents existed and what its strategies expect.
        """
        cancellations = [
            asyncio.create_task(self.cancel_order(key, "shutting down"))
            for key in list(self.resting.values())
        ]
        if not cancellations:
            return
        logger.info(f"Winding down, cancelling {len(cancellations)} orders")
        await asyncio.gather(*cancellations, return_exceptions=True)
        logger.info("All open orders sucessfully cancelled")

    async def collect_results_periodically(self) -> None:
        """Drop finished intent tasks so the list does not grow unbounded."""
        while True:
            if self.tasks:
                _done, pending = await asyncio.wait(
                    self.tasks, timeout=1, return_when=asyncio.FIRST_COMPLETED
                )
                self.tasks = list(pending)
            await asyncio.sleep(2)

    async def run(self) -> None:
        """Consume intents until cancelled."""
        await self.ensure_group()
        logger.info(
            f"Order manager reading {INTENTS_STREAM} as "
            f"{OMS_CONSUMER_GROUP}/{self.config.oms.consumer}"
        )
        while True:
            await self.consume_once()

    async def block_and_shutdown(self, tasks_to_cancel: list[asyncio.Task]) -> None:
        """
        Cancel every running task.

        Parameters
        ----------
        tasks_to_cancel : list[asyncio.Task]
            Long-running tasks to cancel explicitly.
        """
        logger.info("Cancelling all open tasks")
        for task in tasks_to_cancel:
            task.cancel()
        await asyncio.sleep(1)
        for task in self.tasks:
            if not task.done():
                task.cancel()
        logger.info("All open tasks sucessfully closed")


async def main(config: AppConfig) -> None:
    """
    Run the order manager, its legacy bridge and its event follower.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    """
    logger.debug("Order manager starting")
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    manager = OrderManager(config, redis, pool)

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, manager.shutdown_event.set)

    # Let the broker boot before asking it what is open at the venues.
    await asyncio.sleep(2)
    open_orders = await manager.get_open_orders()
    if open_orders:
        logger.info(f"Pre-existing open order were found: {open_orders}")
        manager.adopt(open_orders)

    bridge_publisher = StreamPublisher(maxlen=config.oms.stream_maxlen)
    long_running = [
        asyncio.create_task(manager.run()),
        asyncio.create_task(manager.follow_order_events()),
        asyncio.create_task(legacy_bridge.run(redis, bridge_publisher)),
        asyncio.create_task(manager.collect_results_periodically()),
    ]

    await manager.shutdown_event.wait()
    await manager.cancel_all_open()
    await manager.block_and_shutdown(long_running)
    await asyncio.gather(*long_running, return_exceptions=True)
    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main(load_app_config()))
