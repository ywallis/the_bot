"""Strategy runtime: one event loop over Redis Streams for one strategy.

A strategy written against this module never touches Redis. It subclasses
``Strategy``, overrides the ``on_*`` methods it cares about, and calls
``submit``, ``cancel`` and ``cancel_resting`` on the ``Runtime`` it is handed
at start. The runtime

- derives the streams to read from the strategy's declared ``subscriptions``:
  one book or trade stream per subscribed feed, the balance stream of every
  subscribed venue, and ``oms:events`` filtered down to the strategy's own
  orders;
- runs a single ``XREAD`` over all of them, resolving each tail once and then
  advancing through concrete entry ids, so nothing published between two
  reads is lost;
- drives a ``Clock`` from the ``ts_recv`` of every delivered event, which is
  what makes the same strategy code run live and under replay;
- turns intents into ``XADD``s on ``oms:intents`` under whatever key prefix it
  was given, so a backtest is a prefix and a replay clock away.

The ``async def strategy(redis, config)`` entry point stays valid; this is an
additional way to write a strategy. Non-Python strategies implement the
stream contract directly. See ``docs/design/event-driven-framework.md``
section 8.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from apps.shared.src.config import BOOK_FEED, TRADE_FEED, AppConfig, StrategyConfig
from apps.shared.src.events import (
    ORDER_EVENTS_STREAM,
    AnyEvent,
    BalanceEvent,
    BookEvent,
    CancelIntent,
    OrderEvent,
    OrderIntent,
    OrderKind,
    Side,
    TimeInForce,
    TradeEvent,
    balance_stream,
    book_stream,
    from_stream_fields,
    now_ns,
    prefixed,
    trade_stream,
)
from apps.shared.src.streams import StreamPublisher, entry_id_str, stream_tail

logger = logging.getLogger(__name__)

NS_PER_S = 1_000_000_000
NS_PER_MS = 1_000_000

# Characters the client order id format reserves. ``t-<stamp>_<strategy>_<order>``
# is split on both by the order watcher, so neither identifier may contain them.
RESERVED_ID_CHARS = frozenset("-_")

# Intents remembered so ``cancel`` can rebuild venue and symbol from an id.
SUBMITTED_MEMORY = 10_000


class Clock:
    """
    The time a strategy reasons in.

    Live, ``now`` is the wall clock. Under replay it is the ``ts_recv`` of the
    last event the runtime delivered, so a strategy replayed against a
    recording sees the time the recording saw. Either way ``advance`` is
    called for every delivered event, so ``last_event_ns`` is the age of the
    strategy's information in both modes.

    Attributes
    ----------
    live : bool
        Whether ``now`` reads the wall clock.
    """

    def __init__(self, live: bool = True) -> None:
        """
        Initialize the clock.

        Parameters
        ----------
        live : bool
            True for the wall clock, False for one driven by delivered events.
        """
        self.live = live
        self._last_event_ns: int | None = None

    @classmethod
    def replay(cls) -> "Clock":
        """
        Return a clock driven by delivered events.

        Returns
        -------
        Clock
            The clock. It reads 0 until the first event is delivered.
        """
        return cls(live=False)

    @property
    def last_event_ns(self) -> int | None:
        """
        Return the ``ts_recv`` of the last delivered event.

        Returns
        -------
        int | None
            Nanoseconds, or None before the first event.
        """
        return self._last_event_ns

    def advance(self, ts_recv: int) -> None:
        """
        Record that an event with this receive time was delivered.

        Time never moves backwards: streams are merged by entry id, which
        carries publish jitter, so an event can be delivered after one it was
        received before.

        Parameters
        ----------
        ts_recv : int
            Receive time of the event, nanoseconds.
        """
        if self._last_event_ns is None or ts_recv > self._last_event_ns:
            self._last_event_ns = ts_recv

    def now(self) -> int:
        """
        Return the current time.

        Returns
        -------
        int
            Nanoseconds since the epoch: the wall clock live, the last
            delivered event's receive time under replay.
        """
        if self.live:
            return now_ns()
        return self._last_event_ns or 0


def time_stamp(ts_ns: int) -> str:
    """
    Format a time as the numeric stamp used inside client order ids.

    Parameters
    ----------
    ts_ns : int
        Nanoseconds since the epoch.

    Returns
    -------
    str
        ``yymmddHHMMSSffffff`` in UTC: 18 digits, microsecond resolution.
    """
    seconds, remainder = divmod(ts_ns, NS_PER_S)
    stamp = datetime.fromtimestamp(seconds, UTC).strftime("%y%m%d%H%M%S")
    return f"{stamp}{remainder // 1000:06d}"


def validate_identifier(value: str, what: str) -> str:
    """
    Check that an identifier can travel inside a client order id.

    Parameters
    ----------
    value : str
        The identifier.
    what : str
        What it identifies, for the error message.

    Returns
    -------
    str
        The identifier, unchanged.

    Raises
    ------
    ValueError
        If it is empty or contains a character the id format reserves.
    """
    if not value or RESERVED_ID_CHARS & set(value):
        raise ValueError(
            f"{what} {value!r} cannot be used in an order id: it must be non-empty "
            f"and contain none of {''.join(sorted(RESERVED_ID_CHARS))!r}"
        )
    return value


def strategy_key(identifier: str, order_identifier: str) -> str:
    """
    Return the key intents and order events carry for one order slot.

    Parameters
    ----------
    identifier : str
        Strategy identifier.
    order_identifier : str
        Order slot identifier, e.g. ``es`` for the resting sell.

    Returns
    -------
    str
        ``<identifier>_<order identifier>``, the per-slot key the order
        manager locks and coalesces on.
    """
    return f"{identifier}_{order_identifier}"


def owner_of(event: OrderEvent) -> str:
    """
    Return the identifier of the strategy an order event belongs to.

    Parameters
    ----------
    event : OrderEvent
        The event.

    Returns
    -------
    str
        The strategy identifier, empty for an unattributed order.
    """
    return event.strategy.partition("_")[0]


def snapshot_streams(strategy: StrategyConfig, prefix: str = "") -> list[str]:
    """
    Enumerate the streams whose latest entry is a complete piece of state.

    A balance or book entry supersedes every earlier one, so the last entry
    on the stream is worth delivering to a strategy that starts late. A
    trade or an order event is a fact about a moment and is not.

    Parameters
    ----------
    strategy : StrategyConfig
        The strategy.
    prefix : str
        Key prefix, empty in live trading.

    Returns
    -------
    list[str]
        The balance stream of every subscribed venue, then one book stream
        per subscription listing the book feed. Balances come first so that
        a strategy primed with a book already knows what it can fund.
    """
    venues: list[str] = []
    books: list[str] = []
    for subscription in strategy.subscriptions:
        if subscription.venue not in venues:
            venues.append(subscription.venue)
        if BOOK_FEED in subscription.feeds:
            books.append(book_stream(subscription.venue, subscription.symbol))
    streams = [balance_stream(venue) for venue in venues] + books
    return list(dict.fromkeys(prefixed(prefix, stream) for stream in streams))


def subscribed_streams(strategy: StrategyConfig, prefix: str = "") -> list[str]:
    """
    Enumerate the streams a strategy's runtime reads.

    Parameters
    ----------
    strategy : StrategyConfig
        The strategy.
    prefix : str
        Key prefix, empty in live trading.

    Returns
    -------
    list[str]
        One book or trade stream per subscribed feed, in subscription
        order, then the balance stream of every subscribed venue, then
        ``oms:events``. Prefixed and free of duplicates.
    """
    streams: list[str] = []
    venues: list[str] = []
    for subscription in strategy.subscriptions:
        if BOOK_FEED in subscription.feeds:
            streams.append(book_stream(subscription.venue, subscription.symbol))
        if TRADE_FEED in subscription.feeds:
            streams.append(trade_stream(subscription.venue, subscription.symbol))
        if subscription.venue not in venues:
            venues.append(subscription.venue)
    streams.extend(balance_stream(venue) for venue in venues)
    streams.append(ORDER_EVENTS_STREAM)
    return list(dict.fromkeys(prefixed(prefix, stream) for stream in streams))


def _text(value: Any) -> str:
    """
    Return a Redis reply component as text.

    Parameters
    ----------
    value : Any
        Stream name or entry id from an ``XREAD`` reply, bytes or str.

    Returns
    -------
    str
        The text.
    """
    return entry_id_str(value)


def _decimal(value: Decimal | float | int | str) -> Decimal:
    """
    Convert a quantity to ``Decimal`` without inheriting binary float noise.

    Parameters
    ----------
    value : Decimal | float | int | str
        The quantity.

    Returns
    -------
    Decimal
        The quantity. A float goes through its shortest repr, so ``0.35``
        becomes ``Decimal("0.35")`` rather than its 55-digit expansion.
    """
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


class Strategy:
    """
    Base class for a strategy run by the ``Runtime``.

    Every handler is a no-op here; a strategy overrides the ones it needs.
    Exceptions propagate and end the process: a strategy that cannot keep up
    with its own state must not keep quoting.

    Attributes
    ----------
    timer_interval_s : float | None
        If set, ``on_timer`` is called whenever this much clock time has
        elapsed since the last call. The clock is the runtime's, so under
        replay timers fire by recorded time rather than by wall time.
    """

    timer_interval_s: float | None = None

    async def on_start(self, runtime: "Runtime") -> None:
        """
        Run once before the first read.

        Parameters
        ----------
        runtime : Runtime
            The runtime, which the strategy keeps to submit intents through.
        """

    async def on_book(self, event: BookEvent) -> None:
        """
        Handle a book snapshot.

        Parameters
        ----------
        event : BookEvent
            The event.
        """

    async def on_trade(self, event: TradeEvent) -> None:
        """
        Handle a public trade.

        Parameters
        ----------
        event : TradeEvent
            The event.
        """

    async def on_balance(self, event: BalanceEvent) -> None:
        """
        Handle a balance snapshot.

        Parameters
        ----------
        event : BalanceEvent
            The event.
        """

    async def on_order_event(self, event: OrderEvent) -> None:
        """
        Handle a transition of one of this strategy's own orders.

        Parameters
        ----------
        event : OrderEvent
            The event. Events about other strategies' orders are filtered
            out before this is called.
        """

    async def on_timer(self) -> None:
        """Run when ``timer_interval_s`` of clock time has elapsed."""


class Runtime:
    """
    Read a strategy's streams and dispatch them to its handler.

    Attributes
    ----------
    redis : Any
        A ``redis.asyncio.Redis`` client. Replies may be bytes or str.
    config : AppConfig
        The application configuration.
    strategy : StrategyConfig
        The strategy being run.
    handler : Strategy
        The strategy's code.
    clock : Clock
        The clock the strategy reasons in.
    prefix : str
        Key prefix for every stream read or written, empty live.
    streams : list[str]
        Prefixed streams read, see ``subscribed_streams``.
    cursors : dict[str, str]
        Last entry id delivered per stream, the ``XREAD`` start positions.
    submitted : dict[str, OrderIntent]
        Recently submitted intents by id, bounded by ``SUBMITTED_MEMORY``.
    """

    def __init__(
        self,
        redis: Any,
        config: AppConfig,
        strategy: StrategyConfig,
        handler: Strategy,
        *,
        clock: Clock | None = None,
        prefix: str = "",
        block_ms: int = 1000,
        batch: int = 100,
    ) -> None:
        """
        Initialize the runtime.

        Parameters
        ----------
        redis : Any
            A ``redis.asyncio.Redis`` client.
        config : AppConfig
            The application configuration.
        strategy : StrategyConfig
            The strategy to run.
        handler : Strategy
            The strategy's code.
        clock : Clock | None
            The clock; a live one if omitted.
        prefix : str
            Key prefix, e.g. ``bt:run1``. Empty for live streams.
        block_ms : int
            How long a read waits when no event is available.
        batch : int
            Maximum entries fetched per stream per read.

        Raises
        ------
        ValueError
            If the strategy identifier cannot travel in an order id.
        """
        validate_identifier(strategy.identifier, "Strategy identifier")
        self.redis = redis
        self.config = config
        self.strategy = strategy
        self.handler = handler
        self.clock = clock if clock is not None else Clock()
        self.prefix = prefix
        self.block_ms = block_ms
        self.batch = batch
        self.publisher = StreamPublisher(maxlen=config.oms.stream_maxlen, prefix=prefix)
        self.streams = subscribed_streams(strategy, prefix)
        self.cursors: dict[str, str] = {}
        self.submitted: dict[str, OrderIntent] = {}
        self._last_stamp = 0
        self._cancel_seq = 0
        self._next_timer_ns: int | None = None
        self._stopped = False

    @property
    def identifier(self) -> str:
        """
        Return the strategy identifier.

        Returns
        -------
        str
            The identifier.
        """
        return self.strategy.identifier

    # Intents ---------------------------------------------------------------

    def new_intent_id(self, order_identifier: str) -> str:
        """
        Generate a client order id for one of this strategy's order slots.

        Parameters
        ----------
        order_identifier : str
            The slot, e.g. ``es``.

        Returns
        -------
        str
            ``t-<stamp>_<strategy>_<slot>``. The stamp is the clock time to
            the microsecond, bumped as needed so two ids from this runtime
            never collide even when the clock has not moved, which under
            replay it may not have.

        Raises
        ------
        ValueError
            If the slot identifier cannot travel in an order id.
        """
        validate_identifier(order_identifier, "Order identifier")
        stamp = max(int(time_stamp(self.clock.now())), self._last_stamp + 1)
        self._last_stamp = stamp
        return f"t-{stamp:018d}_{self.identifier}_{order_identifier}"

    def order_intent(
        self,
        *,
        venue: str,
        symbol: str,
        side: Side,
        amount: Decimal | float | str,
        order_identifier: str,
        price: Decimal | float | str | None = None,
        order_type: OrderKind = OrderKind.LIMIT,
        time_in_force: TimeInForce = TimeInForce.GTC,
        replace_of: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> OrderIntent:
        """
        Build an order intent stamped by this runtime's clock.

        Parameters
        ----------
        venue : str
            CCXT short id of the venue to trade on.
        symbol : str
            CCXT symbol.
        side : Side
            Buy or sell.
        amount : Decimal | float | str
            Size in base asset.
        order_identifier : str
            The order slot; it names the strategy key the order manager
            locks and coalesces on, and goes into the client order id.
        price : Decimal | float | str | None
            Limit price. None for a market order.
        order_type : OrderKind
            Limit or market.
        time_in_force : TimeInForce
            Time in force.
        replace_of : str | None
            Intent id this quote supersedes, ``REPLACE_RESTING`` for a quote
            with nothing to name yet, or None for an independent order.
        tags : dict[str, str] | None
            Free-form labels carried through to order events.

        Returns
        -------
        OrderIntent
            The intent, not yet submitted.
        """
        return OrderIntent(
            ts_recv=self.clock.now(),
            intent_id=self.new_intent_id(order_identifier),
            strategy=strategy_key(self.identifier, order_identifier),
            venue=venue,
            symbol=symbol,
            side=side,
            order_type=order_type,
            amount=_decimal(amount),
            price=None if price is None else _decimal(price),
            time_in_force=time_in_force,
            replace_of=replace_of,
            tags=dict(tags or {}),
        )

    async def submit(self, intent: OrderIntent) -> OrderIntent:
        """
        Publish an order intent to ``oms:intents``.

        Parameters
        ----------
        intent : OrderIntent
            The intent, usually built by ``order_intent``.

        Returns
        -------
        OrderIntent
            The same intent, for callers that keep it as their resting order.
            Intents are remembered by id, so two legs that share an id across
            venues leave only the last one reachable through ``cancel``.
        """
        await self.publisher.publish(self.redis, intent)
        self.submitted[intent.intent_id] = intent
        while len(self.submitted) > SUBMITTED_MEMORY:
            del self.submitted[next(iter(self.submitted))]
        logger.debug(f"Submitted {intent.intent_id}")
        return intent

    async def cancel(self, intent_id: str) -> CancelIntent:
        """
        Publish a cancellation for an order this runtime submitted.

        Parameters
        ----------
        intent_id : str
            Id of the intent to cancel.

        Returns
        -------
        CancelIntent
            The published cancel intent.

        Raises
        ------
        KeyError
            If the id was not submitted through this runtime, or was
            submitted longer ago than ``SUBMITTED_MEMORY`` intents.
        """
        target = self.submitted[intent_id]
        return await self._publish_cancel(
            target.venue, target.symbol, target.strategy, intent_id
        )

    async def cancel_resting(
        self, venue: str, symbol: str, order_identifier: str
    ) -> CancelIntent:
        """
        Cancel whatever the order manager has resting for one order slot.

        This is how a strategy cleans up after a restart, when it no longer
        knows the id of the quote it left behind.

        Parameters
        ----------
        venue : str
            CCXT short id.
        symbol : str
            CCXT symbol.
        order_identifier : str
            The slot, e.g. ``es``.

        Returns
        -------
        CancelIntent
            The published cancel intent, with an empty target.
        """
        return await self._publish_cancel(
            venue, symbol, strategy_key(self.identifier, order_identifier), ""
        )

    async def _publish_cancel(
        self, venue: str, symbol: str, strategy: str, target_intent_id: str
    ) -> CancelIntent:
        ts_recv = self.clock.now()
        self._cancel_seq += 1
        intent = CancelIntent(
            ts_recv=ts_recv,
            intent_id=f"cancel-{strategy}-{ts_recv}-{self._cancel_seq}",
            strategy=strategy,
            venue=venue,
            symbol=symbol,
            target_intent_id=target_intent_id,
        )
        await self.publisher.publish(self.redis, intent)
        logger.debug(
            f"Submitted {intent.intent_id} for {target_intent_id or 'resting'}"
        )
        return intent

    # Reading ---------------------------------------------------------------

    def is_mine(self, event: OrderEvent) -> bool:
        """
        Return whether an order event concerns one of this strategy's orders.

        Parameters
        ----------
        event : OrderEvent
            The event.

        Returns
        -------
        bool
            True if the event's strategy key starts with this identifier.
        """
        return owner_of(event) == self.identifier

    async def start(self) -> None:
        """
        Resolve stream tails, run ``on_start``, then prime the strategy's state.

        Tails are resolved before ``on_start`` so that an event published in
        reaction to something the strategy does at start is not missed. The
        latest entry of every snapshot stream (``snapshot_streams``) is then
        delivered as if it had just arrived. Without that a strategy starting
        after the feed handlers, which the orchestrator guarantees, would not
        see a balance until one changed, and a balance changes when an order
        fills, which no quote is sent without a balance to fund it. The first
        live run of the runtime stood still for eight minutes on exactly that.
        """
        self.cursors = {
            stream: await stream_tail(self.redis, stream) for stream in self.streams
        }
        logger.info(f"{self.identifier} reading {self.streams}")
        self._arm_timer()
        await self.handler.on_start(self)
        await self.prime()

    async def prime(self) -> int:
        """
        Deliver the latest entry of every snapshot stream.

        The cursors already sit at those entries, so nothing is delivered
        twice.

        Returns
        -------
        int
            Number of entries delivered.
        """
        delivered = 0
        for stream in snapshot_streams(self.strategy, self.prefix):
            entries = await self.redis.xrevrange(stream, count=1)
            if not entries:
                continue
            entry_id, fields = entries[0]
            try:
                event = from_stream_fields(fields)
            except Exception as error:  # noqa: BLE001, keep priming
                logger.error(f"Undecodable entry {entry_id!r} on {stream}: {error}")
                continue
            await self.dispatch(event)
            delivered += 1
        logger.info(f"{self.identifier} primed with {delivered} snapshots")
        return delivered

    async def step(self) -> int:
        """
        Perform one blocking read and dispatch everything it returned.

        Returns
        -------
        int
            Number of entries delivered.
        """
        response = await self.redis.xread(
            dict(self.cursors), count=self.batch, block=self._block_ms()
        )
        delivered = 0
        # redis-py types the reply as list or dict (RESP3); the client is
        # RESP2 here so it is always the list form.
        for stream, entries in cast(list[Any], response or []):
            stream_name = _text(stream)
            for entry_id, fields in entries:
                self.cursors[stream_name] = _text(entry_id)
                try:
                    event = from_stream_fields(fields)
                except Exception as error:  # noqa: BLE001, keep reading
                    logger.error(
                        f"Undecodable entry {entry_id!r} on {stream_name}: {error}"
                    )
                    continue
                await self.dispatch(event)
                delivered += 1
        await self._fire_timer_if_due()
        return delivered

    async def run(self) -> None:
        """Start, then read and dispatch until ``stop`` is called."""
        await self.start()
        while not self._stopped:
            await self.step()

    def stop(self) -> None:
        """Make ``run`` return after the read in progress."""
        self._stopped = True

    async def dispatch(self, event: AnyEvent) -> None:
        """
        Advance the clock and hand one event to the handler.

        Parameters
        ----------
        event : AnyEvent
            The decoded event. Order events about other strategies and
            event types the handler has no hook for are dropped here.
        """
        self.clock.advance(event.ts_recv)
        match event:
            case BookEvent():
                await self.handler.on_book(event)
            case TradeEvent():
                await self.handler.on_trade(event)
            case BalanceEvent():
                await self.handler.on_balance(event)
            case OrderEvent():
                if self.is_mine(event):
                    await self.handler.on_order_event(event)

    # Timer -----------------------------------------------------------------

    def _interval_ns(self) -> int | None:
        interval = self.handler.timer_interval_s
        if interval is None:
            return None
        return int(interval * NS_PER_S)

    def _arm_timer(self) -> None:
        interval = self._interval_ns()
        if interval is not None:
            self._next_timer_ns = self.clock.now() + interval

    def _block_ms(self) -> int:
        """
        Return how long the next read may block.

        Returns
        -------
        int
            ``block_ms``, shortened live so a due timer is not held up by an
            idle read. Under replay the clock does not move while idle, so
            waiting shorter would gain nothing.
        """
        if self._next_timer_ns is None or not self.clock.live:
            return self.block_ms
        remaining_ms = (self._next_timer_ns - self.clock.now()) // NS_PER_MS
        return max(1, min(self.block_ms, remaining_ms))

    async def _fire_timer_if_due(self) -> None:
        interval = self._interval_ns()
        if interval is None or self._next_timer_ns is None:
            return
        now = self.clock.now()
        if now >= self._next_timer_ns:
            self._next_timer_ns = now + interval
            await self.handler.on_timer()


async def run_strategy(
    redis: Any,
    config: AppConfig,
    strategy: StrategyConfig,
    handler: Strategy,
    **options: Any,
) -> None:
    """
    Run a strategy on a runtime until cancelled.

    Parameters
    ----------
    redis : Any
        A ``redis.asyncio.Redis`` client.
    config : AppConfig
        The application configuration.
    strategy : StrategyConfig
        The strategy to run.
    handler : Strategy
        The strategy's code.
    **options : Any
        Keyword options for ``Runtime``: ``clock``, ``prefix``, ``block_ms``,
        ``batch``.
    """
    await Runtime(redis, config, strategy, handler, **options).run()
