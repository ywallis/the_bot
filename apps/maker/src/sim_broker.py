"""Simulated broker: the venue side of a backtest.

Under a backtest prefix this one process stands in for the order manager,
the broker, the order watcher and the balance watcher. It reads the replayed
books and trades, the replayed opening balances and the strategies' intents,
and publishes what the live system would have published: ``OrderEvent``s to
``oms:events``, a ``LatencyRecord`` per placement to ``oms:latency`` and a
``BalanceEvent`` on every change of a venue's balances. Strategies and the
matcher run unchanged against it.

What it reproduces of the order manager (section 6 of the design), because
the strategies were written against exactly this behaviour:

- one resting order per strategy key, a superseding intent cancelling what
  rests before it places, the cancel and the placement serialised per key,
  and a quote arriving while its key is busy coalescing with the one already
  waiting, the older rejected as superseded;
- ``ACCEPTED`` when the request leaves for the broker, ``OPEN`` when the
  reply comes back, ``CANCELLED`` when a cancellation is confirmed, and the
  fill states as the order watcher would report them, each stamped with the
  time the live process would have published it.

What it simulates of the venue, and how honestly:

- **Latency** is drawn per placement from the measured records of the venue
  (``latency.py``): the leg to the order manager, the leg to the broker and
  the round trip. An order reaches the venue half a round trip after it is
  sent, a report of a fill reaches the bus half a round trip after the fill.
- **A crossing order** fills against the recorded book as it stood when the
  order reached the venue, level by level, at taker fee. A market order that
  exhausts the recorded depth fills the rest at the worst recorded level and
  the report counts it.
- **A resting order** queues behind the size the recorded book showed at its
  price when it arrived, never more than the size shown since. A recorded
  trade at its price consumes that queue first and fills it with what is
  left; a trade through its price fills it whole, since price-time priority
  means its level was taken; a book whose far side crosses its price fills
  it whole at its own price.
- **The recording does not react.** Simulated fills consume no liquidity the
  recorded market had, and the other participants never see the simulated
  order. Every number here is an upper bound on what an order of that size
  would have done.

Causality is the coordination in section 9: with ``--follow`` the broker
processes a market event only once every followed strategy has seen it, so
an intent created before that event, in recorded time, is on the bus before
the broker acts on the event. Without ``--follow`` it processes whatever has
arrived, which is fine for a paced replay and wrong for an unpaced one.
"""

import argparse
import asyncio
import heapq
import json
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.latency import LatencyModel, LatencySample, latency_records
from apps.maker.src.message_processor import TERMINAL_STATES, OrderKey, replaces
from apps.maker.src.order_watcher import ORDER_TAG, STRATEGY_TAG
from apps.shared.src.config import (
    BOOK_FEED,
    TRADE_FEED,
    AppConfig,
    FeeConfig,
    load_app_config,
)
from apps.shared.src.events import (
    INTENTS_STREAM,
    AnyEvent,
    AssetBalance,
    BalanceEvent,
    BookEvent,
    CancelIntent,
    Fill,
    LatencyRecord,
    Liquidity,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    TimeInForce,
    TradeEvent,
    backtest_prefix,
    balance_stream,
    book_stream,
    prefixed,
    trade_stream,
)
from apps.shared.src.runtime import StreamReader
from apps.shared.src.streams import (
    StreamPublisher,
    read_frontier,
    read_progress,
    replay_broker_key,
    replay_closed_key,
    replay_done_key,
    replay_progress_key,
)
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

NS_PER_S = 1_000_000_000
# The broker's name under the replay progress keys.
PROCESS_NAME = "sim"
# Backtest streams are never trimmed: a consumer that falls behind must still
# find every event when it gets there, and a run is bounded by its range.
UNTRIMMED = 1 << 40
# Processing order for events with the same time: the market moved first,
# then the strategy reacted to it.
MARKET_FIRST = 0
INTENT_SECOND = 1
# How often to poll the followed strategies' progress while ahead of them.
FOLLOW_POLL_S = 0.005

ZERO = Decimal(0)


def _decimal(value: float | Decimal | str) -> Decimal:
    """
    Convert a market data float to a Decimal without binary noise.

    Parameters
    ----------
    value : float | Decimal | str
        The value.

    Returns
    -------
    Decimal
        ``Decimal(str(value))`` for a float, so 0.35 stays 0.35.
    """
    return value if isinstance(value, Decimal) else Decimal(str(value))


def split_strategy_key(key: str) -> tuple[str, str]:
    """
    Split a strategy key into the identifier and order slot it names.

    Parameters
    ----------
    key : str
        ``<identifier>_<slot>``, or a bare identifier for an order with no
        slot, such as the matcher's hedges.

    Returns
    -------
    tuple[str, str]
        Identifier and slot, the slot empty when there is none.
    """
    identifier, _, slot = key.partition("_")
    return identifier, slot


# Market state -----------------------------------------------------------------


class Market:
    """
    One venue's recorded market for one symbol, with a short history.

    Attributes
    ----------
    latest : BookEvent | None
        The most recent book.
    history : deque[BookEvent]
        Recent books, oldest first, so an order arriving at the venue a
        fraction of a second after the broker last heard is matched against
        the book of its arrival time rather than of the broker's clock.
    """

    def __init__(self, history_ns: int) -> None:
        """
        Initialize an empty market.

        Parameters
        ----------
        history_ns : int
            How far back books are kept.
        """
        self.history_ns = history_ns
        self.latest: BookEvent | None = None
        self.history: deque[BookEvent] = deque()

    def record(self, book: BookEvent) -> None:
        """
        Note a new book.

        Parameters
        ----------
        book : BookEvent
            The book.
        """
        self.latest = book
        self.history.append(book)
        horizon = book.ts_recv - self.history_ns
        while len(self.history) > 1 and self.history[0].ts_recv < horizon:
            self.history.popleft()

    def book_at(self, ts: int) -> BookEvent | None:
        """
        Return the book in force at a time.

        Parameters
        ----------
        ts : int
            Nanoseconds since the epoch.

        Returns
        -------
        BookEvent | None
            The latest book received at or before the time, the oldest kept
            if the time predates the history, None if nothing was received.
        """
        chosen = None
        for book in reversed(self.history):
            if book.ts_recv <= ts:
                return book
            chosen = book
        return chosen


def best(book: BookEvent, side: Side) -> Decimal | None:
    """
    Return the best price on one side of a book.

    Parameters
    ----------
    book : BookEvent
        The book.
    side : Side
        ``BUY`` for the best bid, ``SELL`` for the best ask.

    Returns
    -------
    Decimal | None
        The price, or None for an empty side.
    """
    levels = book.bids if side is Side.BUY else book.asks
    return _decimal(levels[0][0]) if levels else None


def size_at(book: BookEvent, side: Side, price: Decimal) -> Decimal | None:
    """
    Return the size a book shows at a price on one side.

    Parameters
    ----------
    book : BookEvent
        The book.
    side : Side
        The side the order rests on: a sell rests among the asks.
    price : Decimal
        The price level.

    Returns
    -------
    Decimal | None
        The size, or None if the level is not in the book.
    """
    levels = book.asks if side is Side.SELL else book.bids
    for level_price, level_size in levels:
        if _decimal(level_price) == price:
            return _decimal(level_size)
    return None


def crossing_fills(
    book: BookEvent, side: Side, amount: Decimal, limit: Decimal | None
) -> tuple[list[tuple[Decimal, Decimal]], Decimal]:
    """
    Match an incoming order against the opposite side of a book.

    Parameters
    ----------
    book : BookEvent
        The book at arrival.
    side : Side
        Side of the incoming order.
    amount : Decimal
        Its size.
    limit : Decimal | None
        Its limit price, None for a market order.

    Returns
    -------
    tuple[list[tuple[Decimal, Decimal]], Decimal]
        Fills as price and size per level taken, and the size left over
        once the book no longer crosses the limit or runs out of levels.
    """
    levels = book.asks if side is Side.BUY else book.bids
    fills: list[tuple[Decimal, Decimal]] = []
    remaining = amount
    for level_price, level_size in levels:
        price = _decimal(level_price)
        if limit is not None and (
            (side is Side.BUY and price > limit)
            or (side is Side.SELL and price < limit)
        ):
            break
        take = min(remaining, _decimal(level_size))
        if take <= ZERO:
            continue
        fills.append((price, take))
        remaining -= take
        if remaining <= ZERO:
            break
    return fills, remaining


def crosses(book: BookEvent, side: Side, price: Decimal) -> bool:
    """
    Return whether a book's far side has reached a resting order's price.

    Parameters
    ----------
    book : BookEvent
        The book.
    side : Side
        Side of the resting order.
    price : Decimal
        Its price.

    Returns
    -------
    bool
        True if the best bid is at or above a resting sell, or the best ask
        at or below a resting buy.
    """
    far = best(book, Side.BUY if side is Side.SELL else Side.SELL)
    if far is None:
        return False
    return far >= price if side is Side.SELL else far <= price


# Orders and balances ------------------------------------------------------------


@dataclass
class SimOrder:
    """
    An order the simulated venue knows about.

    Attributes
    ----------
    intent : OrderIntent
        The intent that created it.
    venue_order_id : str
        The id the simulated venue assigned.
    latency : LatencySample
        The delays drawn for its placement.
    ts_oms_recv : int
        When the order manager read the intent.
    ts_broker_send : int
        When the request left for the broker.
    state : OrderState
        Last state reached, as the venue sees it.
    filled : Decimal
        Cumulative filled size.
    remaining : Decimal
        Size still open.
    cost : Decimal
        Sum of price times size over fills, for the average price.
    fee : Decimal
        Fees charged so far, in ``fee_currency``.
    fee_currency : str
        Asset the fee is charged in, the one received.
    queue_ahead : Decimal | None
        Size ahead of the order at its price, None while it does not rest.
    held : Decimal
        Balance held for the open part, in ``held_asset``.
    held_asset : str
        Asset held.
    done : bool
        True once the venue can do nothing more with it.
    rejected : str | None
        Why the venue refused it, if it did.
    reported : bool
        Whether a terminal event has been published for it.
    """

    intent: OrderIntent
    venue_order_id: str
    latency: LatencySample
    ts_oms_recv: int
    ts_broker_send: int
    state: OrderState = OrderState.ACCEPTED
    filled: Decimal = ZERO
    remaining: Decimal = ZERO
    cost: Decimal = ZERO
    fee: Decimal = ZERO
    fee_currency: str = ""
    queue_ahead: Decimal | None = None
    held: Decimal = ZERO
    held_asset: str = ""
    done: bool = False
    rejected: str | None = None
    reported: bool = False

    @property
    def key(self) -> OrderKey:
        """
        Return the venue and intent id.

        Returns
        -------
        OrderKey
            The order manager's book key.
        """
        return (self.intent.venue, self.intent.intent_id)

    @property
    def avg_price(self) -> Decimal | None:
        """
        Return the average fill price.

        Returns
        -------
        Decimal | None
            Cost over filled size, None before any fill.
        """
        return None if self.filled <= ZERO else self.cost / self.filled

    @property
    def resting(self) -> bool:
        """
        Return whether the order sits in the venue's book.

        Returns
        -------
        bool
            True once accepted to rest and until done.
        """
        return self.queue_ahead is not None and not self.done


class Balances:
    """
    Simulated balances per venue, adopted from the recording then kept here.

    Attributes
    ----------
    venues : dict[str, dict[str, list[Decimal]]]
        Per venue and asset, ``[free, used]``.
    adopted : set[str]
        Venues whose opening balance has been taken from the recording.
    """

    def __init__(self) -> None:
        """Initialize with no venue known."""
        self.venues: dict[str, dict[str, list[Decimal]]] = {}
        self.adopted: set[str] = set()
        self.opening: dict[str, dict[str, Decimal]] = {}

    def adopt(self, event: BalanceEvent) -> bool:
        """
        Take a venue's opening balance from a recorded snapshot, once.

        Parameters
        ----------
        event : BalanceEvent
            The recorded snapshot.

        Returns
        -------
        bool
            True if adopted, False if the venue already had a balance: after
            the first snapshot the simulation owns the numbers, and a later
            recorded one is the live run's fill, not this run's.
        """
        if event.venue in self.adopted:
            return False
        self.adopted.add(event.venue)
        self.venues[event.venue] = {
            asset: [_decimal(balance.free), _decimal(balance.used)]
            for asset, balance in event.balances.items()
        }
        self.opening[event.venue] = {
            asset: _decimal(balance.total) for asset, balance in event.balances.items()
        }
        return True

    def _entry(self, venue: str, asset: str) -> list[Decimal]:
        assets = self.venues.setdefault(venue, {})
        return assets.setdefault(asset, [ZERO, ZERO])

    def hold(self, venue: str, asset: str, amount: Decimal) -> None:
        """
        Move an amount from free to used.

        Parameters
        ----------
        venue : str
            Venue id.
        asset : str
            Asset.
        amount : Decimal
            Amount to hold. Free may go negative: the strategy is the one
            judging solvency, and a simulation that silently refused would
            hide a strategy sizing past its balance.
        """
        entry = self._entry(venue, asset)
        entry[0] -= amount
        entry[1] += amount

    def release(self, venue: str, asset: str, amount: Decimal) -> None:
        """
        Move an amount from used back to free.

        Parameters
        ----------
        venue : str
            Venue id.
        asset : str
            Asset.
        amount : Decimal
            Amount to release.
        """
        entry = self._entry(venue, asset)
        entry[1] -= amount
        entry[0] += amount

    def spend_held(self, venue: str, asset: str, amount: Decimal) -> None:
        """
        Consume a held amount: it leaves the venue.

        Parameters
        ----------
        venue : str
            Venue id.
        asset : str
            Asset.
        amount : Decimal
            Amount consumed from ``used``.
        """
        self._entry(venue, asset)[1] -= amount

    def credit(self, venue: str, asset: str, amount: Decimal) -> None:
        """
        Add an amount to free.

        Parameters
        ----------
        venue : str
            Venue id.
        asset : str
            Asset.
        amount : Decimal
            Amount received.
        """
        self._entry(venue, asset)[0] += amount

    def snapshot(self, venue: str, ts_recv: int, seq: int) -> BalanceEvent:
        """
        Build the balance event the balance watcher would publish.

        Parameters
        ----------
        venue : str
            Venue id.
        ts_recv : int
            Time of the snapshot.
        seq : int
            Sequence number.

        Returns
        -------
        BalanceEvent
            The snapshot.
        """
        return BalanceEvent(
            ts_recv=ts_recv,
            venue=venue,
            seq=seq,
            ts_exch=None,
            balances={
                asset: AssetBalance(
                    free=float(free), used=float(used), total=float(free + used)
                )
                for asset, (free, used) in sorted(self.venues.get(venue, {}).items())
            },
        )

    def totals(self, venue: str) -> dict[str, Decimal]:
        """
        Return the current total per asset of a venue.

        Parameters
        ----------
        venue : str
            Venue id.

        Returns
        -------
        dict[str, Decimal]
            Free plus used per asset.
        """
        return {
            asset: free + used
            for asset, (free, used) in self.venues.get(venue, {}).items()
        }


# Report --------------------------------------------------------------------------


@dataclass
class SimReport:
    """
    What a simulated run did.

    Attributes
    ----------
    intents : int
        Order intents read.
    cancels : int
        Cancel intents read.
    placed : int
        Orders that reached the venue and were accepted.
    rejected : int
        Intents refused, superseded in the queue or post-only crossing.
    cancelled : int
        Cancellations confirmed.
    fills : int
        Fill events, one per level or trade that filled something.
    depth_exhausted : int
        Market orders that outran the recorded depth.
    volume : dict[str, Decimal]
        Base filled per venue.
    fees : dict[str, Decimal]
        Fees charged per ``venue:asset``.
    latency : dict[str, Any]
        The latency model's summary.
    opening : dict[str, dict[str, str]]
        Opening totals per venue and asset.
    closing : dict[str, dict[str, str]]
        Closing totals per venue and asset.
    """

    intents: int = 0
    cancels: int = 0
    placed: int = 0
    rejected: int = 0
    cancelled: int = 0
    fills: int = 0
    depth_exhausted: int = 0
    volume: dict[str, Decimal] = field(default_factory=dict)
    fees: dict[str, Decimal] = field(default_factory=dict)
    latency: dict[str, Any] = field(default_factory=dict)
    opening: dict[str, dict[str, str]] = field(default_factory=dict)
    closing: dict[str, dict[str, str]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """
        Return the report as plain JSON-able data.

        Returns
        -------
        dict[str, Any]
            Decimals as strings.
        """
        return {
            "intents": self.intents,
            "cancels": self.cancels,
            "placed": self.placed,
            "rejected": self.rejected,
            "cancelled": self.cancelled,
            "fills": self.fills,
            "depth_exhausted": self.depth_exhausted,
            "volume": {k: str(v) for k, v in sorted(self.volume.items())},
            "fees": {k: str(v) for k, v in sorted(self.fees.items())},
            "latency": self.latency,
            "opening": self.opening,
            "closing": self.closing,
        }


# The broker ------------------------------------------------------------------


Action = Callable[[int], Awaitable[None]]


@dataclass(order=True)
class _Scheduled:
    ts: int
    seq: int
    action: Action = field(compare=False)


@dataclass(order=True)
class _Buffered:
    ts: int
    kind: int
    seq: int
    event: AnyEvent = field(compare=False)


class SimulatedBroker:
    """
    The venue side of a backtest, driven by the replayed prefix.

    Attributes
    ----------
    redis : Any
        A ``redis.asyncio.Redis`` client.
    config : AppConfig
        The application configuration.
    prefix : str
        The backtest prefix.
    latency : LatencyModel
        Per-venue latency to draw from.
    follow : list[str]
        Strategy identifiers whose progress gates market events.
    clock : int
        Simulated time, nanoseconds: the time of the last thing processed.
    orders : dict[OrderKey, SimOrder]
        Orders placed and not yet forgotten, the order manager's book.
    resting : dict[str, OrderKey]
        The order each strategy key has resting.
    report : SimReport
        Counters for the report.
    """

    def __init__(
        self,
        redis: Any,
        config: AppConfig,
        prefix: str,
        latency: LatencyModel,
        *,
        follow: Iterable[str] = (),
        name: str = PROCESS_NAME,
        batch: int = 500,
        block_ms: int = 100,
    ) -> None:
        """
        Initialize the broker.

        Parameters
        ----------
        redis : Any
            A ``redis.asyncio.Redis`` client.
        config : AppConfig
            The application configuration.
        prefix : str
            The backtest prefix, e.g. ``bt:run1``.
        latency : LatencyModel
            Per-venue latency, with a model for every venue traded.
        follow : Iterable[str]
            Strategy identifiers to stay behind; empty to process freely.
        name : str
            This process's name under the progress keys.
        batch : int
            Entries per stream per read.
        block_ms : int
            How long a read waits when nothing is available.

        Raises
        ------
        ValueError
            If the prefix is empty.
        """
        if not prefix:
            raise ValueError("A simulated broker needs a backtest prefix")
        self.redis = redis
        self.config = config
        self.prefix = prefix
        self.latency = latency
        self.follow = list(follow)
        self.name = name
        self.block_ms = block_ms
        self.publisher = StreamPublisher(maxlen=UNTRIMMED, prefix=prefix)
        self.markets: dict[tuple[str, str], Market] = {}
        self.balances = Balances()
        self.orders: dict[OrderKey, SimOrder] = {}
        self.resting: dict[str, OrderKey] = {}
        self.busy: set[str] = set()
        self.waiting_cancels: dict[str, deque[Action]] = {}
        self.queued: dict[str, tuple[OrderIntent, int]] = {}
        self.timeline: list[_Scheduled] = []
        self.buffer: list[_Buffered] = []
        self.clock = 0
        self.report = SimReport()
        self._seq = 0
        self._order_ids = 0
        self._last_intent_wall = time.monotonic()
        self.reader = StreamReader(
            redis,
            [prefixed(prefix, s) for s in self.streams()],
            batch=batch,
            from_start=True,
        )

    # Configuration -------------------------------------------------------------

    def streams(self) -> list[str]:
        """
        Enumerate the unprefixed streams the broker reads.

        Returns
        -------
        list[str]
            One book and trade stream per configured feed, the balance
            stream of every venue, and ``oms:intents``.
        """
        streams: list[str] = []
        for venue, symbol in sorted(self.config.feed_pairs(BOOK_FEED, production)):
            streams.append(book_stream(venue, symbol))
        for venue, symbol in sorted(self.config.feed_pairs(TRADE_FEED, production)):
            streams.append(trade_stream(venue, symbol))
        for venue in self.config.venues:
            streams.append(balance_stream(venue.id))
        streams.append(INTENTS_STREAM)
        return streams

    def fees(self, venue: str) -> FeeConfig:
        """
        Return a venue's fee schedule.

        Parameters
        ----------
        venue : str
            Venue id.

        Returns
        -------
        FeeConfig
            The configured fees, or free trading for a venue without any.
        """
        return self.config.backtest.fees.get(venue, FeeConfig())

    def market(self, venue: str, symbol: str) -> Market:
        """
        Return the market of a venue and symbol, creating it on first use.

        Parameters
        ----------
        venue : str
            Venue id.
        symbol : str
            Symbol.

        Returns
        -------
        Market
            The market.
        """
        key = (venue, symbol)
        if key not in self.markets:
            self.markets[key] = Market(int(self.config.backtest.history_s * NS_PER_S))
        return self.markets[key]

    # Time --------------------------------------------------------------------------

    def schedule(self, ts: int, action: Action) -> None:
        """
        Run an action once simulated time reaches a moment.

        Parameters
        ----------
        ts : int
            Nanoseconds since the epoch.
        action : Action
            Called with the moment it was scheduled for.
        """
        self._seq += 1
        heapq.heappush(self.timeline, _Scheduled(ts, self._seq, action))

    async def run_until(self, ts: int) -> None:
        """
        Run every scheduled action due at or before a moment, in order.

        Parameters
        ----------
        ts : int
            Nanoseconds since the epoch.
        """
        while self.timeline and self.timeline[0].ts <= ts:
            due = heapq.heappop(self.timeline)
            self.clock = max(self.clock, due.ts)
            await due.action(due.ts)
        self.clock = max(self.clock, ts)

    async def flush(self) -> None:
        """Run every scheduled action, however far ahead."""
        while self.timeline:
            await self.run_until(self.timeline[0].ts)

    # Publishing ---------------------------------------------------------------------

    async def publish(self, event: AnyEvent) -> None:
        """
        Publish an event under the prefix.

        Parameters
        ----------
        event : AnyEvent
            The event, already stamped with its simulated time.
        """
        await self.publisher.publish(self.redis, event)

    async def publish_order_event(
        self,
        order: SimOrder,
        state: OrderState,
        ts: int,
        reason: str | None = None,
        last_fill: Fill | None = None,
    ) -> None:
        """
        Publish a state transition of an order.

        Parameters
        ----------
        order : SimOrder
            The order.
        state : OrderState
            The state to report.
        ts : int
            When the live system would have published it.
        reason : str | None
            Rejection or cancellation reason.
        last_fill : Fill | None
            The fill that caused a fill state.
        """
        intent = order.intent
        identifier, slot = split_strategy_key(intent.strategy)
        tags = {STRATEGY_TAG: identifier, ORDER_TAG: slot, **intent.tags}
        await self.publish(
            OrderEvent(
                ts_recv=ts,
                intent_id=intent.intent_id,
                strategy=intent.strategy,
                venue=intent.venue,
                symbol=intent.symbol,
                state=state,
                side=intent.side,
                venue_order_id=order.venue_order_id,
                filled=order.filled,
                remaining=order.remaining,
                avg_price=order.avg_price,
                last_fill=last_fill,
                reason=reason,
                tags=tags,
            )
        )
        if state in TERMINAL_STATES:
            order.reported = True
            self.forget(order.key)

    async def publish_balance(self, venue: str, ts: int) -> None:
        """
        Publish a venue's balances as the balance watcher would.

        Parameters
        ----------
        venue : str
            Venue id.
        ts : int
            When the watcher would have published.
        """
        seq = self.publisher.next_seq(balance_stream(venue))
        await self.publish(self.balances.snapshot(venue, ts, seq))

    def forget(self, key: OrderKey) -> None:
        """
        Drop an order from the book and from its strategy's resting slot.

        Parameters
        ----------
        key : OrderKey
            Venue and intent id.
        """
        order = self.orders.pop(key, None)
        if order is not None and self.resting.get(order.intent.strategy) == key:
            del self.resting[order.intent.strategy]

    # Intents --------------------------------------------------------------------------

    def draw(self, venue: str) -> LatencySample:
        """
        Draw one placement's delays for a venue.

        Parameters
        ----------
        venue : str
            Venue id.

        Returns
        -------
        LatencySample
            The delays.
        """
        return self.latency.for_venue(venue).draw()

    async def on_order_intent(self, intent: OrderIntent) -> None:
        """
        Hand an order intent to the simulated order manager after its lag.

        Parameters
        ----------
        intent : OrderIntent
            The intent, at the moment the strategy created it.
        """
        self.report.intents += 1
        self._last_intent_wall = time.monotonic()
        lag = self.draw(intent.venue).oms_lag_ns
        self.schedule(
            intent.ts_recv + lag, lambda ts: self.oms_order_intent(intent, ts)
        )

    async def on_cancel_intent(self, intent: CancelIntent) -> None:
        """
        Hand a cancel intent to the simulated order manager after its lag.

        Parameters
        ----------
        intent : CancelIntent
            The intent.
        """
        self.report.cancels += 1
        self._last_intent_wall = time.monotonic()
        lag = self.draw(intent.venue).oms_lag_ns
        self.schedule(
            intent.ts_recv + lag, lambda ts: self.oms_cancel_intent(intent, ts)
        )

    async def oms_order_intent(self, intent: OrderIntent, ts: int) -> None:
        """
        Act on an order intent as the order manager would at its read time.

        Parameters
        ----------
        intent : OrderIntent
            The intent.
        ts : int
            When the order manager read it.
        """
        if not replaces(intent):
            await self.place(intent, ts, ts)
            return
        key = intent.strategy
        if key in self.busy:
            superseded = self.queued.get(key)
            self.queued[key] = (intent, ts)
            if superseded is not None:
                await self.reject(
                    superseded[0], f"superseded by {intent.intent_id}", ts
                )
            return
        self.busy.add(key)
        await self.supersede_then_place(intent, ts, ts)

    async def oms_cancel_intent(self, intent: CancelIntent, ts: int) -> None:
        """
        Act on a cancel intent as the order manager would at its read time.

        Parameters
        ----------
        intent : CancelIntent
            The intent.
        ts : int
            When the order manager read it.
        """
        key = intent.strategy

        async def run(now: int) -> None:
            if intent.target_intent_id:
                target: OrderKey | None = (intent.venue, intent.target_intent_id)
            else:
                target = self.resting.get(key)
            order = None if target is None else self.orders.get(target)
            if order is None:
                logger.warning(f"Cancellation for {key} has no open order to cancel")
                await self.release(key, now)
                return
            await self.cancel(order, "cancelled on request", now, None)

        if key in self.busy:
            self.waiting_cancels.setdefault(key, deque()).append(run)
            return
        self.busy.add(key)
        await run(ts)

    async def supersede_then_place(
        self, intent: OrderIntent, ts_oms_recv: int, now: int
    ) -> None:
        """
        Cancel whatever the intent supersedes, one round trip each, then place.

        Parameters
        ----------
        intent : OrderIntent
            The superseding intent; its strategy key is held busy.
        ts_oms_recv : int
            When the order manager read it.
        now : int
            Simulated time.
        """
        keys: list[OrderKey] = []
        if intent.replace_of:
            keys.append((intent.venue, intent.replace_of))
        resting = self.resting.get(intent.strategy)
        if resting is not None and resting not in keys:
            keys.append(resting)
        open_orders = [self.orders[key] for key in keys if key in self.orders]

        async def place(at: int) -> None:
            await self.place(intent, ts_oms_recv, at)

        continuation: Action = place
        # Cancels run one after the other, as the order manager awaits them
        # in turn; the chain is built back to front.
        for order in reversed(open_orders):
            continuation = self._cancel_then(
                order, f"replaced by {intent.intent_id}", continuation
            )
        await continuation(now)

    def _cancel_then(self, order: SimOrder, reason: str, then: Action) -> Action:
        async def run(now: int) -> None:
            await self.cancel(order, reason, now, then)

        return run

    async def cancel(
        self, order: SimOrder, reason: str, now: int, then: Action | None
    ) -> None:
        """
        Send a cancellation to the venue and act on its confirmation.

        Parameters
        ----------
        order : SimOrder
            The order to cancel.
        reason : str
            Carried on the cancelled event.
        now : int
            When the request leaves for the broker.
        then : Action | None
            What to run once confirmed, else the strategy key is released.
        """
        sample = self.draw(order.intent.venue)
        self.schedule(now + sample.one_way_ns, lambda ts: self.venue_cancel(order, ts))

        async def acked(ts: int) -> None:
            if order.state is OrderState.CANCELLED and not order.reported:
                self.report.cancelled += 1
                await self.publish_order_event(
                    order, OrderState.CANCELLED, ts, reason=reason
                )
            else:
                logger.debug(
                    f"Cancel of {order.key} found it already {order.state.value}"
                )
            if then is None:
                await self.release(order.intent.strategy, ts)
            else:
                await then(ts)

        self.schedule(now + sample.rtt_ns, acked)

    async def venue_cancel(self, order: SimOrder, ts: int) -> None:
        """
        Remove an order from the simulated venue's book, if it is still there.

        Parameters
        ----------
        order : SimOrder
            The order.
        ts : int
            Venue time.
        """
        if order.done:
            return
        order.done = True
        order.state = OrderState.CANCELLED
        if order.held > ZERO:
            self.balances.release(order.intent.venue, order.held_asset, order.held)
            order.held = ZERO
            self.schedule(
                ts + self.draw(order.intent.venue).one_way_ns,
                lambda at: self.publish_balance(order.intent.venue, at),
            )

    async def release(self, key: str, now: int) -> None:
        """
        Release a strategy key and start what waited for it.

        Cancellations that waited on the lock go first, since they were
        already waiting when the queued quote was started, then the quote
        that queued behind them, latest wins.

        Parameters
        ----------
        key : str
            The strategy key.
        now : int
            Simulated time.
        """
        waiting = self.waiting_cancels.get(key)
        if waiting:
            await waiting.popleft()(now)
            return
        self.busy.discard(key)
        queued = self.queued.pop(key, None)
        if queued is not None:
            intent, ts_oms_recv = queued
            self.busy.add(key)
            await self.supersede_then_place(intent, ts_oms_recv, now)

    async def reject(self, intent: OrderIntent, reason: str, ts: int) -> None:
        """
        Publish a rejection for an intent that never reached the venue.

        Parameters
        ----------
        intent : OrderIntent
            The intent.
        reason : str
            Why.
        ts : int
            When.
        """
        self.report.rejected += 1
        order = SimOrder(
            intent, "", self.draw(intent.venue), ts, ts, remaining=intent.amount
        )
        await self.publish_order_event(order, OrderState.REJECTED, ts, reason=reason)

    async def place(self, intent: OrderIntent, ts_oms_recv: int, now: int) -> None:
        """
        Send an intent to the venue and schedule its arrival and reply.

        Parameters
        ----------
        intent : OrderIntent
            The intent.
        ts_oms_recv : int
            When the order manager read it, for the latency record.
        now : int
            When the request leaves for the broker.
        """
        sample = self.draw(intent.venue)
        self._order_ids += 1
        order = SimOrder(
            intent=intent,
            venue_order_id=f"sim-{self._order_ids}",
            latency=sample,
            ts_oms_recv=ts_oms_recv,
            ts_broker_send=now,
            remaining=intent.amount,
        )
        self.orders[order.key] = order
        await self.publish_order_event(order, OrderState.ACCEPTED, now)
        self.schedule(now + sample.one_way_ns, lambda ts: self.venue_arrive(order, ts))
        self.schedule(now + sample.rtt_ns, lambda ts: self.placement_acked(order, ts))

    async def venue_arrive(self, order: SimOrder, ts: int) -> None:
        """
        Match an order against the book of its arrival, then rest or finish it.

        Parameters
        ----------
        order : SimOrder
            The order.
        ts : int
            Venue time.
        """
        intent = order.intent
        book = self.market(intent.venue, intent.symbol).book_at(ts)
        if book is None:
            order.rejected = "no recorded book to match against"
            order.done = True
            return
        limit = intent.price if intent.order_type is OrderKind.LIMIT else None
        fills, remaining = crossing_fills(book, intent.side, intent.amount, limit)
        tif = intent.time_in_force
        if (
            intent.order_type is OrderKind.LIMIT
            and tif is TimeInForce.POST_ONLY
            and fills
        ):
            order.rejected = "post-only order would cross"
            order.done = True
            return
        if tif is TimeInForce.FOK and remaining > ZERO:
            order.done = True
            order.state = OrderState.CANCELLED
            self.schedule(
                ts + order.latency.one_way_ns,
                lambda at: self._report_state(order, at, "fill or kill not fillable"),
            )
            return
        if intent.order_type is OrderKind.MARKET and remaining > ZERO:
            if fills:
                worst = fills[-1][0]
                fills.append((worst, remaining))
                remaining = ZERO
                self.report.depth_exhausted += 1
                logger.warning(
                    f"{intent.intent_id} outran the recorded depth on {intent.venue}"
                )
            else:
                order.rejected = "empty book side"
                order.done = True
                return
        self.report.placed += 1
        order.state = OrderState.OPEN
        for price, amount in fills:
            await self.fill(order, price, amount, Liquidity.TAKER, ts)
        if order.remaining <= ZERO:
            return
        if tif is TimeInForce.IOC:
            order.done = True
            order.state = OrderState.CANCELLED
            self.schedule(
                ts + order.latency.one_way_ns,
                lambda at: self._report_state(
                    order, at, "immediate or cancel remainder"
                ),
            )
            return
        # It rests: hold the balance it ties up and take a place in the queue.
        assert limit is not None
        base, quote = intent.symbol.split("/")
        if intent.side is Side.SELL:
            order.held_asset, order.held = base, order.remaining
        else:
            order.held_asset, order.held = quote, order.remaining * limit
        self.balances.hold(intent.venue, order.held_asset, order.held)
        self.schedule(
            ts + self.draw(intent.venue).one_way_ns,
            lambda at: self.publish_balance(intent.venue, at),
        )
        order.queue_ahead = size_at(book, intent.side, limit) or ZERO

    async def _report_state(self, order: SimOrder, ts: int, reason: str) -> None:
        if not order.reported:
            self.report.cancelled += 1
            await self.publish_order_event(order, order.state, ts, reason=reason)

    async def placement_acked(self, order: SimOrder, ts: int) -> None:
        """
        Act on the broker's reply to a placement.

        Parameters
        ----------
        order : SimOrder
            The order.
        ts : int
            When the reply came back.
        """
        intent = order.intent
        if order.rejected is not None:
            self.report.rejected += 1
            await self.publish_order_event(
                order, OrderState.REJECTED, ts, reason=order.rejected
            )
        else:
            if not order.reported:
                await self.publish_order_event(order, OrderState.OPEN, ts)
            if replaces(intent) and not order.done:
                self.resting[intent.strategy] = order.key
            elif intent.order_type is OrderKind.MARKET:
                self.forget(order.key)
        await self.publish(
            LatencyRecord(
                ts_recv=ts,
                intent_id=intent.intent_id,
                venue=intent.venue,
                ts_created=intent.ts_recv,
                ts_oms_recv=order.ts_oms_recv,
                ts_broker_send=order.ts_broker_send,
                ts_broker_ack=ts,
                ts_venue_ack=order.ts_broker_send + order.latency.one_way_ns,
            )
        )
        if replaces(intent):
            await self.release(intent.strategy, ts)

    # Fills ----------------------------------------------------------------------------

    async def fill(
        self,
        order: SimOrder,
        price: Decimal,
        amount: Decimal,
        liquidity: Liquidity,
        ts: int,
    ) -> None:
        """
        Fill part of an order at the venue and schedule its report.

        Parameters
        ----------
        order : SimOrder
            The order.
        price : Decimal
            Fill price.
        amount : Decimal
            Fill size.
        liquidity : Liquidity
            Maker or taker, which selects the fee.
        ts : int
            Venue time of the fill.
        """
        intent = order.intent
        venue = intent.venue
        base, quote = intent.symbol.split("/")
        fees = self.fees(venue)
        rate = _decimal(fees.maker if liquidity is Liquidity.MAKER else fees.taker)
        remaining_before = order.remaining
        order.filled += amount
        order.remaining -= amount
        order.cost += price * amount
        if intent.side is Side.SELL:
            fee = amount * price * rate
            fee_currency = quote
            if order.held > ZERO:
                released = order.held * amount / remaining_before
                order.held -= released
                self.balances.spend_held(venue, base, released)
            else:
                self.balances.credit(venue, base, -amount)
            self.balances.credit(venue, quote, amount * price - fee)
        else:
            fee = amount * rate
            fee_currency = base
            if order.held > ZERO:
                released = order.held * amount / remaining_before
                order.held -= released
                self.balances.spend_held(venue, quote, released)
                # The hold was at the limit; the fill may be cheaper.
                self.balances.credit(venue, quote, released - amount * price)
            else:
                self.balances.credit(venue, quote, -amount * price)
            self.balances.credit(venue, base, amount - fee)
        order.fee += fee
        order.fee_currency = fee_currency
        self.report.fills += 1
        self.report.volume[venue] = self.report.volume.get(venue, ZERO) + amount
        fee_key = f"{venue}:{fee_currency}"
        self.report.fees[fee_key] = self.report.fees.get(fee_key, ZERO) + fee
        if order.remaining <= ZERO:
            order.done = True
            order.state = OrderState.FILLED
            if order.held > ZERO:
                self.balances.release(venue, order.held_asset, order.held)
                order.held = ZERO
        else:
            order.state = OrderState.PARTIALLY_FILLED
        state = order.state
        last_fill = Fill(
            price=price,
            amount=amount,
            fee=fee,
            fee_currency=fee_currency,
            liquidity=liquidity,
            venue_trade_id=f"sim-t{self.report.fills}",
        )
        report_at = ts + self.draw(venue).one_way_ns

        async def report(at: int) -> None:
            await self.publish_order_event(order, state, at, last_fill=last_fill)
            await self.publish_balance(venue, at)

        self.schedule(report_at, report)

    async def on_book(self, book: BookEvent) -> None:
        """
        Note a book and fill what it crosses or requeue what it shows.

        Parameters
        ----------
        book : BookEvent
            The book.
        """
        self.market(book.venue, book.symbol).record(book)
        for order in self.resting_orders(book.venue, book.symbol):
            intent = order.intent
            assert intent.price is not None
            if crosses(book, intent.side, intent.price):
                await self.fill(
                    order, intent.price, order.remaining, Liquidity.MAKER, book.ts_recv
                )
                continue
            shown = size_at(book, intent.side, intent.price)
            order.queue_ahead = (
                ZERO if shown is None else min(order.queue_ahead or ZERO, shown)
            )

    async def on_trade(self, trade: TradeEvent) -> None:
        """
        Fill resting orders a recorded trade reached.

        Parameters
        ----------
        trade : TradeEvent
            The trade.
        """
        price = _decimal(trade.price)
        amount = _decimal(trade.amount)
        for order in self.resting_orders(trade.venue, trade.symbol):
            intent = order.intent
            assert intent.price is not None and order.queue_ahead is not None
            through = (
                price > intent.price
                if intent.side is Side.SELL
                else price < intent.price
            )
            if through:
                await self.fill(
                    order, intent.price, order.remaining, Liquidity.MAKER, trade.ts_recv
                )
            elif price == intent.price:
                consumed = min(order.queue_ahead, amount)
                order.queue_ahead -= consumed
                left = amount - consumed
                if left > ZERO:
                    await self.fill(
                        order,
                        intent.price,
                        min(order.remaining, left),
                        Liquidity.MAKER,
                        trade.ts_recv,
                    )

    def resting_orders(self, venue: str, symbol: str) -> list[SimOrder]:
        """
        Return the orders resting in one market.

        Parameters
        ----------
        venue : str
            Venue id.
        symbol : str
            Symbol.

        Returns
        -------
        list[SimOrder]
            Resting orders, oldest first.
        """
        return [
            order
            for order in self.orders.values()
            if order.resting
            and order.intent.venue == venue
            and order.intent.symbol == symbol
        ]

    # Main loop ----------------------------------------------------------------------

    async def handle(self, event: AnyEvent) -> None:
        """
        Process one buffered event at its time.

        Parameters
        ----------
        event : AnyEvent
            The event.
        """
        await self.run_until(event.ts_recv)
        match event:
            case BookEvent():
                await self.on_book(event)
            case TradeEvent():
                await self.on_trade(event)
            case BalanceEvent():
                if self.balances.adopt(event):
                    logger.info(
                        f"Opening balance for {event.venue} adopted from the recording"
                    )
            case OrderIntent():
                await self.on_order_intent(event)
            case CancelIntent():
                await self.on_cancel_intent(event)

    async def gate(self) -> int:
        """
        Return the time up to which market events may be processed.

        Returns
        -------
        int
            The slowest followed strategy's progress, or no bound when
            nothing is followed.
        """
        if not self.follow:
            return 1 << 62
        progress = await read_progress(self.redis, self.prefix, self.follow)
        values = [value for value in progress.values() if value is not None]
        if len(values) < len(self.follow):
            return -1
        return min(values)

    async def step(self) -> int:
        """
        Read what arrived, then process everything up to the gate in time order.

        Returns
        -------
        int
            Events processed.
        """
        frontier = await read_frontier(self.redis, self.prefix)
        blocks = await self.reader.read(self.block_ms)
        for stream, entries in blocks:
            for entry_id, event in entries:
                self.reader.advance(stream, entry_id)
                if event is None:
                    continue
                kind = (
                    INTENT_SECOND
                    if isinstance(event, OrderIntent | CancelIntent)
                    else MARKET_FIRST
                )
                self._seq += 1
                heapq.heappush(
                    self.buffer, _Buffered(event.ts_recv, kind, self._seq, event)
                )
        processed = 0
        gate = await self.gate()
        while self.buffer and self.buffer[0].ts <= gate:
            await self.handle(heapq.heappop(self.buffer).event)
            processed += 1
        # Caught up with nothing pending: the frontier is as far as anyone
        # has got, and it is this process's progress too.
        idle = not blocks and not self.buffer
        progress = max(self.clock, frontier) if idle else self.clock
        await self.redis.set(replay_progress_key(self.prefix, self.name), progress)
        return processed

    async def finished(self) -> bool:
        """
        Return whether the run is over: replay done, strategies through, nothing left.

        Returns
        -------
        bool
            True once the done key is set, every followed strategy has
            reached its value, the streams are read to their tails, the
            buffer is empty and no intent has arrived for ``idle_s``.
        """
        done = await self.redis.get(replay_done_key(self.prefix))
        if done is None or self.buffer:
            return False
        if self.follow:
            progress = await read_progress(self.redis, self.prefix, self.follow)
            if any(value is None or value < int(done) for value in progress.values()):
                return False
        if not await self.reader.at_tail():
            return False
        return time.monotonic() - self._last_intent_wall >= self.config.backtest.idle_s

    async def run(self) -> SimReport:
        """
        Run until the backtest is over, then wind down and report.

        Returns
        -------
        SimReport
            The report.
        """
        await self.reader.start()
        await self.redis.set(replay_broker_key(self.prefix), self.name)
        logger.info(f"Simulating {sorted(self.latency.venues)} under {self.prefix}")
        for venue in self.config.venues:
            fees = self.fees(venue.id)
            if venue.id not in self.config.backtest.fees:
                logger.warning(f"No fees configured for {venue.id}: it trades free")
            else:
                logger.info(
                    f"Fees for {venue.id}: maker {fees.maker}, taker {fees.taker}"
                )
        while True:
            processed = await self.step()
            if processed == 0 and await self.finished():
                break
            if processed == 0 and self.follow:
                await asyncio.sleep(FOLLOW_POLL_S)
        await self.shutdown()
        return self.report

    async def shutdown(self) -> None:
        """Run out the timeline, cancel what rests, and complete the report."""
        await self.flush()
        open_orders = [order for order in self.orders.values() if not order.done]
        for order in open_orders:
            key = order.intent.strategy
            if key in self.busy:
                self.waiting_cancels.setdefault(key, deque()).append(
                    self._cancel_then(order, "shutting down", self._release_action(key))
                )
            else:
                self.busy.add(key)
                await self.cancel(order, "shutting down", self.clock, None)
        await self.flush()
        self.report.latency = self.latency.summary()
        self.report.opening = {
            venue: {asset: str(total) for asset, total in sorted(totals.items())}
            for venue, totals in sorted(self.balances.opening.items())
        }
        self.report.closing = {
            venue: {asset: str(total) for asset, total in sorted(totals.items())}
            for venue, totals in sorted(
                (venue, self.balances.totals(venue)) for venue in self.balances.venues
            )
        }
        await self.redis.set(replay_closed_key(self.prefix), self.clock)
        logger.info(f"Simulation over: {json.dumps(self.report.as_dict(), indent=2)}")

    def _release_action(self, key: str) -> Action:
        async def run(ts: int) -> None:
            await self.release(key, ts)

        return run


# Command line ----------------------------------------------------------------------


def parse_assumption(text: str) -> tuple[str, float]:
    """
    Parse a ``VENUE=MS`` assumption.

    Parameters
    ----------
    text : str
        For example ``venue_b=400``.

    Returns
    -------
    tuple[str, float]
        Venue id and round trip in milliseconds.

    Raises
    ------
    argparse.ArgumentTypeError
        If the text is not of that shape.
    """
    venue, sep, ms = text.partition("=")
    if not sep or not venue:
        raise argparse.ArgumentTypeError(f"expected VENUE=MS, got {text!r}")
    try:
        return venue, float(ms)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"round trip must be a number, got {ms!r}"
        ) from None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parse the command line.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, ``sys.argv[1:]`` if omitted.

    Returns
    -------
    argparse.Namespace
        ``run_id``, ``follow``, ``assume_rtt_ms`` (list of venue and ms),
        ``root``, ``seed``, ``report`` and ``name``.
    """
    parser = argparse.ArgumentParser(
        description="Simulate the venues for a backtest run."
    )
    parser.add_argument("run_id", help="backtest run id; streams are under bt:<run_id>")
    parser.add_argument(
        "--follow",
        metavar="NAME",
        action="append",
        default=[],
        help="strategy identifier to stay behind; repeatable, and needed for an unpaced replay",
    )
    parser.add_argument(
        "--assume-rtt-ms",
        metavar="VENUE=MS",
        type=parse_assumption,
        action="append",
        default=[],
        help="round trip to assume for a venue with no measured placements",
    )
    parser.add_argument(
        "--root", type=Path, default=None, help="recording root, for oms:latency"
    )
    parser.add_argument("--seed", type=int, default=0, help="seed of the latency draws")
    parser.add_argument(
        "--report", type=Path, default=None, help="write the report as JSON here"
    )
    parser.add_argument(
        "--name", default=PROCESS_NAME, help="name under the progress keys"
    )
    return parser.parse_args(argv)


def build_latency(
    config: AppConfig, root: Path, assumptions: list[tuple[str, float]], seed: int
) -> LatencyModel:
    """
    Build the latency model for a run from the recording and the assumptions.

    Parameters
    ----------
    config : AppConfig
        The application configuration; every declared venue needs a model.
    root : Path
        Recording root.
    assumptions : list[tuple[str, float]]
        Venue ids with an assumed round trip in milliseconds.
    seed : int
        Seed of the draws.

    Returns
    -------
    LatencyModel
        The model.

    Raises
    ------
    KeyError
        If a declared venue has neither measurements nor an assumption.
    """
    model = LatencyModel(seed=seed)
    for venue, ms in assumptions:
        model.assume(venue, ms)
    added = model.add_records(latency_records(root))
    logger.info(f"Latency model: {added} measured placements, {model.summary()}")
    model.require(venue.id for venue in config.venues)
    return model


async def main(config: AppConfig, args: argparse.Namespace) -> SimReport:
    """
    Run the simulated broker against the configured Redis.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    args : argparse.Namespace
        Parsed command line.

    Returns
    -------
    SimReport
        The report.
    """
    prefix = backtest_prefix(args.run_id)
    root = args.root if args.root is not None else Path(config.recorder.root)
    latency = build_latency(config, root, args.assume_rtt_ms, args.seed)
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=4
    )
    redis = Redis(decode_responses=False, connection_pool=pool)
    broker = SimulatedBroker(
        redis, config, prefix, latency, follow=args.follow, name=args.name
    )
    try:
        report = await broker.run()
    finally:
        await redis.aclose()
    if args.report is not None:
        args.report.write_text(json.dumps(report.as_dict(), indent=2))
        logger.info(f"Report written to {args.report}")
    return report


if __name__ == "__main__":
    asyncio.run(main(load_app_config(), parse_args()))
