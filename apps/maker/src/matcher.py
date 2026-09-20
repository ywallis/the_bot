"""Taker hedge for maker fills.

When an order placed by a matching strategy fills on its maker venue, this
process buys or sells the same quantity on the taker venue to flatten the
position. Its input is ``oms:events`` and its output is an ``OrderIntent`` on
``oms:intents``, so it is an ordinary consumer of the bus rather than a
component wired into the exchange websocket.

Sizing uses the fee schedule published on ``acct:fees``: venues charge fees
in the quote asset, in the base asset, or in whichever side was received,
and the rate depends on the account's volume tier, so the schedule is
fetched periodically rather than fixed in code. A fee charged in base
erodes the asset whose balance the system keeps stable and is compensated
in the hedge; a fee charged in quote comes out of the USDT leg and is left
to the accountant. What a venue actually charged on a fill is checked
against the schedule and a mismatch is logged: the schedule is an input,
not a promise.

Until phase 3 this module also owned the ``watch_orders`` loop and was the
only thing in the system that could see a fill. That half now lives in
``order_watcher.py``, which leaves this file as pure hedging logic and makes
it the natural candidate to move into the strategies repo. See
``docs/design/event-driven-framework.md`` section 6.
"""

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any, cast

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.order_watcher import STRATEGY_TAG
from apps.shared.src.config import AppConfig, load_app_config
from apps.shared.src.events import (
    ORDER_EVENTS_STREAM,
    FeeScheduleEvent,
    Liquidity,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    decode,
    fees_stream,
    from_stream_fields,
    now_ns,
)
from apps.shared.src.fees import (
    base_quote,
    fee_currency_for,
    fee_in_base,
    fees_snapshot_key,
    schedule_for,
)
from apps.shared.src.streams import StreamPublisher, entry_id_str, stream_tail
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# How long startup waits for every venue's fee schedule to be published by
# the fee watcher, and how often it checks. A hedge sized without the
# schedule leaves a fee-sized residual position, so the matcher prefers to
# wait; past the deadline it hedges unadjusted and logs the gap on every
# fill from a venue without a schedule.
SCHEDULE_WAIT_S = 60.0
SCHEDULE_POLL_S = 2.0

# A hedge below the venue's minimum notional is bumped above it by this
# factor: the price can slip below the minimum between sizing and execution,
# and an unhedged position is worse than a slightly oversized hedge.
DUST_MARGIN = Decimal("1.03")

# Decimal places an order amount may carry when the venue does not report
# its precision. The value the matcher quantized to before schedules existed.
FALLBACK_PRECISION = 4

# How far the fee a venue actually reported on a fill may deviate from the
# schedule's rate, in either direction, before the mismatch is logged. A
# mismatch means the configured policy or the account's fee tier moved.
MAGNITUDE_TOLERANCE = Decimal("1.5")

# States whose report can carry size that has filled and is not yet hedged.
# A partial fill is hedged as soon as it is reported rather than when the
# order finishes: live, 161 base units of a quote sat unhedged for 24 seconds
# until the strategy happened to replace the order, and a sweep is exactly
# the moment the other venue is moving. A cancelled or expired order that
# filled in part still leaves whatever of that was not hedged yet.
HEDGEABLE_STATES: frozenset[OrderState] = frozenset(
    {
        OrderState.PARTIALLY_FILLED,
        OrderState.FILLED,
        OrderState.CANCELLED,
        OrderState.EXPIRED,
    }
)

# Orders remembered so a repeated report does not hedge the same size twice.
HEDGE_MEMORY = 1000


class HedgeBook:
    """
    How much of each order's fill the hedges sent so far cover.

    The book counts what was hedged, not what was filled. The two differ
    when a hedge is bumped over the venue's minimum notional: the bump
    covers fill that has not happened yet, and is netted off the next
    report rather than sent again. Live, before this distinction, every
    sub-minimum partial fill of one order was bumped on its own and the
    hedges piled up to many times the position they flattened.

    Attributes
    ----------
    max_size : int
        Orders remembered; the oldest is forgotten once full.
    """

    def __init__(self, max_size: int) -> None:
        """
        Initialize the book.

        Parameters
        ----------
        max_size : int
            Orders remembered.
        """
        self.max_size = max_size
        self._hedged: OrderedDict[tuple[str, str], Decimal] = OrderedDict()
        self._count: dict[tuple[str, str], int] = {}

    def hedged(self, key: tuple[str, str]) -> Decimal:
        """
        Return the fill size the hedges of an order cover so far.

        Parameters
        ----------
        key : tuple[str, str]
            Venue and intent id.

        Returns
        -------
        Decimal
            Covered size, in the units of the filled order, zero for an
            order never seen. It exceeds the size filled when the last
            hedge was bumped over the venue's minimum.
        """
        return self._hedged.get(key, Decimal(0))

    def hedges(self, key: tuple[str, str]) -> int:
        """
        Return how many hedges have gone out for an order.

        Parameters
        ----------
        key : tuple[str, str]
            Venue and intent id.

        Returns
        -------
        int
            The count.
        """
        return self._count.get(key, 0)

    def record(self, key: tuple[str, str], covers: Decimal) -> None:
        """
        Note that one more hedge of an order went out.

        Called once the hedge is published, not when it is decided on: a
        hedge that is priced, sized or published unsuccessfully has not
        covered anything, and recording it early left that size open with
        nothing to hedge it later.

        Parameters
        ----------
        key : tuple[str, str]
            Venue and intent id.
        covers : Decimal
            Fill size this hedge covers, see ``HedgeSize.covers``. Added to
            what earlier hedges covered.
        """
        if key in self._hedged:
            self._hedged.move_to_end(key)
        elif len(self._hedged) >= self.max_size:
            oldest, _ = self._hedged.popitem(last=False)
            self._count.pop(oldest, None)
        self._hedged[key] = self._hedged.get(key, Decimal(0)) + covers
        self._count[key] = self._count.get(key, 0) + 1


@dataclass(frozen=True)
class HedgeSize:
    """
    The size of one hedge, on both sides of it.

    Attributes
    ----------
    quantity : Decimal
        Quantity to trade on the matching venue, quantized to its precision.
    covers : Decimal
        Fill size the hedge flattens, in the units of the filled order.
        Equal to the size hedged, except when the hedge was bumped over the
        matching venue's minimum notional: then it is larger by the bump,
        so the excess is netted off the next fill instead of being hedged
        again.
    """

    quantity: Decimal
    covers: Decimal


def _leg(
    schedule: FeeScheduleEvent | None, symbol: str, side: Side, taker: bool
) -> tuple[Decimal, bool, bool]:
    """
    Return the fee terms of one leg of the hedge.

    Parameters
    ----------
    schedule : FeeScheduleEvent | None
        The schedule of the venue the leg trades on.
    symbol : str
        CCXT symbol.
    side : Side
        Side of the leg.
    taker : bool
        True for the hedge leg, which crosses the spread; False for the
        fill leg, which earned the maker rate.

    Returns
    -------
    tuple[Decimal, bool, bool]
        The rate as a fraction of traded value (zero when unknown), whether
        the fee is charged in the base asset (False when unknown), and
        whether the schedule covers the symbol at all.
    """
    if schedule is None:
        return Decimal(0), False, False
    base, quote = base_quote(symbol)
    in_base = fee_in_base(schedule.fee_currency, side, base, quote)
    entry = schedule_for(schedule, symbol)
    if entry is None:
        return Decimal(0), in_base, False
    rate = entry.taker if taker else entry.maker
    return (rate if rate is not None else Decimal(0)), in_base, True


def size_hedge(
    origin: FeeScheduleEvent | None,
    matching: FeeScheduleEvent | None,
    symbol: str,
    side: Side,
    filled: Decimal,
    price: Decimal,
) -> HedgeSize:
    """
    Size the hedge for a fill so the base balance ends where it started.

    The fill's base flow is computed from the origin venue's policy and
    maker rate: a buy charged in base delivers ``filled * (1 - rate)``, a
    sell charged in base takes ``filled * (1 + rate)`` out of the balance,
    and a fee in quote leaves the base flow untouched. The hedge then moves
    exactly that much the other way on the matching venue, sized up where
    the hedge's own fee is charged in base: a buy delivers
    ``amount * (1 - rate)`` and a sell costs ``amount * (1 + rate)``. A fee
    charged in quote is left to the USDT leg.

    The result is quantized to the venue's amount precision so the order is
    not rejected for its 13th decimal place: down, since the hedge should
    not exceed the position it flattens. A hedge whose notional then falls
    under the matching venue's minimum is rounded up instead, which is the
    only way it gets accepted at all - one truncated tick is enough to put
    a bumped order back under the minimum it was bumped to clear.

    A fill too small to quantize to anything sizes to zero. The caller
    decides what to do with that; there is no size that both hedges it and
    the venue accepts.

    A bumped hedge covers more than the fill that triggered it. The
    result says how much, in the fill's own units, so the excess counts
    against the next fill of the same order rather than being hedged
    twice.

    Parameters
    ----------
    origin : FeeScheduleEvent | None
        The fee schedule of the venue the fill happened on. None, or a
        schedule without the symbol, hedges the fill unadjusted.
    matching : FeeScheduleEvent | None
        The fee schedule of the venue the hedge will be placed on.
    symbol : str
        CCXT symbol.
    side : Side
        Side of the filled order.
    filled : Decimal
        Quantity filled.
    price : Decimal
        Price the fill executed at.

    Returns
    -------
    HedgeSize
        Quantity to trade on the matching venue and the fill it covers.
    """
    origin_rate, origin_in_base, _ = _leg(origin, symbol, side, taker=False)
    hedge_side = Side.SELL if side is Side.BUY else Side.BUY
    matching_rate, matching_in_base, matched = _leg(
        matching, symbol, hedge_side, taker=True
    )

    if side is Side.BUY:
        held = filled * (1 - origin_rate) if origin_in_base else filled
        quantity = held / (1 + matching_rate) if matching_in_base else held
    else:
        sold = filled * (1 + origin_rate) if origin_in_base else filled
        quantity = sold / (1 - matching_rate) if matching_in_base else sold

    min_cost = None
    precision = None
    if matched and matching is not None:
        entry = schedule_for(matching, symbol)
        if entry is not None:
            min_cost = entry.min_cost
            precision = entry.amount_precision
    places = precision if precision is not None else FALLBACK_PRECISION
    tick = Decimal(1).scaleb(-places)
    unbumped = quantity
    quantity = quantity.quantize(tick, rounding=ROUND_DOWN)
    covers = filled
    if min_cost is not None and quantity * price < Decimal(str(min_cost)):
        quantity = (Decimal(str(min_cost)) * DUST_MARGIN / price).quantize(
            tick, rounding=ROUND_UP
        )
        if unbumped > 0:
            covers = filled * quantity / unbumped
    return HedgeSize(quantity=quantity, covers=covers)


def hedge_quantity(
    origin: FeeScheduleEvent | None,
    matching: FeeScheduleEvent | None,
    symbol: str,
    side: Side,
    filled: Decimal,
    price: Decimal,
) -> Decimal:
    """
    Return the quantity ``size_hedge`` would trade, without what it covers.

    Parameters
    ----------
    origin : FeeScheduleEvent | None
        See ``size_hedge``.
    matching : FeeScheduleEvent | None
        See ``size_hedge``.
    symbol : str
        CCXT symbol.
    side : Side
        Side of the filled order.
    filled : Decimal
        Quantity filled.
    price : Decimal
        Price the fill executed at.

    Returns
    -------
    Decimal
        Quantity to trade on the matching venue.
    """
    return size_hedge(origin, matching, symbol, side, filled, price).quantity


def hedge_price(event: OrderEvent) -> Decimal | None:
    """
    Return the price a fill executed at.

    Parameters
    ----------
    event : OrderEvent
        The order event.

    Returns
    -------
    Decimal | None
        The average fill price, falling back to the price of the last fill
        the event carried, or None if the event reports neither. An event
        with a fill but no price is a venue reporting something we cannot
        size a hedge from, and is skipped rather than guessed at.
    """
    if event.avg_price is not None:
        return event.avg_price
    if event.last_fill is not None:
        return event.last_fill.price
    return None


def fee_mismatch(event: OrderEvent, schedule: FeeScheduleEvent | None) -> str | None:
    """
    Describe how a venue's reported fill fee contradicts the schedule.

    The schedule is what hedges are sized with, so what the venue actually
    charged is checked against it: a fee in an unexpected currency means
    the account's fee mode moved, a rate far from the schedule's means the
    account's volume tier did.

    Parameters
    ----------
    event : OrderEvent
        The order event reporting the fill.
    schedule : FeeScheduleEvent | None
        The fee schedule of the venue the fill happened on.

    Returns
    -------
    str | None
        A human readable description of the mismatch, None when the
        reported fee agrees with the schedule or carries too little
        information to check.
    """
    if schedule is None or event.last_fill is None:
        return None
    if event.side is None or event.filled <= 0:
        return None
    fill = event.last_fill
    if fill.fee is None or fill.fee_currency is None:
        return None
    entry = schedule_for(schedule, event.symbol)
    if entry is None:
        return None
    base, quote = base_quote(event.symbol)
    expected = fee_currency_for(schedule.fee_currency, event.side, base, quote)
    if fill.fee_currency != expected:
        return f"fee charged in {fill.fee_currency}, schedule says {expected}"
    rate = entry.taker if fill.liquidity is Liquidity.TAKER else entry.maker
    if rate is None or rate == 0:
        return None
    if fill.fee_currency == base:
        observed = fill.fee / event.filled
    else:
        price = event.avg_price or fill.price
        if price <= 0:
            return None
        observed = fill.fee / (event.filled * price)
    if observed > rate * MAGNITUDE_TOLERANCE or observed < rate / MAGNITUDE_TOLERANCE:
        return f"fee rate {observed} deviates from the schedule's {rate}"
    return None


def hedge_id(event: OrderEvent, sequence: int) -> str:
    """
    Return the client order id of the ``sequence``-th hedge of an order.

    Parameters
    ----------
    event : OrderEvent
        The order event that reported the fill.
    sequence : int
        1 for the first hedge of the order, 2 for the next, and so on.

    Returns
    -------
    str
        The filled order's own id for the first hedge, which is unique per
        venue and keeps the hedge traceable to what it hedges. A later hedge
        of the same order needs an id the venue has not seen, so it carries
        a fresh stamp in the same ``t-<stamp>_<strategy>_<slot>`` shape,
        which the order watcher still attributes to the strategy; the
        ``hedge_of`` tag names the order for everything else.
    """
    if sequence <= 1:
        return event.intent_id
    try:
        _prefix, rest = event.intent_id.split("-", 1)
        _stamp, strategy, slot = rest.split("_")
    except ValueError:
        return f"{event.intent_id}h{sequence}"
    return f"t-{now_ns() // 1000:018d}_{strategy}_{slot}"


def hedge_intent(
    event: OrderEvent,
    matching_venue: str,
    quantity: Decimal,
    price: Decimal,
    sequence: int = 1,
) -> OrderIntent:
    """
    Build the market order that flattens a fill.

    Parameters
    ----------
    event : OrderEvent
        The order event that reported the fill.
    matching_venue : str
        Venue to place the hedge on.
    quantity : Decimal
        Quantity to trade, already sized and quantized.
    price : Decimal
        Price the fill executed at, passed through for venues that size a
        market order by cost.
    sequence : int
        Which hedge of the order this is, see ``hedge_id``.

    Returns
    -------
    OrderIntent
        The intent.
    """
    side = Side.BUY if event.side is Side.SELL else Side.SELL
    return OrderIntent(
        ts_recv=now_ns(),
        intent_id=hedge_id(event, sequence),
        strategy="matching",
        venue=matching_venue,
        symbol=event.symbol,
        side=side,
        order_type=OrderKind.MARKET,
        amount=quantity,
        price=price,
        tags={"hedge_of": event.intent_id, "origin_venue": event.venue},
    )


def should_hedge(
    event: OrderEvent, should_match: dict[str, str], hedged: HedgeBook
) -> tuple[str, Decimal] | None:
    """
    Decide whether an order event calls for a hedge, where, and how much.

    Parameters
    ----------
    event : OrderEvent
        The order event.
    should_match : dict[str, str]
        Taker venue per strategy identifier.
    hedged : HedgeBook
        Fill size covered per order. Read here, written by the caller once
        the hedge is actually out.

    Returns
    -------
    tuple[str, Decimal] | None
        The venue to hedge on and the size not yet hedged, or None if the
        event adds nothing to hedge: the size it reports is already covered,
        which is what a repeated report of the same fill looks like, or
        what a report after a bumped hedge looks like until the fill
        catches up with the bump.
    """
    if event.state not in HEDGEABLE_STATES or event.filled <= 0:
        return None
    if event.side is None:
        logger.warning(f"Order event {event.intent_id} carries no side, not hedging")
        return None

    strategy_identifier = event.tags.get(STRATEGY_TAG, "")
    matching_venue = should_match.get(strategy_identifier)
    if matching_venue is None:
        logger.debug(f"{event.intent_id} belongs to no matching strategy")
        return None
    if matching_venue == event.venue:
        logger.debug("Cannot match self.")
        return None

    key = (event.venue, event.intent_id)
    unhedged = event.filled - hedged.hedged(key)
    if unhedged <= 0:
        logger.debug(f"{event.intent_id} reports {event.filled} filled, all hedged")
        return None
    return matching_venue, unhedged


def hedging_venues(config: AppConfig, production_mode: bool) -> set[str]:
    """
    Return the venues whose fee schedules a hedge needs.

    Sizing a hedge prices two legs: the venue the fill happened on and the
    venue it is flattened on. Only strategies that hedge, and only those
    active in this mode, produce either. The full venue list would be wrong
    to wait on: the fee watcher covers the venues strategies subscribe to,
    so a venue declared for market data alone never publishes a schedule
    and waiting on it burns the whole deadline on every start.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    production_mode : bool
        Which set of strategies is running, see ``active_strategies``.

    Returns
    -------
    set[str]
        Venue ids, empty if nothing hedges.
    """
    venues: set[str] = set()
    for strategy in config.active_strategies(production_mode):
        if not strategy.params.get("should_match"):
            continue
        venues.add(strategy.params["taker_exchange"])
        venues |= {sub.venue for sub in strategy.subscriptions}
    return venues


async def load_schedules(redis: Redis, venues: set[str]) -> dict[str, FeeScheduleEvent]:
    """
    Read the fee schedule of every given venue from its snapshot key.

    The fee watcher publishes schedules as it starts, so startup waits for
    them: a hedge sized without a schedule leaves a fee-sized residual
    position. Past the deadline, whatever is still missing is logged and
    the matcher starts anyway, hedging those venues' fills unadjusted.

    Nothing is read from the bus while this waits, so the caller resolves
    its stream cursor first: fills published during the wait belong to the
    matcher, not to the gap before it started.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    venues : set[str]
        Venue ids to wait for, see ``hedging_venues``.

    Returns
    -------
    dict[str, FeeScheduleEvent]
        The schedules that were available, keyed by venue id.
    """
    schedules: dict[str, FeeScheduleEvent] = {}
    deadline = time.monotonic() + SCHEDULE_WAIT_S
    while True:
        for venue in sorted(venues):
            if venue in schedules:
                continue
            raw = await redis.get(fees_snapshot_key(venue))
            if raw is None:
                continue
            try:
                event = decode(raw)
            except Exception as error:  # noqa: BLE001, keep waiting for a good one
                logger.error(f"Undecodable fee schedule for {venue}: {error}")
                continue
            if isinstance(event, FeeScheduleEvent):
                schedules[venue] = event
        missing = sorted(venues - schedules.keys())
        if not missing:
            return schedules
        if time.monotonic() >= deadline:
            logger.error(
                f"No fee schedule for {missing} after {SCHEDULE_WAIT_S}s; "
                "hedges on them go unadjusted"
            )
            return schedules
        await asyncio.sleep(SCHEDULE_POLL_S)


async def handle_order_event(
    redis: Redis,
    publisher: StreamPublisher,
    event: OrderEvent,
    should_match: dict[str, str],
    hedged: HedgeBook,
    schedules: dict[str, FeeScheduleEvent],
) -> OrderIntent | None:
    """
    Hedge one order event, if it needs hedging.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``oms:intents``.
    event : OrderEvent
        The order event.
    should_match : dict[str, str]
        Taker venue per strategy identifier.
    hedged : HedgeBook
        Fill size covered per order, updated once the hedge is published.
    schedules : dict[str, FeeScheduleEvent]
        Fee schedules by venue id, see ``load_schedules``.

    Returns
    -------
    OrderIntent | None
        The hedge that was published, if any.
    """
    decision = should_hedge(event, should_match, hedged)
    if decision is None:
        return None
    matching_venue, unhedged = decision

    price = hedge_price(event)
    if price is None or price <= 0:
        logger.error(f"Cannot price a hedge for {event.intent_id}, skipping")
        return None

    mismatch = fee_mismatch(event, schedules.get(event.venue))
    if mismatch is not None:
        logger.warning(f"{event.venue} contradicts its fee schedule: {mismatch}")

    if event.venue not in schedules or matching_venue not in schedules:
        logger.error(
            f"Hedging {event.intent_id} without a fee schedule for "
            f"{event.venue if event.venue not in schedules else matching_venue}"
        )
    size = size_hedge(
        schedules.get(event.venue),
        schedules.get(matching_venue),
        event.symbol,
        event.side or Side.BUY,
        unhedged,
        price,
    )
    if size.quantity <= 0:
        logger.error(
            f"Hedge for {event.intent_id} sizes to {size.quantity} from a fill of "
            f"{unhedged} at {price}; leaving it unhedged"
        )
        return None

    key = (event.venue, event.intent_id)
    sequence = hedged.hedges(key) + 1
    intent = hedge_intent(event, matching_venue, size.quantity, price, sequence)
    await publisher.publish(redis, intent)
    # Only now: a hedge that never went out has covered nothing, and the
    # next report of this order must still see its size as unhedged.
    hedged.record(key, size.covers)
    logger.info(f"Matching order was sent: {intent}")
    return intent


async def resolve_cursors(redis: Redis, venues: set[str]) -> dict[str, str]:
    """
    Find where ``consume_order_events`` starts reading each of its streams.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    venues : set[str]
        Venues whose fee schedule streams are followed, see
        ``hedging_venues``.

    Returns
    -------
    dict[str, str]
        The current tail of ``oms:events`` and of every venue's fee stream,
        keyed by stream name.
    """
    streams = [ORDER_EVENTS_STREAM] + [fees_stream(venue) for venue in sorted(venues)]
    return {stream: await stream_tail(redis, stream) for stream in streams}


async def consume_order_events(
    redis: Redis,
    publisher: StreamPublisher,
    should_match: dict[str, str],
    block_ms: int,
    batch: int,
    schedules: dict[str, FeeScheduleEvent],
    cursors: dict[str, str],
) -> None:
    """
    Read ``oms:events`` and hedge every fill that calls for one.

    Reading starts at ``cursors``, the tails of the streams as the caller
    found them: a fill from before then has either been hedged already or
    is old enough that hedging it now would open a new position rather
    than close one. The tails are resolved once rather than passed as
    ``$`` on every read, which would silently drop a fill published
    between two reads, and they are resolved by the caller before any
    startup wait, so a fill during that wait is still hedged.

    The fee streams of the hedging venues are read alongside, and a new
    schedule replaces the venue's entry in ``schedules`` as it arrives.
    The fee watcher republishes on every refresh and whenever the account
    changes tier; a matcher that only read the snapshot at startup sized
    every hedge with the rate of the day it was started.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``oms:intents``.
    should_match : dict[str, str]
        Taker venue per strategy identifier.
    block_ms : int
        How long a blocking read waits when no event is available.
    batch : int
        Maximum events fetched per read.
    schedules : dict[str, FeeScheduleEvent]
        Fee schedules by venue id, see ``load_schedules``. Updated in place
        as the fee streams deliver.
    cursors : dict[str, str]
        Stream id to read from per stream, see ``resolve_cursors``.
    """
    hedged = HedgeBook(HEDGE_MEMORY)
    positions: dict[Any, Any] = dict(cursors)
    while True:
        response = await redis.xread(positions, count=batch, block=block_ms)
        # redis-py types the reply as list or dict (RESP3); the client is
        # RESP2 here so it is always the list form.
        for stream, entries in cast(list[Any], response or []):
            stream_name = entry_id_str(stream)
            for entry_id, fields in entries:
                positions[stream_name] = entry_id_str(entry_id)
                try:
                    event = from_stream_fields(fields)
                except Exception as error:  # noqa: BLE001, keep consuming
                    logger.error(
                        f"Undecodable entry {entry_id} on {stream_name}: {error}"
                    )
                    continue
                if isinstance(event, FeeScheduleEvent):
                    schedules[event.venue] = event
                    logger.info(
                        f"Fee schedule for {event.venue} refreshed "
                        f"({event.source.value}, {len(event.symbols)} symbols)"
                    )
                    continue
                if not isinstance(event, OrderEvent):
                    continue
                await handle_order_event(
                    redis, publisher, event, should_match, hedged, schedules
                )


def matching_venues(config: AppConfig) -> dict[str, str]:
    """
    Read the taker venue of every strategy that hedges.

    Parameters
    ----------
    config : AppConfig
        The application configuration.

    Returns
    -------
    dict[str, str]
        Taker venue per strategy identifier. Matching applies to every
        strategy regardless of its production flag.
    """
    should_match: dict[str, str] = {}
    for strategy in config.strategies:
        if strategy.params.get("should_match"):
            should_match[strategy.identifier] = strategy.params["taker_exchange"]
    return should_match


async def main(config: AppConfig) -> None:
    """
    Run the matcher until cancelled.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    """
    should_match = matching_venues(config)
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    publisher = StreamPublisher(maxlen=config.oms.stream_maxlen)
    # Before the wait below, not after it: the orchestrator starts this
    # process ahead of the fee watcher, and a fill published while the
    # schedules are still missing is one this matcher owns.
    venues = hedging_venues(config, production)
    cursors = await resolve_cursors(redis, venues)
    schedules = await load_schedules(redis, venues)
    logger.info(
        f"Matching fills for {sorted(should_match)} with schedules for "
        f"{sorted(schedules)} (production={production})"
    )
    try:
        await consume_order_events(
            redis,
            publisher,
            should_match,
            config.oms.block_ms,
            config.oms.batch,
            schedules,
            cursors,
        )
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main(load_app_config()))
