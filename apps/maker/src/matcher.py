"""Taker hedge for maker fills.

When an order placed by a matching strategy fills on its maker venue, this
process buys or sells the same quantity on the taker venue to flatten the
position. Its input is ``oms:events`` and its output is an ``OrderIntent`` on
``oms:intents``, so it is an ordinary consumer of the bus rather than a
component wired into the exchange websocket.

Until phase 3 this module also owned the ``watch_orders`` loop and was the
only thing in the system that could see a fill. That half now lives in
``order_watcher.py``, which leaves this file as pure hedging logic and makes
it the natural candidate to move into the strategies repo. See
``docs/design/event-driven-framework.md`` section 6.
"""

import asyncio
import logging
from decimal import Decimal
from typing import Any, cast

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.order_watcher import STRATEGY_TAG
from apps.maker.src.structs import LimitedSet
from apps.shared.src.config import AppConfig, load_app_config
from apps.shared.src.events import (
    ORDER_EVENTS_STREAM,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    from_stream_fields,
    now_ns,
)
from apps.shared.src.streams import StreamPublisher, entry_id_str, stream_tail
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# TODO:
# - Make fee fetching dynamic
NATIVE_ASSET_FEE: dict[str, float] = {"bitget": 0.001, "gate": 0.001}

# Smallest notional a venue will accept, and the notional to bump a hedge to
# when the fill was smaller than that.
MIN_NOTIONAL = 3.0
TARGET_NOTIONAL = 3.1

# An order is worth hedging once it can no longer fill any further. A
# cancelled or expired order that filled in part still leaves a position.
HEDGEABLE_STATES: frozenset[OrderState] = frozenset(
    {OrderState.FILLED, OrderState.CANCELLED, OrderState.EXPIRED}
)

# Orders remembered so a repeated terminal event does not hedge twice.
HEDGE_MEMORY = 1000


def hedge_quantity(
    origin_venue: str, matching_venue: str, side: Side, filled: float, price: float
) -> float:
    """
    Size the hedge for a fill.

    Venues that charge their fee in the asset rather than the quote leave us
    with less base than we sold, or require more base than we bought, so the
    quantity is adjusted on whichever leg pays in kind. A fill below the
    venue's minimum notional is rounded up to a size the venue will accept:
    an unhedged position is worse than a slightly oversized hedge.

    Parameters
    ----------
    origin_venue : str
        Venue the fill happened on.
    matching_venue : str
        Venue the hedge will be placed on.
    side : Side
        Side of the filled order.
    filled : float
        Quantity filled.
    price : float
        Price the fill executed at.

    Returns
    -------
    float
        Quantity to trade on the matching venue.
    """
    quantity = filled
    if side is Side.BUY and origin_venue in NATIVE_ASSET_FEE:
        quantity = quantity * (1 - NATIVE_ASSET_FEE[origin_venue])

    if price * quantity <= MIN_NOTIONAL:
        quantity = TARGET_NOTIONAL / price

    if side is Side.SELL and matching_venue in NATIVE_ASSET_FEE:
        quantity = quantity / (1 - NATIVE_ASSET_FEE[matching_venue])

    return quantity


def hedge_price(event: OrderEvent) -> float | None:
    """
    Return the price a fill executed at.

    Parameters
    ----------
    event : OrderEvent
        The order event.

    Returns
    -------
    float | None
        The average fill price, falling back to the price of the last fill
        the event carried, or None if the event reports neither. An event
        with a fill but no price is a venue reporting something we cannot
        size a hedge from, and is skipped rather than guessed at.
    """
    if event.avg_price is not None:
        return float(event.avg_price)
    if event.last_fill is not None:
        return float(event.last_fill.price)
    return None


def hedge_intent(
    event: OrderEvent, matching_venue: str, quantity: float, price: float
) -> OrderIntent:
    """
    Build the market order that flattens a fill.

    Parameters
    ----------
    event : OrderEvent
        The order event that reported the fill.
    matching_venue : str
        Venue to place the hedge on.
    quantity : float
        Quantity to trade.
    price : float
        Price the fill executed at, passed through for venues that size a
        market order by cost.

    Returns
    -------
    OrderIntent
        The intent. It reuses the filled order's client id, which is unique
        per venue and makes the hedge traceable back to what it hedges.
    """
    side = Side.BUY if event.side is Side.SELL else Side.SELL
    return OrderIntent(
        ts_recv=now_ns(),
        intent_id=event.intent_id,
        strategy="matching",
        venue=matching_venue,
        symbol=event.symbol,
        side=side,
        order_type=OrderKind.MARKET,
        amount=Decimal(quantity).quantize(Decimal("0.0000")),
        price=Decimal(price),
        tags={"hedge_of": event.intent_id, "origin_venue": event.venue},
    )


def should_hedge(
    event: OrderEvent, should_match: dict[str, str], hedged: LimitedSet
) -> str | None:
    """
    Decide whether an order event calls for a hedge, and where.

    Parameters
    ----------
    event : OrderEvent
        The order event.
    should_match : dict[str, str]
        Taker venue per strategy identifier.
    hedged : LimitedSet
        Orders already hedged.

    Returns
    -------
    str | None
        The venue to hedge on, or None if the event needs no hedge.
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
    if key in hedged:
        logger.warning(f"The order no {event.intent_id} tried getting matched twice.")
        return None
    hedged.add(key)
    return matching_venue


async def handle_order_event(
    redis: Redis,
    publisher: StreamPublisher,
    event: OrderEvent,
    should_match: dict[str, str],
    hedged: LimitedSet,
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
    hedged : LimitedSet
        Orders already hedged.

    Returns
    -------
    OrderIntent | None
        The hedge that was published, if any.
    """
    matching_venue = should_hedge(event, should_match, hedged)
    if matching_venue is None:
        return None

    price = hedge_price(event)
    if price is None or price <= 0:
        logger.error(f"Cannot price a hedge for {event.intent_id}, skipping")
        return None

    quantity = hedge_quantity(
        event.venue, matching_venue, event.side or Side.BUY, float(event.filled), price
    )
    intent = hedge_intent(event, matching_venue, quantity, price)
    await publisher.publish(redis, intent)
    logger.info(f"Matching order was sent: {intent}")
    return intent


async def consume_order_events(
    redis: Redis,
    publisher: StreamPublisher,
    should_match: dict[str, str],
    block_ms: int,
    batch: int,
) -> None:
    """
    Read ``oms:events`` and hedge every fill that calls for one.

    Reading starts at the tail of the stream as it stands when this begins:
    a fill from before then has either been hedged already or is old enough
    that hedging it now would open a new position rather than close one.
    The tail is resolved once rather than passed as ``$`` on every read,
    which would silently drop a fill published between two reads.

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
    """
    hedged = LimitedSet(HEDGE_MEMORY)
    cursor = await stream_tail(redis, ORDER_EVENTS_STREAM)
    while True:
        response = await redis.xread(
            {ORDER_EVENTS_STREAM: cursor}, count=batch, block=block_ms
        )
        # redis-py types the reply as list or dict (RESP3); the client is
        # RESP2 here so it is always the list form.
        for _stream, entries in cast(list[Any], response or []):
            for entry_id, fields in entries:
                cursor = entry_id_str(entry_id)
                try:
                    event = from_stream_fields(fields)
                except Exception as error:  # noqa: BLE001, keep consuming
                    logger.error(f"Undecodable order event {entry_id}: {error}")
                    continue
                if not isinstance(event, OrderEvent):
                    continue
                await handle_order_event(redis, publisher, event, should_match, hedged)


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
    logger.info(f"Matching fills for {sorted(should_match)} (production={production})")
    try:
        await consume_order_events(
            redis, publisher, should_match, config.oms.block_ms, config.oms.batch
        )
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main(load_app_config()))
