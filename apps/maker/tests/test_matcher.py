"""Tests for the taker hedge."""

import asyncio
from decimal import Decimal
from typing import Any, cast

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src.matcher import (
    HEDGE_MEMORY,
    NATIVE_ASSET_FEE,
    consume_order_events,
    handle_order_event,
    hedge_intent,
    hedge_price,
    hedge_quantity,
    matching_venues,
    should_hedge,
)
from apps.maker.src.order_watcher import STRATEGY_TAG
from apps.maker.src.structs import LimitedSet
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
)
from apps.shared.src.events import (
    prefixed,
    INTENTS_STREAM,
    Fill,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    from_stream_fields,
    now_ns,
)
from apps.shared.src.runtime import Clock
from apps.shared.src.streams import StreamPublisher, replay_closed_key

SHOULD_MATCH = {"lmb": "bitget"}


def fill_event(
    intent_id: str = "t-250906120000_lmb_eb",
    venue: str = "mexc",
    state: OrderState = OrderState.FILLED,
    side: Side = Side.SELL,
    filled: Decimal = Decimal("40"),
    avg_price: Decimal | None = Decimal("0.35"),
    strategy_id: str = "lmb",
    last_fill: Fill | None = None,
) -> OrderEvent:
    """Return an order event as the order watcher would publish it."""
    return OrderEvent(
        ts_recv=now_ns(),
        intent_id=intent_id,
        strategy=f"{strategy_id}_eb",
        venue=venue,
        symbol="ALPH/USDT",
        state=state,
        side=side,
        venue_order_id=f"venue-{intent_id}",
        filled=filled,
        remaining=Decimal(0),
        avg_price=avg_price,
        last_fill=last_fill,
        tags={STRATEGY_TAG: strategy_id, "order_id": "eb"},
    )


async def intents_on(redis: Any, prefix: str = "") -> list[OrderIntent]:
    """Return every order intent published to the intents stream."""
    entries = cast(list[Any], await redis.xrange(prefixed(prefix, INTENTS_STREAM)))
    decoded = (from_stream_fields(fields) for _id, fields in entries)
    return [event for event in decoded if isinstance(event, OrderIntent)]


# Sizing --------------------------------------------------------------------


def test_a_sell_fill_is_hedged_one_for_one_on_a_quote_fee_venue():
    """With no fee paid in the asset, the hedge is the size that filled."""
    assert hedge_quantity("mexc", "kraken", Side.SELL, 40.0, 1.0) == 40.0


def test_a_buy_on_a_native_fee_venue_leaves_less_to_hedge():
    """A venue that takes its fee in the asset leaves us short of the fill."""
    quantity = hedge_quantity("gate", "mexc", Side.BUY, 40.0, 1.0)
    assert quantity == pytest.approx(40.0 * (1 - NATIVE_ASSET_FEE["gate"]))


def test_a_sell_hedged_on_a_native_fee_venue_needs_more():
    """Buying back on a venue that charges in the asset needs a bigger order."""
    quantity = hedge_quantity("mexc", "bitget", Side.SELL, 40.0, 1.0)
    assert quantity == pytest.approx(40.0 / (1 - NATIVE_ASSET_FEE["bitget"]))


def test_a_dust_fill_is_rounded_up_to_a_tradeable_size():
    """A fill under the minimum notional is hedged at a size a venue accepts."""
    quantity = hedge_quantity("mexc", "kraken", Side.SELL, 1.0, 1.0)
    assert quantity == pytest.approx(3.1)


def test_hedge_price_prefers_the_average():
    """The average fill price is what the hedge is sized and priced against."""
    assert hedge_price(fill_event(avg_price=Decimal("0.36"))) == 0.36


def test_hedge_price_falls_back_to_the_last_fill():
    """Without an average, the price of the fill that closed the order is used."""
    event = fill_event(
        avg_price=None, last_fill=Fill(price=Decimal("0.37"), amount=Decimal("40"))
    )
    assert hedge_price(event) == 0.37


def test_hedge_price_is_none_when_the_venue_reported_none():
    """An event with no price at all cannot size a hedge."""
    assert hedge_price(fill_event(avg_price=None)) is None


def test_hedge_intent_is_an_opposing_market_order():
    """The hedge trades the other way, at market, on the other venue."""
    intent = hedge_intent(fill_event(), "bitget", 40.0, 0.35)
    assert intent.side is Side.BUY
    assert intent.venue == "bitget"
    assert intent.order_type is OrderKind.MARKET
    assert intent.symbol == "ALPH/USDT"
    assert intent.intent_id == "t-250906120000_lmb_eb"
    assert intent.tags["hedge_of"] == "t-250906120000_lmb_eb"
    assert intent.tags["origin_venue"] == "mexc"


def test_hedge_intent_of_a_buy_sells():
    """A filled buy is flattened by selling."""
    assert hedge_intent(fill_event(side=Side.BUY), "bitget", 1.0, 1.0).side is Side.SELL


# Selection -----------------------------------------------------------------


def test_an_open_order_is_not_hedged():
    """Only an order that can no longer fill further is hedged."""
    event = fill_event(state=OrderState.OPEN)
    assert should_hedge(event, SHOULD_MATCH, LimitedSet(10)) is None


def test_a_partially_filled_order_waits():
    """A partial fill is not hedged: the order can still fill further."""
    event = fill_event(state=OrderState.PARTIALLY_FILLED)
    assert should_hedge(event, SHOULD_MATCH, LimitedSet(10)) is None


def test_a_cancelled_order_that_filled_is_still_hedged():
    """A cancellation after a partial fill still leaves a position to close."""
    event = fill_event(state=OrderState.CANCELLED, filled=Decimal("10"))
    assert should_hedge(event, SHOULD_MATCH, LimitedSet(10)) == "bitget"


def test_an_unfilled_order_is_not_hedged():
    """Nothing filled, nothing to hedge."""
    event = fill_event(state=OrderState.CANCELLED, filled=Decimal(0))
    assert should_hedge(event, SHOULD_MATCH, LimitedSet(10)) is None


def test_an_order_of_another_strategy_is_not_hedged():
    """Only strategies that asked to be matched are matched."""
    event = fill_event(strategy_id="other")
    assert should_hedge(event, SHOULD_MATCH, LimitedSet(10)) is None


def test_an_unattributed_order_is_not_hedged():
    """An order placed outside this system belongs to no strategy."""
    event = fill_event()
    event.tags = {}
    assert should_hedge(event, SHOULD_MATCH, LimitedSet(10)) is None


def test_a_fill_on_the_taker_venue_is_not_hedged():
    """The hedge venue cannot hedge against itself."""
    event = fill_event(venue="bitget")
    assert should_hedge(event, SHOULD_MATCH, LimitedSet(10)) is None


def test_an_order_without_a_side_is_not_hedged():
    """A hedge needs a direction, and guessing one would double a position."""
    event = fill_event()
    event.side = None
    assert should_hedge(event, SHOULD_MATCH, LimitedSet(10)) is None


def test_the_same_order_is_only_hedged_once():
    """A replayed terminal event must not open a second position."""
    hedged = LimitedSet(HEDGE_MEMORY)
    event = fill_event()
    assert should_hedge(event, SHOULD_MATCH, hedged) == "bitget"
    assert should_hedge(fill_event(), SHOULD_MATCH, hedged) is None


def test_the_same_order_id_on_two_venues_is_hedged_separately():
    """Order ids are unique per venue, so the dedupe key carries the venue."""
    hedged = LimitedSet(HEDGE_MEMORY)
    assert should_hedge(fill_event(venue="mexc"), SHOULD_MATCH, hedged) == "bitget"
    assert should_hedge(fill_event(venue="gate"), {"lmb": "bitget"}, hedged) == "bitget"


# Publication ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_fill_publishes_a_hedge_intent():
    """The hedge reaches the order manager as an intent."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    intent = await handle_order_event(
        redis, publisher, fill_event(), SHOULD_MATCH, LimitedSet(10)
    )

    assert intent is not None
    published = (await intents_on(redis))[0]
    assert published.venue == "bitget"
    assert published.side is Side.BUY
    assert published.order_type is OrderKind.MARKET
    assert published.strategy == "matching"


@pytest.mark.asyncio
async def test_an_event_that_needs_no_hedge_publishes_nothing():
    """Events the matcher does not act on leave the intents stream empty."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    intent = await handle_order_event(
        redis,
        publisher,
        fill_event(state=OrderState.OPEN),
        SHOULD_MATCH,
        LimitedSet(10),
    )

    assert intent is None
    assert await redis.xlen(INTENTS_STREAM) == 0


@pytest.mark.asyncio
async def test_an_unpriceable_fill_is_skipped():
    """A fill the venue gave no price for cannot be hedged."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    intent = await handle_order_event(
        redis, publisher, fill_event(avg_price=None), SHOULD_MATCH, LimitedSet(10)
    )

    assert intent is None
    assert await redis.xlen(INTENTS_STREAM) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [True, False], ids=["decoded", "bytes"])
async def test_consume_order_events_hedges_what_it_reads(decode: bool):
    """
    The consumer loop turns fills on the bus into hedges.

    Run against both a decoding and a non-decoding client: the connection
    pool decides which one production gets, and a cursor built with
    ``str()`` from a bytes id is rejected on the next read. That killed this
    loop one event before the first live fill, which then went unhedged.
    """
    redis = fakeredis.FakeRedis(decode_responses=decode)
    publisher = StreamPublisher(maxlen=100)
    # A fill from before the matcher started must not be hedged.
    await publisher.publish(redis, fill_event(intent_id="old"))

    consumer = asyncio.create_task(
        consume_order_events(redis, publisher, SHOULD_MATCH, 10, 10)
    )
    await asyncio.sleep(0.05)
    # Two fills, read separately, so the cursor built from the first read has
    # to survive as the argument of the second.
    await publisher.publish(redis, fill_event(intent_id="first"))
    await asyncio.sleep(0.15)
    await publisher.publish(redis, fill_event(intent_id="second"))
    await asyncio.sleep(0.15)

    assert not consumer.done(), "the consumer died between the two fills"
    consumer.cancel()

    assert [i.intent_id for i in await intents_on(redis)] == ["first", "second"]


@pytest.mark.asyncio
async def test_consume_order_events_survives_an_undecodable_entry():
    """One bad entry does not stop the matcher from hedging the next fill."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    consumer = asyncio.create_task(
        consume_order_events(redis, publisher, SHOULD_MATCH, 10, 10)
    )
    await asyncio.sleep(0.05)
    from apps.shared.src.events import ORDER_EVENTS_STREAM

    await redis.xadd(ORDER_EVENTS_STREAM, {"type": "order_event", "data": "{"})
    await publisher.publish(redis, fill_event(intent_id="after"))
    await asyncio.sleep(0.15)
    consumer.cancel()

    assert [i.intent_id for i in await intents_on(redis)] == ["after"]


# Configuration -------------------------------------------------------------


def test_matching_venues_reads_every_strategy_that_hedges():
    """Matching applies regardless of the production flag."""
    subscription = Subscription(venue="mexc", symbol="ALPH/USDT")
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(),
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=False,
                subscriptions=(subscription,),
                params={"should_match": True, "taker_exchange": "bitget"},
            ),
            StrategyConfig(
                identifier="plain",
                type="take_take",
                production=True,
                subscriptions=(subscription,),
                params={},
            ),
        ),
    )

    assert matching_venues(config) == {"lmb": "bitget"}


def test_hedge_intent_is_stamped_with_the_time_it_is_given():
    """Under replay a hedge carries event time, live the wall clock."""
    stamped = hedge_intent(fill_event(), "bitget", 40.0, 0.35, ts_recv=123)
    assert stamped.ts_recv == 123
    assert (
        hedge_intent(fill_event(), "bitget", 40.0, 0.35).ts_recv > 1_700_000_000 * 10**9
    )


@pytest.mark.asyncio
async def test_a_replay_clock_stamps_the_hedge_with_the_fill_time():
    """The clock advances to the fill's receive time and the hedge carries it."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100, prefix="bt:r1")
    event = fill_event()
    intent = await handle_order_event(
        redis, publisher, event, SHOULD_MATCH, LimitedSet(10), Clock.replay()
    )
    assert intent is not None and intent.ts_recv == event.ts_recv
    assert [i.intent_id for i in await intents_on(redis, "bt:r1")] == [event.intent_id]


@pytest.mark.asyncio
async def test_consume_under_replay_reads_from_the_start_and_stops_when_closed():
    """Every fill on the replayed stream is hedged, then the closed key ends the loop."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100, prefix="bt:r1")
    await publisher.publish(redis, fill_event(intent_id="before"))
    consumer = asyncio.create_task(
        consume_order_events(
            redis, publisher, SHOULD_MATCH, 10, 10, prefix="bt:r1", clock=Clock.replay()
        )
    )
    await asyncio.sleep(0.05)
    await publisher.publish(redis, fill_event(intent_id="after"))
    await asyncio.sleep(0.05)
    assert not consumer.done()
    await redis.set(replay_closed_key("bt:r1"), 1)
    await asyncio.wait_for(consumer, timeout=1)
    assert [i.intent_id for i in await intents_on(redis, "bt:r1")] == [
        "before",
        "after",
    ]
    assert await redis.xlen(INTENTS_STREAM) == 0
