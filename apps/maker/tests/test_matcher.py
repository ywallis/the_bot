"""Tests for the taker hedge."""

import asyncio
from decimal import Decimal
from typing import Any, cast

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import matcher
from apps.maker.src.matcher import (
    HEDGE_MEMORY,
    HedgeBook,
    hedge_id,
    consume_order_events,
    resolve_cursors,
    size_hedge,
    fee_mismatch,
    handle_order_event,
    hedge_intent,
    hedge_price,
    hedge_quantity,
    hedging_venues,
    load_schedules,
    matching_venues,
    should_hedge,
)
from apps.maker.src.order_watcher import STRATEGY_TAG
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
    VenueConfig,
)
from apps.shared.src.events import (
    INTENTS_STREAM,
    ORDER_EVENTS_STREAM,
    FeeScheduleEvent,
    FeeSource,
    fees_stream,
    Fill,
    Liquidity,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    SymbolFees,
    from_stream_fields,
    now_ns,
)
from apps.shared.src.streams import StreamPublisher, stream_tail

SYMBOL = "BTC/USDT"
SHOULD_MATCH = {"lmb": "venue_b"}


def schedule(
    venue: str,
    policy: str,
    maker: str = "0.001",
    taker: str = "0.002",
    min_cost: str | None = "3.0",
    precision: int | None = 4,
) -> FeeScheduleEvent:
    """Return a fee schedule for one venue as the fee watcher publishes it."""
    return FeeScheduleEvent(
        ts_recv=now_ns(),
        venue=venue,
        fee_currency=policy,
        symbols=[
            SymbolFees(
                symbol=SYMBOL,
                maker=Decimal(maker),
                taker=Decimal(taker),
                min_cost=None if min_cost is None else float(min_cost),
                amount_precision=precision,
            )
        ],
        source=FeeSource.MARKETS,
    )


def fill_event(
    intent_id: str = "t-250906120000_lmb_eb",
    venue: str = "venue_a",
    state: OrderState = OrderState.FILLED,
    side: Side = Side.SELL,
    filled: Decimal = Decimal("40"),
    avg_price: Decimal | None = Decimal(1),
    strategy_id: str = "lmb",
    last_fill: Fill | None = None,
) -> OrderEvent:
    """Return an order event as the order watcher would publish it."""
    return OrderEvent(
        ts_recv=now_ns(),
        intent_id=intent_id,
        strategy=f"{strategy_id}_eb",
        venue=venue,
        symbol=SYMBOL,
        state=state,
        side=side,
        venue_order_id=f"venue-{intent_id}",
        filled=filled,
        remaining=Decimal(0),
        avg_price=avg_price,
        last_fill=last_fill,
        tags={STRATEGY_TAG: strategy_id, "order_id": "eb"},
    )


async def intents_on(redis: Any) -> list[OrderIntent]:
    """Return every order intent published to the intents stream."""
    entries = cast(list[Any], await redis.xrange(INTENTS_STREAM))
    decoded = (from_stream_fields(fields) for _id, fields in entries)
    return [event for event in decoded if isinstance(event, OrderIntent)]


QUOTE = schedule("venue_a", "quote")
RECEIVED = schedule("venue_a", "received")
BASE = schedule("venue_a", "base")


# Sizing --------------------------------------------------------------------


def test_a_sell_fill_is_hedged_one_for_one_on_a_quote_fee_venue():
    """With both venues charging in quote, the hedge is the size that filled."""
    quantity = hedge_quantity(
        QUOTE, schedule("venue_b", "quote"), SYMBOL, Side.SELL, Decimal(40), Decimal(1)
    )
    assert quantity == Decimal("40.0000")


def test_a_buy_on_a_received_fee_venue_leaves_less_to_hedge():
    """A venue that takes a buy's fee in the asset leaves us short of the fill."""
    quantity = hedge_quantity(
        RECEIVED,
        schedule("venue_b", "quote"),
        SYMBOL,
        Side.BUY,
        Decimal(40),
        Decimal(1),
    )
    assert quantity == Decimal("39.9600")


def test_a_buy_hedged_on_a_received_fee_venue_needs_more():
    """A hedge buy on a venue charging in the asset buys back the fee too."""
    quantity = hedge_quantity(
        QUOTE,
        schedule("venue_b", "received"),
        SYMBOL,
        Side.SELL,
        Decimal(40),
        Decimal(1),
    )
    assert quantity == Decimal("40.0801")


def test_a_sell_hedged_on_a_base_fee_venue_accounts_for_its_own_fee():
    """A hedge sell charged in base sizes itself down so it does not oversell."""
    quantity = hedge_quantity(
        QUOTE, schedule("venue_b", "base"), SYMBOL, Side.BUY, Decimal(40), Decimal(1)
    )
    assert quantity == Decimal("39.9201")


def test_a_sell_on_a_base_fee_venue_took_more_than_the_fill():
    """A venue charging a sell's fee in base erodes the balance beyond the fill."""
    quantity = hedge_quantity(
        BASE, schedule("venue_b", "quote"), SYMBOL, Side.SELL, Decimal(40), Decimal(1)
    )
    assert quantity == Decimal("40.0400")


def test_the_hedge_leg_uses_the_taker_rate_and_the_fill_the_maker_rate():
    """The two legs are charged differently, and are sized accordingly."""
    quantity = hedge_quantity(
        schedule("venue_a", "quote", maker="0.001"),
        schedule("venue_b", "received", taker="0.004"),
        SYMBOL,
        Side.SELL,
        Decimal(40),
        Decimal(1),
    )
    assert quantity == Decimal("40.1606")


def test_a_dust_fill_is_rounded_up_to_a_tradeable_size():
    """A fill under the minimum notional is hedged at a size a venue accepts."""
    quantity = hedge_quantity(
        QUOTE,
        schedule("venue_b", "quote", min_cost="3.0"),
        SYMBOL,
        Side.SELL,
        Decimal(1),
        Decimal(1),
    )
    assert quantity == Decimal("3.0900")


def test_an_unknown_minimum_notional_leaves_the_size_alone():
    """Without a reported minimum, a small fill is hedged at its own size."""
    quantity = hedge_quantity(
        QUOTE,
        schedule("venue_b", "quote", min_cost=None),
        SYMBOL,
        Side.SELL,
        Decimal(1),
        Decimal(1),
    )
    assert quantity == Decimal("1.0000")


def test_a_bumped_hedge_stays_above_the_minimum_on_an_expensive_asset():
    """
    The bump is rounded up to the tick, not down through it.

    One tick of an asset priced at 100k is worth more than the 3% margin
    the bump adds, so truncating gave a size of zero: an order the venue
    rejects, for a position left open.
    """
    quantity = hedge_quantity(
        QUOTE,
        schedule("venue_b", "quote", min_cost="5.0", precision=4),
        SYMBOL,
        Side.SELL,
        Decimal("0.00001"),
        Decimal(100_000),
    )
    assert quantity == Decimal("0.0001")
    assert quantity * Decimal(100_000) >= Decimal("5.0")


def test_quantizing_below_the_minimum_bumps_the_hedge_back_over_it():
    """A size the tick truncates under the minimum is bumped, not shipped."""
    quantity = hedge_quantity(
        QUOTE,
        schedule("venue_b", "quote", min_cost="3.5", precision=0),
        SYMBOL,
        Side.SELL,
        Decimal("3.9"),
        Decimal(1),
    )
    assert quantity == Decimal(4)


def test_a_bumped_hedge_covers_more_than_the_fill():
    """The bump is reported in the fill's units, to net off the next report."""
    size = size_hedge(
        QUOTE,
        schedule("venue_b", "quote", min_cost="1.0", precision=0),
        SYMBOL,
        Side.SELL,
        Decimal("10"),
        Decimal("0.01"),
    )
    assert size.quantity == Decimal("103")
    assert size.covers == Decimal("103")


def test_a_hedge_that_is_not_bumped_covers_exactly_the_fill():
    """Fee adjustment changes the quantity sent, not the fill it covers."""
    size = size_hedge(
        QUOTE,
        schedule("venue_b", "received", precision=2),
        SYMBOL,
        Side.SELL,
        Decimal(40),
        Decimal(1),
    )
    assert size.quantity == Decimal("40.08")
    assert size.covers == Decimal(40)


def test_a_fill_smaller_than_the_tick_sizes_to_nothing():
    """With no minimum to bump to, a sub-tick fill has no tradeable size."""
    quantity = hedge_quantity(
        QUOTE,
        schedule("venue_b", "quote", min_cost=None, precision=4),
        SYMBOL,
        Side.SELL,
        Decimal("0.00001"),
        Decimal(1),
    )
    assert quantity == 0


def test_a_missing_schedule_hedges_unadjusted():
    """No schedule at all sizes the hedge at the fill, with a fallback precision."""
    quantity = hedge_quantity(None, None, SYMBOL, Side.BUY, Decimal(40), Decimal(1))
    assert quantity == Decimal("40.0000")


def test_the_hedge_is_quantized_to_the_venue_precision():
    """The order amount carries no more decimals than the venue accepts."""
    quantity = hedge_quantity(
        QUOTE,
        schedule("venue_b", "received", precision=2),
        SYMBOL,
        Side.SELL,
        Decimal(40),
        Decimal(1),
    )
    assert quantity == Decimal("40.08")


# Pricing -------------------------------------------------------------------


def test_hedge_price_prefers_the_average():
    """The average fill price is what the hedge is sized and priced against."""
    assert hedge_price(fill_event(avg_price=Decimal("0.36"))) == Decimal("0.36")


def test_hedge_price_falls_back_to_the_last_fill():
    """Without an average, the price of the fill that closed the order is used."""
    event = fill_event(
        avg_price=None, last_fill=Fill(price=Decimal("0.37"), amount=Decimal("40"))
    )
    assert hedge_price(event) == Decimal("0.37")


def test_hedge_price_is_none_when_the_venue_reported_none():
    """An event with no price at all cannot size a hedge."""
    assert hedge_price(fill_event(avg_price=None)) is None


# Verification --------------------------------------------------------------


def fee(fill: Fill) -> OrderEvent:
    """Return a sell fill event carrying a reported fee."""
    return fill_event(side=Side.SELL, last_fill=fill)


def test_a_fee_in_an_unexpected_currency_is_a_mismatch():
    """A venue charging a fee outside its declared policy has moved."""
    event = fee(
        Fill(
            price=Decimal(1),
            amount=Decimal(40),
            fee=Decimal("0.04"),
            fee_currency="BTC",
        )
    )
    mismatch = fee_mismatch(event, QUOTE)
    assert mismatch is not None and "BTC" in mismatch


def test_a_fee_rate_far_from_the_schedule_is_a_mismatch():
    """A reported rate far above the schedule's means the tier moved."""
    event = fee(
        Fill(
            price=Decimal(1),
            amount=Decimal(40),
            fee=Decimal("0.4"),
            fee_currency="USDT",
        )
    )
    rich = schedule("venue_a", "quote", maker="0.001")
    assert fee_mismatch(event, rich) is not None


def test_a_fee_matching_the_schedule_verifies():
    """The fee the venue charged agrees with the schedule."""
    event = fee(
        Fill(
            price=Decimal(1),
            amount=Decimal(40),
            fee=Decimal("0.04"),
            fee_currency="USDT",
        )
    )
    assert fee_mismatch(event, QUOTE) is None


def test_a_fee_on_the_edge_of_the_tolerance_verifies():
    """Small tier drift is tolerated; only a real move is reported."""
    event = fee(
        Fill(
            price=Decimal(1),
            amount=Decimal(40),
            fee=Decimal("0.055"),
            fee_currency="USDT",
        )
    )
    assert fee_mismatch(event, QUOTE) is None


def test_a_fill_without_a_fee_cannot_be_verified():
    """Venues that do not report fees on updates are not mismatches."""
    assert fee_mismatch(fill_event(), QUOTE) is None


def test_a_fill_without_a_schedule_cannot_be_verified():
    """Nothing to check against, nothing to report."""
    event = fee(
        Fill(
            price=Decimal(1),
            amount=Decimal(40),
            fee=Decimal("0.04"),
            fee_currency="BTC",
        )
    )
    assert fee_mismatch(event, None) is None


def test_a_fee_outside_the_symbol_is_not_checked():
    """A schedule without the symbol cannot confirm or contradict a fee."""
    event = fee(
        Fill(
            price=Decimal(1),
            amount=Decimal(40),
            fee=Decimal("0.04"),
            fee_currency="BTC",
        )
    )
    other = FeeScheduleEvent(
        ts_recv=1,
        venue="venue_a",
        fee_currency="quote",
        symbols=[
            SymbolFees(
                symbol="ETH/USDT",
                maker=Decimal("0.001"),
                taker=Decimal("0.002"),
                min_cost=1.0,
                amount_precision=4,
            )
        ],
        source=FeeSource.MARKETS,
    )
    assert fee_mismatch(event, other) is None


def test_a_taker_fee_is_checked_against_the_taker_rate():
    """The reported liquidity picks which side of the schedule to compare."""
    event = fee(
        Fill(
            price=Decimal(1),
            amount=Decimal(40),
            fee=Decimal("0.2"),
            fee_currency="USDT",
            liquidity=Liquidity.TAKER,
        )
    )
    assert fee_mismatch(event, QUOTE) is not None


# Intent building -----------------------------------------------------------


def test_hedge_intent_is_an_opposing_market_order():
    """The hedge trades the other way, at market, on the other venue."""
    intent = hedge_intent(fill_event(), "venue_b", Decimal("40.0000"), Decimal("0.35"))
    assert intent.side is Side.BUY
    assert intent.venue == "venue_b"
    assert intent.order_type is OrderKind.MARKET
    assert intent.symbol == SYMBOL
    assert intent.intent_id == "t-250906120000_lmb_eb"
    assert intent.amount == Decimal("40.0000")
    assert intent.tags["hedge_of"] == "t-250906120000_lmb_eb"
    assert intent.tags["origin_venue"] == "venue_a"


def test_hedge_intent_of_a_buy_sells():
    """A filled buy is flattened by selling."""
    assert (
        hedge_intent(fill_event(side=Side.BUY), "venue_b", Decimal(1), Decimal(1)).side
        is Side.SELL
    )


# Selection -----------------------------------------------------------------


def test_an_open_order_is_not_hedged():
    """Only an order that can no longer fill further is hedged."""
    event = fill_event(state=OrderState.OPEN)
    assert should_hedge(event, SHOULD_MATCH, HedgeBook(10)) is None


def test_a_partial_fill_is_hedged_at_once():
    """A partial fill is hedged when reported, not when the order finishes."""
    event = fill_event(state=OrderState.PARTIALLY_FILLED, filled=Decimal("16"))
    assert should_hedge(event, SHOULD_MATCH, HedgeBook(10)) == (
        "venue_b",
        Decimal("16"),
    )


def test_later_reports_hedge_only_what_is_new():
    """Each report hedges the size added since the last one, and no more."""
    hedged = HedgeBook(10)
    key = ("venue_a", "t-250906120000_lmb_eb")
    first = fill_event(state=OrderState.PARTIALLY_FILLED, filled=Decimal("16"))
    assert should_hedge(first, SHOULD_MATCH, hedged) == ("venue_b", Decimal("16"))
    hedged.record(key, Decimal("16"))
    again = fill_event(state=OrderState.PARTIALLY_FILLED, filled=Decimal("16"))
    assert should_hedge(again, SHOULD_MATCH, hedged) is None
    more = fill_event(state=OrderState.PARTIALLY_FILLED, filled=Decimal("40"))
    assert should_hedge(more, SHOULD_MATCH, hedged) == ("venue_b", Decimal("24"))
    hedged.record(key, Decimal("24"))
    done = fill_event(state=OrderState.CANCELLED, filled=Decimal("40"))
    assert should_hedge(done, SHOULD_MATCH, hedged) is None
    assert hedged.hedges(key) == 2


def test_should_hedge_does_not_write_the_book():
    """Deciding on a hedge covers nothing; only a published hedge does."""
    hedged = HedgeBook(10)
    event = fill_event(state=OrderState.PARTIALLY_FILLED, filled=Decimal("16"))
    assert should_hedge(event, SHOULD_MATCH, hedged) == ("venue_b", Decimal("16"))
    assert should_hedge(event, SHOULD_MATCH, hedged) == ("venue_b", Decimal("16"))
    assert hedged.hedged(("venue_a", event.intent_id)) == 0


def test_a_report_behind_a_bumped_hedge_adds_nothing():
    """After a bump, the fill has to catch up with the hedge before more goes out."""
    hedged = HedgeBook(10)
    key = ("venue_a", "t-250906120000_lmb_eb")
    hedged.record(key, Decimal("103"))
    behind = fill_event(state=OrderState.PARTIALLY_FILLED, filled=Decimal("100"))
    assert should_hedge(behind, SHOULD_MATCH, hedged) is None
    ahead = fill_event(state=OrderState.PARTIALLY_FILLED, filled=Decimal("110"))
    assert should_hedge(ahead, SHOULD_MATCH, hedged) == ("venue_b", Decimal("7"))


def test_hedge_ids_stay_unique_and_attributable():
    """The first hedge keeps the order's id; later ones get a fresh stamp."""
    event = fill_event()
    assert hedge_id(event, 1) == event.intent_id
    second = hedge_id(event, 2)
    assert second != event.intent_id
    assert second.startswith("t-") and second.endswith("_lmb_eb")
    assert hedge_id(fill_event(intent_id="foreign"), 2) == "foreignh2"


def test_the_hedge_book_forgets_its_oldest_order():
    """The book is bounded, and forgets counts with sizes."""
    book = HedgeBook(2)
    book.record(("v", "a"), Decimal(1))
    book.record(("v", "b"), Decimal(1))
    book.record(("v", "c"), Decimal(1))
    assert book.hedged(("v", "a")) == 0 and book.hedges(("v", "a")) == 0
    assert book.hedged(("v", "c")) == 1


def test_a_cancelled_order_that_filled_is_still_hedged():
    """A cancellation after a partial fill still leaves a position to close."""
    event = fill_event(state=OrderState.CANCELLED, filled=Decimal("10"))
    assert should_hedge(event, SHOULD_MATCH, HedgeBook(10)) == (
        "venue_b",
        Decimal("10"),
    )


def test_an_unfilled_order_is_not_hedged():
    """Nothing filled, nothing to hedge."""
    event = fill_event(state=OrderState.CANCELLED, filled=Decimal(0))
    assert should_hedge(event, SHOULD_MATCH, HedgeBook(10)) is None


def test_an_order_of_another_strategy_is_not_hedged():
    """Only strategies that asked to be matched are matched."""
    event = fill_event(strategy_id="other")
    assert should_hedge(event, SHOULD_MATCH, HedgeBook(10)) is None


def test_an_unattributed_order_is_not_hedged():
    """An order placed outside this system belongs to no strategy."""
    event = fill_event()
    event.tags = {}
    assert should_hedge(event, SHOULD_MATCH, HedgeBook(10)) is None


def test_a_fill_on_the_taker_venue_is_not_hedged():
    """The hedge venue cannot hedge against itself."""
    event = fill_event(venue="venue_b")
    assert should_hedge(event, SHOULD_MATCH, HedgeBook(10)) is None


def test_an_order_without_a_side_is_not_hedged():
    """A hedge needs a direction, and guessing one would double a position."""
    event = fill_event()
    event.side = None
    assert should_hedge(event, SHOULD_MATCH, HedgeBook(10)) is None


@pytest.mark.asyncio
async def test_the_same_order_is_only_hedged_once():
    """A replayed terminal event must not open a second position."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}
    hedged = HedgeBook(HEDGE_MEMORY)

    first = await handle_order_event(
        redis, publisher, fill_event(), SHOULD_MATCH, hedged, schedules
    )
    replay = await handle_order_event(
        redis, publisher, fill_event(), SHOULD_MATCH, hedged, schedules
    )

    assert first is not None and replay is None
    assert len(await intents_on(redis)) == 1


def test_the_same_order_id_on_two_venues_is_hedged_separately():
    """Order ids are unique per venue, so the dedupe key carries the venue."""
    hedged = HedgeBook(HEDGE_MEMORY)
    assert should_hedge(fill_event(venue="venue_a"), SHOULD_MATCH, hedged) is not None
    assert should_hedge(fill_event(venue="venue_c"), SHOULD_MATCH, hedged) is not None


# Publication ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_fill_publishes_a_hedge_intent():
    """The hedge reaches the order manager as an intent."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}

    intent = await handle_order_event(
        redis, publisher, fill_event(), SHOULD_MATCH, HedgeBook(10), schedules
    )

    assert intent is not None
    published = (await intents_on(redis))[0]
    assert published.venue == "venue_b"
    assert published.side is Side.BUY
    assert published.order_type is OrderKind.MARKET
    assert published.strategy == "matching"


@pytest.mark.asyncio
async def test_an_event_that_needs_no_hedge_publishes_nothing():
    """Events the matcher does not act on leave the intents stream empty."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}

    intent = await handle_order_event(
        redis,
        publisher,
        fill_event(state=OrderState.OPEN),
        SHOULD_MATCH,
        HedgeBook(10),
        schedules,
    )

    assert intent is None
    assert await redis.xlen(INTENTS_STREAM) == 0


@pytest.mark.asyncio
async def test_an_unpriceable_fill_is_skipped():
    """A fill the venue gave no price for cannot be hedged."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}

    intent = await handle_order_event(
        redis,
        publisher,
        fill_event(avg_price=None),
        SHOULD_MATCH,
        HedgeBook(10),
        schedules,
    )

    assert intent is None
    assert await redis.xlen(INTENTS_STREAM) == 0


@pytest.mark.asyncio
async def test_a_fill_that_could_not_be_hedged_is_hedged_by_the_next_report():
    """A hedge that never went out covers nothing; the next report carries it."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}
    hedged = HedgeBook(10)

    unpriced = await handle_order_event(
        redis,
        publisher,
        fill_event(
            state=OrderState.PARTIALLY_FILLED, filled=Decimal("16"), avg_price=None
        ),
        SHOULD_MATCH,
        hedged,
        schedules,
    )
    final = await handle_order_event(
        redis,
        publisher,
        fill_event(state=OrderState.FILLED, filled=Decimal("40")),
        SHOULD_MATCH,
        hedged,
        schedules,
    )

    assert unpriced is None
    assert final is not None and final.amount == Decimal("40")
    assert final.intent_id == fill_event().intent_id, "this was the first hedge"
    assert len(await intents_on(redis)) == 1


@pytest.mark.asyncio
async def test_a_hedge_that_fails_to_publish_covers_nothing():
    """The book is written after the publish, so a failed one leaves it unhedged."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}
    hedged = HedgeBook(10)

    class FailingPublisher(StreamPublisher):
        async def publish(self, redis: Any, event: Any) -> str:
            raise ConnectionError("redis went away")

    with pytest.raises(ConnectionError):
        await handle_order_event(
            redis,
            FailingPublisher(maxlen=100),
            fill_event(),
            SHOULD_MATCH,
            hedged,
            schedules,
        )

    assert hedged.hedged(("venue_a", fill_event().intent_id)) == 0
    assert hedged.hedges(("venue_a", fill_event().intent_id)) == 0
    retried = await handle_order_event(
        redis,
        StreamPublisher(maxlen=100),
        fill_event(),
        SHOULD_MATCH,
        hedged,
        schedules,
    )
    assert retried is not None and retried.amount == Decimal("40")


@pytest.mark.asyncio
async def test_sub_minimum_partials_do_not_pile_up_bumped_hedges():
    """
    Twenty dust partials of one order hedge about the order, not twenty bumps.

    Each partial on its own is under the matching venue's minimum notional
    and would be bumped to clear it. The bump covers fill that has not
    happened yet, so it has to count against the next reports rather than
    be sent again on each of them.
    """
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {
        "venue_a": QUOTE,
        "venue_b": schedule("venue_b", "quote", min_cost="1.0", precision=0),
    }
    hedged = HedgeBook(10)

    for step in range(1, 21):
        state = OrderState.FILLED if step == 20 else OrderState.PARTIALLY_FILLED
        await handle_order_event(
            redis,
            publisher,
            fill_event(
                state=state, filled=Decimal(10 * step), avg_price=Decimal("0.01")
            ),
            SHOULD_MATCH,
            hedged,
            schedules,
        )

    intents = await intents_on(redis)
    total = sum(intent.amount for intent in intents)
    # One bump of 103 covers the first ten partials; the eleventh leaves 7
    # uncovered, which is dust again and is bumped once more.
    assert [intent.amount for intent in intents] == [Decimal("103"), Decimal("103")]
    assert total == Decimal("206")
    assert total < Decimal("200") + Decimal("103") * 2, "one bump at most is left over"


@pytest.mark.asyncio
async def test_a_hedge_that_sizes_to_nothing_is_not_published(caplog):
    """A zero-amount intent is an order the venue can only reject."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {
        "venue_a": QUOTE,
        "venue_b": schedule("venue_b", "quote", min_cost=None, precision=4),
    }

    intent = await handle_order_event(
        redis,
        publisher,
        fill_event(filled=Decimal("0.00001")),
        SHOULD_MATCH,
        HedgeBook(10),
        schedules,
    )

    assert intent is None
    assert await redis.xlen(INTENTS_STREAM) == 0
    assert any("leaving it unhedged" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_a_hedge_without_a_schedule_still_goes_out(caplog):
    """A missing schedule is logged loudly, and the hedge is still placed."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    intent = await handle_order_event(
        redis, publisher, fill_event(), SHOULD_MATCH, HedgeBook(10), {}
    )

    assert intent is not None
    assert any("without a fee schedule" in r.message for r in caplog.records)


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
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}
    # A fill from before the matcher started must not be hedged.
    await publisher.publish(redis, fill_event(intent_id="old"))

    cursor = await stream_tail(redis, ORDER_EVENTS_STREAM)
    consumer = asyncio.create_task(
        consume_order_events(
            redis,
            publisher,
            SHOULD_MATCH,
            10,
            10,
            schedules,
            {ORDER_EVENTS_STREAM: cursor},
        )
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
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}

    cursor = await stream_tail(redis, ORDER_EVENTS_STREAM)
    consumer = asyncio.create_task(
        consume_order_events(
            redis,
            publisher,
            SHOULD_MATCH,
            10,
            10,
            schedules,
            {ORDER_EVENTS_STREAM: cursor},
        )
    )
    await asyncio.sleep(0.05)
    await redis.xadd(ORDER_EVENTS_STREAM, {"type": "order_event", "data": "{"})
    await publisher.publish(redis, fill_event(intent_id="after"))
    await asyncio.sleep(0.15)
    consumer.cancel()

    assert [i.intent_id for i in await intents_on(redis)] == ["after"]


@pytest.mark.asyncio
async def test_consume_order_events_picks_up_a_refreshed_fee_schedule():
    """A schedule the fee watcher republishes replaces the one loaded at startup."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    # The startup schedule charges nothing, so a hedge is sized one for one.
    schedules = {
        "venue_a": QUOTE,
        "venue_b": schedule("venue_b", "received", taker="0", precision=2),
    }
    cursors = await resolve_cursors(redis, {"venue_a", "venue_b"})
    assert set(cursors) == {
        ORDER_EVENTS_STREAM,
        fees_stream("venue_a"),
        fees_stream("venue_b"),
    }
    consumer = asyncio.create_task(
        consume_order_events(redis, publisher, SHOULD_MATCH, 10, 10, schedules, cursors)
    )
    await asyncio.sleep(0.05)

    await publisher.publish(redis, fill_event(intent_id="before"))
    await asyncio.sleep(0.15)
    # The account moves tier: the venue now charges 0.2% taker in base.
    await publisher.publish(redis, schedule("venue_b", "received", precision=2))
    await asyncio.sleep(0.15)
    await publisher.publish(redis, fill_event(intent_id="after"))
    await asyncio.sleep(0.15)

    assert not consumer.done(), "the consumer died on a fee schedule"
    consumer.cancel()

    intents = {intent.intent_id: intent for intent in await intents_on(redis)}
    assert intents["before"].amount == Decimal("40.00")
    assert intents["after"].amount == Decimal("40.08")
    assert schedules["venue_b"].symbols[0].taker == Decimal("0.002")


# Schedule loading ----------------------------------------------------------


@pytest.mark.asyncio
async def test_load_schedules_reads_the_snapshot_keys():
    """Published schedules are picked up for every declared venue."""
    from apps.shared.src import events
    from apps.shared.src.fees import fees_snapshot_key

    redis = fakeredis.FakeRedis(decode_responses=True)
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(
            VenueConfig(id="venue_a", name="A"),
            VenueConfig(id="venue_b", name="B"),
        ),
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=True,
                subscriptions=(Subscription(venue="venue_a", symbol=SYMBOL),),
                params={},
            ),
        ),
    )
    await redis.set(fees_snapshot_key("venue_a"), events.encode(QUOTE))
    await redis.set(
        fees_snapshot_key("venue_b"), events.encode(schedule("venue_b", "received"))
    )

    schedules = await load_schedules(redis, config.venue_ids)

    assert set(schedules) == {"venue_a", "venue_b"}
    assert schedules["venue_b"].fee_currency == "received"


@pytest.mark.asyncio
async def test_load_schedules_gives_up_on_venues_that_never_publish(monkeypatch):
    """Past the deadline the matcher starts with what it has, loudly."""
    from apps.shared.src import events
    from apps.shared.src.fees import fees_snapshot_key

    monkeypatch.setattr(matcher, "SCHEDULE_WAIT_S", 0.05)
    monkeypatch.setattr(matcher, "SCHEDULE_POLL_S", 0.01)
    redis = fakeredis.FakeRedis(decode_responses=True)
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(
            VenueConfig(id="venue_a", name="A"),
            VenueConfig(id="venue_b", name="B"),
        ),
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=True,
                subscriptions=(Subscription(venue="venue_a", symbol=SYMBOL),),
                params={},
            ),
        ),
    )
    await redis.set(fees_snapshot_key("venue_a"), events.encode(QUOTE))

    schedules = await load_schedules(redis, config.venue_ids)

    assert set(schedules) == {"venue_a"}


@pytest.mark.asyncio
async def test_load_schedules_survives_an_undecodable_key(monkeypatch):
    """A corrupted snapshot delays the venue rather than killing startup."""
    monkeypatch.setattr(matcher, "SCHEDULE_WAIT_S", 0.05)
    monkeypatch.setattr(matcher, "SCHEDULE_POLL_S", 0.01)
    redis = fakeredis.FakeRedis(decode_responses=True)
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(VenueConfig(id="venue_a", name="A"),),
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=True,
                subscriptions=(Subscription(venue="venue_a", symbol=SYMBOL),),
                params={},
            ),
        ),
    )
    await redis.set("fees-venue_a", "{")

    schedules = await load_schedules(redis, config.venue_ids)

    assert schedules == {}


# Configuration -------------------------------------------------------------


def test_matching_venues_reads_every_strategy_that_hedges():
    """Matching applies regardless of the production flag."""
    subscription = Subscription(venue="venue_a", symbol=SYMBOL)
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(
            VenueConfig(id="venue_a", name="A"),
            VenueConfig(id="venue_b", name="B"),
        ),
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=False,
                subscriptions=(subscription,),
                params={"should_match": True, "taker_exchange": "venue_b"},
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

    assert matching_venues(config) == {"lmb": "venue_b"}


def test_hedging_venues_covers_both_legs_and_nothing_else():
    """
    Startup waits for the venues a hedge prices, not every declared one.

    A venue declared for market data alone has no fee watcher behind it, so
    waiting for its schedule burns the whole deadline on every start.
    """
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(
            VenueConfig(id="venue_a", name="A"),
            VenueConfig(id="venue_b", name="B"),
            VenueConfig(id="venue_c", name="C"),
        ),
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=True,
                subscriptions=(Subscription(venue="venue_a", symbol=SYMBOL),),
                params={"should_match": True, "taker_exchange": "venue_b"},
            ),
            StrategyConfig(
                identifier="paper",
                type="single_edge_liquidity",
                production=False,
                subscriptions=(Subscription(venue="venue_c", symbol=SYMBOL),),
                params={"should_match": True, "taker_exchange": "venue_c"},
            ),
        ),
    )

    assert hedging_venues(config, True) == {"venue_a", "venue_b"}
    assert hedging_venues(config, False) == {"venue_c"}


def test_hedging_venues_is_empty_when_nothing_hedges():
    """Nothing to hedge means nothing to wait for."""
    config = AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(VenueConfig(id="venue_a", name="A"),),
        strategies=(
            StrategyConfig(
                identifier="plain",
                type="take_take",
                production=True,
                subscriptions=(Subscription(venue="venue_a", symbol=SYMBOL),),
                params={},
            ),
        ),
    )

    assert hedging_venues(config, True) == set()


@pytest.mark.asyncio
async def test_load_schedules_does_not_wait_for_nothing(monkeypatch):
    """An empty venue set returns at once rather than sitting out the wait."""
    monkeypatch.setattr(matcher, "SCHEDULE_WAIT_S", 30)
    monkeypatch.setattr(matcher, "SCHEDULE_POLL_S", 30)
    redis = fakeredis.FakeRedis(decode_responses=True)

    schedules = await asyncio.wait_for(load_schedules(redis, set()), timeout=1)

    assert schedules == {}


@pytest.mark.asyncio
async def test_a_fill_during_the_startup_wait_is_still_hedged():
    """
    The cursor is resolved before the schedules are waited for.

    The orchestrator starts the matcher ahead of the fee watcher, so the
    wait for schedules runs on every cold start. A cursor taken after it
    would skip every fill published meanwhile, silently.
    """
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}
    await publisher.publish(redis, fill_event(intent_id="old"))

    cursor = await stream_tail(redis, ORDER_EVENTS_STREAM)
    # Stands in for the schedule wait between resolving the cursor and the
    # first read.
    await publisher.publish(redis, fill_event(intent_id="during_the_wait"))

    consumer = asyncio.create_task(
        consume_order_events(
            redis,
            publisher,
            SHOULD_MATCH,
            10,
            10,
            schedules,
            {ORDER_EVENTS_STREAM: cursor},
        )
    )
    await asyncio.sleep(0.1)
    consumer.cancel()

    assert [i.intent_id for i in await intents_on(redis)] == ["during_the_wait"]


@pytest.mark.asyncio
async def test_a_second_increment_is_published_under_a_fresh_id():
    """Two hedges of one order must not share a client order id at the venue."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    schedules = {"venue_a": QUOTE, "venue_b": schedule("venue_b", "quote")}
    hedged = HedgeBook(10)

    first = await handle_order_event(
        redis,
        publisher,
        fill_event(state=OrderState.PARTIALLY_FILLED, filled=Decimal("16")),
        SHOULD_MATCH,
        hedged,
        schedules,
    )
    second = await handle_order_event(
        redis,
        publisher,
        fill_event(state=OrderState.FILLED, filled=Decimal("40")),
        SHOULD_MATCH,
        hedged,
        schedules,
    )
    assert first is not None and second is not None
    assert first.intent_id == fill_event().intent_id
    assert second.intent_id != first.intent_id
    assert second.amount == Decimal("24")
    assert second.tags["hedge_of"] == fill_event().intent_id
    assert len(await intents_on(redis)) == 2
