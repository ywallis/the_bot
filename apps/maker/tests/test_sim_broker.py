"""Tests for the simulated broker."""

import argparse
import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import sim_broker as sb
from apps.maker.src.latency import LatencyModel
from apps.shared.src.config import (
    AppConfig,
    BacktestConfig,
    FeeConfig,
    MarketDataConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
    VenueConfig,
)
from apps.shared.src.events import (
    INTENTS_STREAM,
    LATENCY_STREAM,
    ORDER_EVENTS_STREAM,
    REPLACE_RESTING,
    AnyEvent,
    AssetBalance,
    BalanceEvent,
    BookEvent,
    CancelIntent,
    LatencyRecord,
    Liquidity,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    TimeInForce,
    TradeEvent,
    balance_stream,
    from_stream_fields,
    prefixed,
)
from apps.shared.src.streams import (
    StreamPublisher,
    replay_frontier_key,
    replay_closed_key,
    replay_done_key,
    replay_progress_key,
)
from apps.shared.src.utils import production

PREFIX = "bt:t"
A, B = "venue_a", "venue_b"
SYMBOL = "BASE/QUOTE"
T0 = 1_788_703_200_000_000_000
MS = 1_000_000
S = 1_000_000_000
# Assumed round trips: every leg is then a constant and every time exact.
RTT_A, RTT_B = 300 * MS, 400 * MS
OMS_LAG = 600_000  # the assumed model's strategy-to-order-manager leg


def config() -> AppConfig:
    """Return two venues, one strategy on both with books and trades, fees on both."""
    return AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(VenueConfig(id=A, name="A"), VenueConfig(id=B, name="B")),
        strategies=(
            StrategyConfig(
                identifier="s",
                type="t",
                production=production,
                subscriptions=(
                    Subscription(venue=A, symbol=SYMBOL, feeds=("book", "trade")),
                    Subscription(venue=B, symbol=SYMBOL, feeds=("book", "trade")),
                ),
                params={},
            ),
        ),
        backtest=BacktestConfig(
            fees={
                A: FeeConfig(maker=0.0, taker=0.001),
                B: FeeConfig(maker=0.0005, taker=0.001),
            },
            idle_s=0.0,
        ),
    )


def latency() -> LatencyModel:
    """Return a model that assumes both venues' round trips."""
    model = LatencyModel()
    model.assume(A, RTT_A / MS)
    model.assume(B, RTT_B / MS)
    return model


def book(
    venue: str, ts: int, bids: list[list[float]], asks: list[list[float]], seq: int = 1
) -> BookEvent:
    """Return a book."""
    return BookEvent(
        ts_recv=ts,
        venue=venue,
        symbol=SYMBOL,
        seq=seq,
        ts_exch=None,
        bids=bids,
        asks=asks,
    )


def trade(venue: str, ts: int, price: float, amount: float) -> TradeEvent:
    """Return a trade."""
    return TradeEvent(
        ts_recv=ts,
        venue=venue,
        symbol=SYMBOL,
        seq=1,
        ts_exch=None,
        trade_id=None,
        side=None,
        price=price,
        amount=amount,
    )


def balance(
    venue: str, ts: int, base: float = 1000.0, quote: float = 500.0
) -> BalanceEvent:
    """Return a balance snapshot with everything free."""
    return BalanceEvent(
        ts_recv=ts,
        venue=venue,
        seq=1,
        ts_exch=None,
        balances={
            "BASE": AssetBalance(free=base, used=0.0, total=base),
            "QUOTE": AssetBalance(free=quote, used=0.0, total=quote),
        },
    )


def intent(
    ts: int,
    intent_id: str,
    side: Side = Side.SELL,
    amount: str = "10",
    price: str | None = "0.35",
    venue: str = A,
    replace_of: str | None = REPLACE_RESTING,
    order_type: OrderKind = OrderKind.LIMIT,
    tif: TimeInForce = TimeInForce.GTC,
    strategy: str = "s_es",
) -> OrderIntent:
    """Return an order intent."""
    return OrderIntent(
        ts_recv=ts,
        intent_id=intent_id,
        strategy=strategy,
        venue=venue,
        symbol=SYMBOL,
        side=side,
        order_type=order_type,
        amount=Decimal(amount),
        price=None if price is None else Decimal(price),
        time_in_force=tif,
        replace_of=replace_of,
    )


def cancel(
    ts: int, target: str, strategy: str = "s_es", venue: str = A
) -> CancelIntent:
    """Return a cancel intent."""
    return CancelIntent(
        ts_recv=ts,
        intent_id=f"c-{ts}",
        strategy=strategy,
        venue=venue,
        symbol=SYMBOL,
        target_intent_id=target,
    )


class Harness:
    """A broker on a fake Redis, with helpers to publish and read back."""

    def __init__(
        self,
        follow: list[str] | None = None,
        decode: bool = False,
        participation: float | None = None,
    ) -> None:
        """Build the broker over a fake Redis."""
        self.redis = fakeredis.FakeRedis(decode_responses=decode)
        self.publisher = StreamPublisher(maxlen=1000, prefix=PREFIX)
        self.broker = sb.SimulatedBroker(
            self.redis,
            config(),
            PREFIX,
            latency(),
            follow=follow or [],
            block_ms=1,
            participation=participation,
        )

    async def start(self) -> None:
        """Start the reader and adopt opening balances on both venues."""
        await self.broker.reader.start()
        await self.publish(balance(A, T0 - S), balance(B, T0 - S))

    async def publish(self, *events: AnyEvent) -> int:
        """Publish under the prefix and let the broker read."""
        for event in events:
            await self.publisher.publish(self.redis, event)
        return await self.broker.step()

    async def advance(self, ts: int) -> None:
        """Run the broker's timeline up to a time."""
        await self.broker.run_until(ts)

    async def events(self) -> list[OrderEvent]:
        """Return every order event published, oldest first."""
        entries = await self.redis.xrange(prefixed(PREFIX, ORDER_EVENTS_STREAM))
        return cast(list[OrderEvent], [from_stream_fields(f) for _, f in entries])

    async def states(self) -> list[tuple[str, str, int]]:
        """Return (intent id, state, ts_recv) per order event."""
        return [(e.intent_id, e.state.value, e.ts_recv) for e in await self.events()]

    async def balances(self, venue: str) -> list[BalanceEvent]:
        """Return every balance event on a venue, oldest first."""
        entries = await self.redis.xrange(prefixed(PREFIX, balance_stream(venue)))
        return cast(list[BalanceEvent], [from_stream_fields(f) for _, f in entries])

    async def latency_records(self) -> list[LatencyRecord]:
        """Return every latency record, oldest first."""
        entries = await self.redis.xrange(prefixed(PREFIX, LATENCY_STREAM))
        return cast(list[LatencyRecord], [from_stream_fields(f) for _, f in entries])


# Pure pieces ---------------------------------------------------------------


def test_crossing_fills_walk_the_far_side_up_to_the_limit():
    """A buy takes asks from the best while they are at or below its limit."""
    b = book(A, T0, bids=[[0.30, 5]], asks=[[0.35, 4], [0.36, 3], [0.40, 10]])
    fills, remaining = sb.crossing_fills(b, Side.BUY, Decimal(10), Decimal("0.36"))
    assert fills == [(Decimal("0.35"), Decimal(4)), (Decimal("0.36"), Decimal(3))]
    assert remaining == Decimal(3)
    fills, remaining = sb.crossing_fills(b, Side.BUY, Decimal(10), None)
    assert [f[1] for f in fills] == [Decimal(4), Decimal(3), Decimal(3)]
    assert remaining == 0
    fills, remaining = sb.crossing_fills(b, Side.SELL, Decimal(2), Decimal("0.31"))
    assert fills == [] and remaining == 2
    fills, remaining = sb.crossing_fills(b, Side.SELL, Decimal(2), Decimal("0.30"))
    assert fills == [(Decimal("0.30"), Decimal(2))]


def test_size_at_and_crosses_read_the_resting_side():
    """A sell rests among the asks; a bid at or above it crosses it."""
    b = book(A, T0, bids=[[0.34, 5]], asks=[[0.35, 4], [0.36, 3]])
    assert sb.size_at(b, Side.SELL, Decimal("0.36")) == 3
    assert sb.size_at(b, Side.SELL, Decimal("0.37")) is None
    assert sb.size_at(b, Side.BUY, Decimal("0.34")) == 5
    assert not sb.crosses(b, Side.SELL, Decimal("0.35"))
    assert sb.crosses(b, Side.SELL, Decimal("0.34"))
    assert sb.crosses(b, Side.BUY, Decimal("0.35"))
    assert not sb.crosses(b, Side.BUY, Decimal("0.34"))
    assert not sb.crosses(
        book(A, T0, bids=[], asks=[[0.35, 1]]), Side.SELL, Decimal("0.1")
    )


def test_market_history_serves_the_book_of_a_past_moment():
    """An order arriving between two books is matched against the earlier one."""
    market = sb.Market(history_ns=10 * S)
    assert market.book_at(T0) is None
    market.record(book(A, T0, [[0.30, 1]], [[0.35, 1]], seq=1))
    market.record(book(A, T0 + S, [[0.31, 1]], [[0.36, 1]], seq=2))
    market.record(book(A, T0 + 5 * S, [[0.32, 1]], [[0.37, 1]], seq=3))
    assert market.book_at(T0 + S // 2).seq == 1  # type: ignore[union-attr]
    assert market.book_at(T0 + 3 * S).seq == 2  # type: ignore[union-attr]
    assert market.book_at(T0 + 30 * S).seq == 3  # type: ignore[union-attr]
    assert market.book_at(T0 - S).seq == 1  # type: ignore[union-attr]
    # A book ten seconds on ages out everything older than the history.
    market.record(book(A, T0 + 12 * S, [[0.33, 1]], [[0.38, 1]], seq=4))
    assert [b.seq for b in market.history] == [3, 4]
    assert market.book_at(T0 - S).seq == 3  # type: ignore[union-attr]


def test_split_strategy_key():
    """A key is identifier and slot; a bare identifier has no slot."""
    assert sb.split_strategy_key("fmb_es") == ("fmb", "es")
    assert sb.split_strategy_key("matching") == ("matching", "")


# Placement -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_resting_sell_is_accepted_opened_and_held():
    """ACCEPTED at the order manager, OPEN a round trip later, the base held meanwhile."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50], [0.36, 20]]))
    await h.publish(intent(T0, "i1", amount="10", price="0.36"))
    await h.advance(T0 + S)
    oms = T0 + OMS_LAG
    assert await h.states() == [("i1", "accepted", oms), ("i1", "open", oms + RTT_A)]
    order = h.broker.orders[(A, "i1")]
    assert order.queue_ahead == 20  # behind what the book showed at 0.36
    assert order.held == 10 and order.held_asset == "BASE"
    assert h.broker.resting["s_es"] == (A, "i1")
    assert h.broker.report.placed == 1
    # The hold is published when the balance watcher would have seen it.
    published = await h.balances(A)
    assert published[-1].ts_recv == oms + RTT_A // 2 + RTT_A // 2
    assert published[-1].balances["BASE"].used == 10.0
    assert published[-1].balances["BASE"].free == 990.0
    records = await h.latency_records()
    assert len(records) == 1
    assert records[0].ts_created == T0
    assert records[0].ts_oms_recv == oms
    assert records[0].ts_broker_send == oms
    assert records[0].ts_broker_ack == oms + RTT_A
    assert records[0].ts_venue_ack == oms + RTT_A // 2
    open_event = (await h.events())[-1]
    assert open_event.venue_order_id == "sim-1"
    assert open_event.tags == {"strategy_id": "s", "order_id": "es"}


@pytest.mark.asyncio
async def test_a_trade_through_the_price_fills_no_more_than_it_printed():
    """A print above a resting sell reaches it, but fills only what the print held."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50]]))
    await h.publish(intent(T0, "i1", amount="10", price="0.36"))
    await h.publish(
        book(A, T0 + S, [[0.34, 100]], [[0.35, 50]], seq=2)
    )  # order is live now
    await h.publish(trade(A, T0 + 2 * S, price=0.37, amount=1.0))
    await h.advance(T0 + 3 * S)
    states = await h.states()
    assert [s for _, s, _ in states] == ["accepted", "open", "partially_filled"]
    assert states[-1][2] == T0 + 2 * S + RTT_A // 2  # reported half a round trip later
    event = (await h.events())[-1]
    assert event.filled == 1 and event.remaining == 9  # the print, not the order
    assert event.avg_price == Decimal("0.36")  # still at its own price
    assert event.last_fill is not None
    assert event.last_fill.amount == 1
    assert event.last_fill.liquidity is Liquidity.MAKER
    assert event.last_fill.fee == 0  # maker fee on A is zero
    assert event.last_fill.fee_currency == "QUOTE"
    assert (A, "i1") in h.broker.orders and "s_es" in h.broker.resting  # still resting
    last = (await h.balances(A))[-1]
    assert last.balances["BASE"].total == pytest.approx(999.0)
    assert last.balances["QUOTE"].total == pytest.approx(500.0 + 0.36)
    assert h.broker.report.fills == 1
    assert h.broker.report.volume[A] == 1


@pytest.mark.asyncio
async def test_the_opening_balance_drops_the_recorded_holds():
    """A hold in the recording is the live run's order, not this run's."""
    h = Harness()
    await h.broker.reader.start()
    held = BalanceEvent(
        ts_recv=T0 - S,
        venue=A,
        seq=1,
        ts_exch=None,
        balances={
            "BASE": AssetBalance(free=634.0, used=366.0, total=1000.0),
            "QUOTE": AssetBalance(free=500.0, used=0.0, total=500.0),
        },
    )
    await h.publish(held)
    free, used = h.broker.balances.venues[A]["BASE"]
    assert free == 1000 and used == 0  # not 634 free and 366 held
    # The report's opening is the total either way, so runs stay comparable.
    assert h.broker.balances.opening[A]["BASE"] == 1000


@pytest.mark.asyncio
async def test_participation_takes_only_a_share_of_the_print():
    """A share below one means the print filled others too, as it did in reality."""
    h = Harness(participation=0.25)
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50]]))
    await h.publish(intent(T0, "i1", amount="10", price="0.36"))
    await h.publish(book(A, T0 + S, [[0.34, 100]], [[0.35, 50]], seq=2))
    await h.publish(trade(A, T0 + 2 * S, price=0.37, amount=8.0))
    await h.advance(T0 + 3 * S)
    event = (await h.events())[-1]
    assert event.last_fill is not None
    assert event.last_fill.amount == 2  # a quarter of the eight that printed
    assert event.state is OrderState.PARTIALLY_FILLED
    assert h.broker.report.volume[A] == 2


@pytest.mark.asyncio
async def test_a_trade_at_the_price_consumes_the_queue_first():
    """Only what is left after the size ahead of us fills us, partially then whole."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50], [0.36, 20]]))
    await h.publish(intent(T0, "i1", amount="10", price="0.36"))
    await h.publish(book(A, T0 + S, [[0.34, 100]], [[0.35, 50], [0.36, 20]], seq=2))
    await h.publish(
        trade(A, T0 + 2 * S, price=0.36, amount=15.0)
    )  # 20 ahead, nothing for us
    await h.publish(
        trade(A, T0 + 3 * S, price=0.36, amount=8.0)
    )  # 5 ahead left, 3 for us
    await h.publish(trade(A, T0 + 4 * S, price=0.36, amount=100.0))  # the rest
    await h.advance(T0 + 5 * S)
    states = [(s, ts) for _, s, ts in await h.states()]
    assert states[2:] == [
        ("partially_filled", T0 + 3 * S + RTT_A // 2),
        ("filled", T0 + 4 * S + RTT_A // 2),
    ]
    events = await h.events()
    assert events[2].last_fill is not None and events[2].last_fill.amount == 3
    assert events[3].last_fill is not None and events[3].last_fill.amount == 7
    assert events[3].filled == 10


@pytest.mark.asyncio
async def test_the_book_can_shrink_the_queue_and_cross_the_order():
    """Less shown at our level means less ahead; a bid at our ask fills us at our price."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50], [0.36, 20]]))
    await h.publish(intent(T0, "i1", amount="10", price="0.36"))
    await h.publish(book(A, T0 + S, [[0.34, 100]], [[0.35, 50], [0.36, 4]], seq=2))
    assert h.broker.orders[(A, "i1")].queue_ahead == 4
    await h.publish(
        book(A, T0 + 2 * S, [[0.34, 100]], [[0.35, 50]], seq=3)
    )  # level gone
    assert h.broker.orders[(A, "i1")].queue_ahead == 0
    await h.publish(
        book(A, T0 + 3 * S, [[0.37, 100]], [[0.38, 50]], seq=4)
    )  # bid through us
    await h.advance(T0 + 4 * S)
    filled = (await h.events())[-1]
    assert filled.state is OrderState.FILLED
    assert filled.avg_price == Decimal("0.36")


@pytest.mark.asyncio
async def test_a_market_buy_fills_against_the_book_of_its_arrival_at_taker_fee():
    """Levels are taken from the best; the fee comes out of the base received."""
    h = Harness()
    await h.start()
    await h.publish(book(B, T0, [[0.34, 100]], [[0.35, 4], [0.36, 3]]))
    # The book moves before the order arrives half a round trip later.
    await h.publish(
        book(B, T0 + 100 * MS, [[0.34, 100]], [[0.40, 4], [0.41, 3]], seq=2)
    )
    await h.publish(
        intent(
            T0,
            "h1",
            side=Side.BUY,
            amount="5",
            price=None,
            venue=B,
            replace_of=None,
            order_type=OrderKind.MARKET,
            strategy="matching",
        )
    )
    await h.advance(T0 + S)
    events = await h.events()
    # The reply and the fill reports reach the bus at the same instant; the
    # reply was on its way first.
    assert [e.state.value for e in events] == [
        "accepted",
        "open",
        "partially_filled",
        "filled",
    ]
    fills = [e.last_fill for e in events if e.last_fill is not None]
    assert [(f.price, f.amount) for f in fills] == [
        (Decimal("0.40"), Decimal(4)),
        (Decimal("0.41"), Decimal(1)),
    ]
    assert all(
        f.liquidity is Liquidity.TAKER and f.fee_currency == "BASE" for f in fills
    )
    assert fills[0].fee == Decimal("4") * Decimal("0.001")
    assert events[3].avg_price == (Decimal("0.40") * 4 + Decimal("0.41")) / 5
    assert events[0].tags == {"strategy_id": "matching", "order_id": ""}
    # A market order never rests and leaves the book once acknowledged.
    assert (B, "h1") not in h.broker.orders
    last = (await h.balances(B))[-1]
    assert last.balances["QUOTE"].total == pytest.approx(500.0 - (0.40 * 4 + 0.41))
    assert last.balances["BASE"].total == pytest.approx(1000.0 + 5 - 0.005)


@pytest.mark.asyncio
async def test_a_market_order_beyond_the_recorded_depth_fills_at_the_worst_level():
    """The recording shows twenty levels; a bigger order pays the last one and is counted."""
    h = Harness()
    await h.start()
    await h.publish(book(B, T0, [[0.34, 100]], [[0.35, 4]]))
    await h.publish(
        intent(
            T0,
            "h1",
            side=Side.BUY,
            amount="10",
            price=None,
            venue=B,
            replace_of=None,
            order_type=OrderKind.MARKET,
            strategy="matching",
        )
    )
    await h.advance(T0 + S)
    events = await h.events()
    assert [e.state.value for e in events] == [
        "accepted",
        "open",
        "partially_filled",
        "filled",
    ]
    assert events[-1].avg_price == Decimal("0.35")
    assert h.broker.report.depth_exhausted == 1


@pytest.mark.asyncio
async def test_post_only_and_immediate_or_cancel_are_honoured():
    """Post-only that would cross is rejected; IOC fills what crosses and cancels the rest."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 4]]))
    await h.publish(
        intent(
            T0,
            "p1",
            side=Side.BUY,
            amount="5",
            price="0.35",
            replace_of=None,
            tif=TimeInForce.POST_ONLY,
            strategy="s_po",
        ),
        intent(
            T0,
            "q1",
            side=Side.BUY,
            amount="5",
            price="0.35",
            replace_of=None,
            tif=TimeInForce.IOC,
            strategy="s_ioc",
        ),
    )
    await h.advance(T0 + S)
    by_id: dict[str, list[str]] = {}
    for e in await h.events():
        by_id.setdefault(e.intent_id, []).append(e.state.value)
    assert by_id["p1"] == ["accepted", "rejected"]
    assert by_id["q1"] == ["accepted", "open", "partially_filled", "cancelled"]
    q1 = [
        e
        for e in await h.events()
        if e.intent_id == "q1" and e.state is OrderState.CANCELLED
    ][0]
    assert q1.filled == 4 and q1.remaining == 1
    assert h.broker.report.rejected == 1


# Superseding and cancelling ---------------------------------------------------


@pytest.mark.asyncio
async def test_a_superseding_quote_cancels_what_rests_before_it_places():
    """Cancel round trip, then placement; the slot points at the new order."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50]]))
    await h.publish(intent(T0, "i1", price="0.36"))
    await h.publish(book(A, T0 + S, [[0.34, 100]], [[0.35, 50]], seq=2))
    await h.publish(intent(T0 + S, "i2", price="0.37", replace_of="i1"))
    await h.advance(T0 + 3 * S)
    states = await h.states()
    oms2 = T0 + S + OMS_LAG
    assert states[2:] == [
        ("i1", "cancelled", oms2 + RTT_A),
        ("i2", "accepted", oms2 + RTT_A),
        ("i2", "open", oms2 + 2 * RTT_A),
    ]
    cancelled = [e for e in await h.events() if e.state is OrderState.CANCELLED][0]
    assert cancelled.reason == "replaced by i2"
    assert h.broker.resting["s_es"] == (A, "i2")
    assert (A, "i1") not in h.broker.orders
    # The hold moved from the old order to the new one.
    last = (await h.balances(A))[-1]
    assert last.balances["BASE"].used == 10.0
    assert h.broker.report.cancelled == 1


@pytest.mark.asyncio
async def test_quotes_arriving_while_busy_coalesce_latest_wins():
    """The queued quote is replaced by a newer one and rejected as superseded."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50]]))
    await h.publish(intent(T0, "i1", price="0.36"))
    # Two more quotes before i1's round trip completes.
    await h.publish(intent(T0 + 10 * MS, "i2", price="0.37", replace_of="i1"))
    await h.publish(intent(T0 + 20 * MS, "i3", price="0.38", replace_of="i2"))
    await h.advance(T0 + 3 * S)
    states = [(i, s) for i, s, _ in await h.states()]
    assert states == [
        ("i1", "accepted"),
        ("i2", "rejected"),
        ("i1", "open"),
        ("i1", "cancelled"),
        ("i3", "accepted"),
        ("i3", "open"),
    ]
    rejected = [e for e in await h.events() if e.state is OrderState.REJECTED][0]
    assert rejected.reason == "superseded by i3"
    assert h.broker.resting["s_es"] == (A, "i3")


@pytest.mark.asyncio
async def test_a_cancel_intent_cancels_by_target_or_whatever_rests():
    """A named target and an empty one both reach the resting order."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50]]))
    await h.publish(intent(T0, "i1", price="0.36"))
    await h.publish(book(A, T0 + S, [[0.34, 100]], [[0.35, 50]], seq=2))
    await h.publish(cancel(T0 + S, "i1"))
    await h.advance(T0 + 3 * S)
    states = await h.states()
    assert states[-1] == ("i1", "cancelled", T0 + S + OMS_LAG + RTT_A)
    assert (await h.events())[-1].reason == "cancelled on request"
    assert "s_es" not in h.broker.resting
    last = (await h.balances(A))[-1]
    assert last.balances["BASE"].used == 0.0 and last.balances["BASE"].free == 1000.0

    await h.publish(intent(T0 + 4 * S, "i2", price="0.36"))
    await h.publish(book(A, T0 + 5 * S, [[0.34, 100]], [[0.35, 50]], seq=3))
    await h.publish(cancel(T0 + 5 * S, ""))  # whatever rests
    await h.advance(T0 + 7 * S)
    assert (await h.states())[-1][:2] == ("i2", "cancelled")
    assert h.broker.orders == {}
    # Nothing to cancel is a warning, not an error, and frees the key.
    await h.publish(cancel(T0 + 8 * S, ""))
    await h.advance(T0 + 9 * S)
    assert "s_es" not in h.broker.busy


@pytest.mark.asyncio
async def test_a_cancel_that_arrives_after_the_fill_changes_nothing():
    """The venue no longer has the order; the fill stands and no cancelled event follows."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50]]))
    await h.publish(intent(T0, "i1", price="0.36"))
    await h.publish(book(A, T0 + S, [[0.34, 100]], [[0.35, 50]], seq=2))
    # The cancel is created at T0+1s and reaches the venue at +150.6ms;
    # a trade fills the order at T0+1s+100ms, before that.
    await h.publish(cancel(T0 + S, "i1"))
    # The print carries the whole order: a fill is capped by what printed.
    await h.publish(trade(A, T0 + S + 100 * MS, price=0.40, amount=10.0))
    await h.advance(T0 + 3 * S)
    states = [s for _, s, _ in await h.states()]
    assert states == ["accepted", "open", "filled"]
    assert h.broker.report.cancelled == 0
    assert "s_es" not in h.broker.busy


# Coordination and shutdown ------------------------------------------------------


@pytest.mark.asyncio
async def test_market_events_wait_for_the_followed_strategies():
    """A cancel created before a trade, in recorded time, beats it whatever the publish order."""
    h = Harness(follow=["s"])
    await h.start()
    # Nothing is processed until the strategy reports progress at all.
    assert h.broker.buffer and not h.broker.balances.adopted
    await h.redis.set(replay_progress_key(PREFIX, "s"), T0)
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50]]))
    await h.publish(intent(T0, "i1", price="0.36"))
    await h.publish(book(A, T0 + S, [[0.34, 100]], [[0.35, 50]], seq=2))
    # The trade at T0+1.2s is published before the strategy's cancel at T0+1s.
    await h.publish(trade(A, T0 + S + 200 * MS, price=0.40, amount=1.0))
    assert [type(b.event).__name__ for b in h.broker.buffer] == [
        "BookEvent",
        "TradeEvent",
    ]
    await h.redis.set(replay_progress_key(PREFIX, "s"), T0 + S)
    await h.publish(cancel(T0 + S, "i1"))
    assert [type(b.event).__name__ for b in h.broker.buffer] == ["TradeEvent"]
    await h.redis.set(replay_progress_key(PREFIX, "s"), T0 + 2 * S)
    await h.broker.step()
    await h.advance(T0 + 3 * S)
    states = [s for _, s, _ in await h.states()]
    # The cancel reached the venue at T0+1s+150.6ms, before the trade at T0+1.2s.
    assert states == ["accepted", "open", "cancelled"]
    assert (
        int(await h.redis.get(replay_progress_key(PREFIX, "sim"))) >= T0 + S + 200 * MS
    )


@pytest.mark.asyncio
async def test_finished_needs_done_progress_tails_and_idle():
    """Every condition for winding down is checked."""
    h = Harness(follow=["s"])
    await h.start()
    assert not await h.broker.finished()
    await h.redis.set(replay_done_key(PREFIX), T0 + S)
    assert not await h.broker.finished()  # strategy not through, buffer not empty
    await h.redis.set(replay_progress_key(PREFIX, "s"), T0 + S)
    await h.broker.step()
    assert h.broker.buffer == []
    assert await h.broker.finished()
    await h.redis.set(replay_progress_key(PREFIX, "s"), T0)
    assert not await h.broker.finished()


@pytest.mark.asyncio
async def test_run_winds_down_cancelling_what_rests_and_reports():
    """At the end resting orders are cancelled as at shutdown and totals reported."""
    h = Harness(follow=["s"])
    await h.redis.set(replay_progress_key(PREFIX, "s"), T0 + 2 * S)
    for event in (
        balance(A, T0 - S),
        balance(B, T0 - S),
        book(A, T0, [[0.34, 100]], [[0.35, 50]]),
        intent(T0, "i1", price="0.36"),
        book(A, T0 + S, [[0.34, 100]], [[0.35, 50]], seq=2),
    ):
        await h.publisher.publish(h.redis, event)
    await h.redis.set(replay_done_key(PREFIX), T0 + S)
    report = await asyncio.wait_for(h.broker.run(), timeout=5)
    states = [(i, s) for i, s, _ in await h.states()]
    assert states == [("i1", "accepted"), ("i1", "open"), ("i1", "cancelled")]
    assert (await h.events())[-1].reason == "shutting down"
    assert report.placed == 1 and report.cancelled == 1
    assert report.opening[A] == {"BASE": "1000.0", "QUOTE": "500.0"}
    assert report.closing[A] == {"BASE": "1000.0", "QUOTE": "500.0"}
    assert report.latency[A]["source"] == "assumed"
    assert report.as_dict()["volume"] == {}
    assert h.broker.orders == {}
    assert int(await h.redis.get(replay_closed_key(PREFIX))) == h.broker.clock


@pytest.mark.asyncio
async def test_a_later_recorded_balance_does_not_override_the_simulation():
    """After the opening snapshot the broker owns the numbers."""
    h = Harness()
    await h.start()
    await h.publish(balance(A, T0, base=5.0, quote=5.0))
    assert h.broker.balances.totals(A)["BASE"] == 1000


def test_streams_cover_feeds_balances_and_intents():
    """The broker reads every configured feed, every balance stream and the intents."""
    broker = sb.SimulatedBroker(fakeredis.FakeRedis(), config(), PREFIX, latency())
    assert broker.streams() == [
        f"md:book:{A}:{SYMBOL}",
        f"md:book:{B}:{SYMBOL}",
        f"md:trade:{A}:{SYMBOL}",
        f"md:trade:{B}:{SYMBOL}",
        f"acct:balance:{A}",
        f"acct:balance:{B}",
        INTENTS_STREAM,
    ]
    assert broker.reader.streams[0] == prefixed(PREFIX, f"md:book:{A}:{SYMBOL}")
    with pytest.raises(ValueError):
        sb.SimulatedBroker(fakeredis.FakeRedis(), config(), "", latency())


# Command line -------------------------------------------------------------------


def test_parse_args_and_assumptions():
    """Assumptions are VENUE=MS pairs; follow repeats."""
    args = sb.parse_args(
        ["r1", "--follow", "a", "--follow", "b", "--assume-rtt-ms", "venue_b=400"]
    )
    assert args.run_id == "r1"
    assert args.follow == ["a", "b"]
    assert args.assume_rtt_ms == [("venue_b", 400.0)]
    assert args.name == "sim"
    with pytest.raises(argparse.ArgumentTypeError):
        sb.parse_assumption("venue_b")
    with pytest.raises(argparse.ArgumentTypeError):
        sb.parse_assumption("venue_b=fast")


def test_build_latency_requires_a_model_for_every_declared_venue(tmp_path: Path):
    """No measurements and no assumption is a refusal, not a silent constant."""
    with pytest.raises(KeyError):
        sb.build_latency(config(), tmp_path, [], seed=0)
    model = sb.build_latency(config(), tmp_path, [(A, 300), (B, 400)], seed=0)
    assert model.summary()[A]["source"] == "assumed"


@pytest.mark.asyncio
async def test_an_idle_broker_reports_the_frontier_as_its_progress():
    """With nothing buffered and nothing read, the broker is as far as the replay."""
    h = Harness()
    await h.start()
    await h.publish(book(A, T0, [[0.34, 100]], [[0.35, 50]]))
    assert int(await h.redis.get(replay_progress_key(PREFIX, "sim"))) == T0
    await h.redis.set(replay_frontier_key(PREFIX), T0 + 30 * S)
    await h.broker.step()
    assert int(await h.redis.get(replay_progress_key(PREFIX, "sim"))) == T0 + 30 * S
    # Held back by the gate, it reports where it actually is.
    gated = Harness(follow=["s"])
    await gated.start()
    await gated.redis.set(replay_frontier_key(PREFIX), T0 + 30 * S)
    await gated.broker.step()
    assert gated.broker.buffer
    assert int(await gated.redis.get(replay_progress_key(PREFIX, "sim"))) == 0
