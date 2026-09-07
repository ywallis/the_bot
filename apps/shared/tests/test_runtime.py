"""Tests for the strategy runtime."""

import asyncio
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from fakeredis import aioredis as fakeredis

from apps.maker.src.enums import OidComponent
from apps.maker.src.utils import info_from_oid
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    OmsConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
)
from apps.shared.src.events import (
    INTENTS_STREAM,
    ORDER_EVENTS_STREAM,
    REPLACE_RESTING,
    AnyEvent,
    AssetBalance,
    BalanceEvent,
    BookEvent,
    CancelIntent,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    TradeEvent,
    from_stream_fields,
    now_ns,
    prefixed,
)
from apps.shared.src.runtime import (
    Clock,
    Runtime,
    Strategy,
    owner_of,
    strategy_key,
    subscribed_streams,
    time_stamp,
    validate_identifier,
)
from apps.shared.src.streams import StreamPublisher

T0 = 1_757_200_000_000_000_000  # 2025-09-06T23:06:40Z


def config() -> AppConfig:
    """Return a configuration with fast, deterministic settings."""
    return AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(),
        strategies=(),
        oms=OmsConfig(block_ms=10, batch=10),
    )


def strategy_config(identifier: str = "fmb") -> StrategyConfig:
    """Return a two-venue strategy subscribed to books and one trade feed."""
    return StrategyConfig(
        identifier=identifier,
        type="fake_maker",
        production=False,
        subscriptions=(
            Subscription(venue="mexc", symbol="ALPH/USDT", feeds=("book", "trade")),
            Subscription(venue="bitget", symbol="ALPH/USDT", feeds=("book",)),
        ),
        params={},
    )


def book(venue: str = "mexc", ts_recv: int = T0, seq: int = 1) -> BookEvent:
    """Return a one-level book."""
    return BookEvent(
        ts_recv=ts_recv,
        venue=venue,
        symbol="ALPH/USDT",
        seq=seq,
        ts_exch=None,
        bids=[(0.34, 100.0)],
        asks=[(0.35, 100.0)],
    )


def trade(ts_recv: int = T0) -> TradeEvent:
    """Return a trade."""
    return TradeEvent(
        ts_recv=ts_recv,
        venue="mexc",
        symbol="ALPH/USDT",
        seq=1,
        ts_exch=None,
        trade_id="1",
        side=Side.BUY,
        price=0.35,
        amount=10.0,
    )


def balance(venue: str = "mexc", ts_recv: int = T0) -> BalanceEvent:
    """Return a balance snapshot."""
    return BalanceEvent(
        ts_recv=ts_recv,
        venue=venue,
        seq=1,
        ts_exch=None,
        balances={"USDT": AssetBalance(free=50.0, used=0.0, total=50.0)},
    )


def order_event(strategy: str, ts_recv: int = T0) -> OrderEvent:
    """Return an order event for a strategy key."""
    return OrderEvent(
        ts_recv=ts_recv,
        intent_id=f"t-1_{strategy}",
        strategy=strategy,
        venue="mexc",
        symbol="ALPH/USDT",
        state=OrderState.OPEN,
        side=Side.SELL,
    )


class Recorder(Strategy):
    """A strategy that records what it is handed."""

    def __init__(self) -> None:
        """Initialize empty records."""
        self.started: Runtime | None = None
        self.events: list[AnyEvent] = []
        self.timers = 0

    async def on_start(self, runtime: Runtime) -> None:
        """Keep the runtime."""
        self.started = runtime

    async def on_book(self, event: BookEvent) -> None:
        """Record the event."""
        self.events.append(event)

    async def on_trade(self, event: TradeEvent) -> None:
        """Record the event."""
        self.events.append(event)

    async def on_balance(self, event: BalanceEvent) -> None:
        """Record the event."""
        self.events.append(event)

    async def on_order_event(self, event: OrderEvent) -> None:
        """Record the event."""
        self.events.append(event)

    async def on_timer(self) -> None:
        """Count the call."""
        self.timers += 1


class TimedRecorder(Recorder):
    """A recorder with a one second timer."""

    timer_interval_s = 1.0


@pytest_asyncio.fixture(params=[True, False], ids=["decoded", "bytes"])
async def redis(request: pytest.FixtureRequest):
    """Return a fake Redis, decoding replies or not, as the pools decide live."""
    return fakeredis.FakeRedis(decode_responses=request.param)


async def publish(redis: Any, *events: AnyEvent, prefix: str = "") -> None:
    """Publish events the way the feed handlers do."""
    publisher = StreamPublisher(maxlen=1000, prefix=prefix)
    for event in events:
        await publisher.publish(redis, event)


async def intents_on(redis: Any, prefix: str = "") -> list[Any]:
    """Return every intent published, oldest first."""
    entries = await redis.xrange(prefixed(prefix, INTENTS_STREAM))
    return [from_stream_fields(fields) for _entry_id, fields in entries]


# Clock ----------------------------------------------------------------------


def test_a_live_clock_reads_the_wall_clock():
    """Live, ``now`` tracks the wall clock and ignores delivered events."""
    clock = Clock()
    clock.advance(T0)
    assert abs(clock.now() - now_ns()) < 1_000_000_000
    assert clock.last_event_ns == T0


def test_a_replay_clock_reads_the_last_event():
    """Under replay, ``now`` is the last delivered receive time."""
    clock = Clock.replay()
    assert clock.now() == 0
    clock.advance(T0)
    assert clock.now() == T0


def test_the_clock_never_moves_backwards():
    """An event delivered out of receive order does not turn time back."""
    clock = Clock.replay()
    clock.advance(T0 + 10)
    clock.advance(T0)
    assert clock.now() == T0 + 10


def test_time_stamp_is_utc_to_the_microsecond():
    """The stamp is 18 digits and independent of the local timezone."""
    stamp = time_stamp(T0 + 123_456_789)
    assert stamp == "250906230640123456"
    assert len(stamp) == 18


# Identifiers ----------------------------------------------------------------


@pytest.mark.parametrize("bad", ["", "a_b", "a-b", "_"])
def test_reserved_characters_are_rejected(bad: str):
    """Identifiers that would break the order id format are refused."""
    with pytest.raises(ValueError):
        validate_identifier(bad, "Identifier")


def test_owner_of_reads_the_strategy_key():
    """The owner of an event is the part of its strategy key before the slot."""
    assert owner_of(order_event("fmb_es")) == "fmb"
    assert owner_of(order_event("")) == ""
    assert strategy_key("fmb", "es") == "fmb_es"


def test_subscribed_streams_follow_the_config():
    """Feeds, venues and order events, in a stable order, without duplicates."""
    assert subscribed_streams(strategy_config(), prefix="bt:1") == [
        "bt:1:md:book:mexc:ALPH/USDT",
        "bt:1:md:trade:mexc:ALPH/USDT",
        "bt:1:md:book:bitget:ALPH/USDT",
        "bt:1:acct:balance:mexc",
        "bt:1:acct:balance:bitget",
        "bt:1:oms:events",
    ]


def test_the_runtime_refuses_an_identifier_it_cannot_encode():
    """A strategy whose identifier breaks the id format fails at construction."""
    with pytest.raises(ValueError):
        Runtime(None, config(), strategy_config("fake_maker"), Recorder())


# Intents --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_order_intent_is_stamped_by_the_clock(redis: Any):
    """The intent carries the clock time, the slot key and a parseable id."""
    clock = Clock.replay()
    clock.advance(T0)
    runtime = Runtime(redis, config(), strategy_config(), Recorder(), clock=clock)
    intent = runtime.order_intent(
        venue="mexc",
        symbol="ALPH/USDT",
        side=Side.SELL,
        amount=40.0,
        price=0.35,
        order_identifier="es",
        replace_of=REPLACE_RESTING,
    )
    assert intent.ts_recv == T0
    assert intent.strategy == "fmb_es"
    assert intent.amount == Decimal("40")
    assert intent.price == Decimal("0.35")
    assert intent.order_type is OrderKind.LIMIT
    assert intent.replace_of == ""
    assert info_from_oid(intent.intent_id, OidComponent.STRATEGY) == "fmb"
    assert info_from_oid(intent.intent_id, OidComponent.ORDER) == "es"
    assert info_from_oid(intent.intent_id, OidComponent.TIME) == "250906230640000000"


@pytest.mark.asyncio
async def test_intent_ids_are_unique_under_a_frozen_clock(redis: Any):
    """Two ids generated at the same replayed instant still differ."""
    clock = Clock.replay()
    clock.advance(T0)
    runtime = Runtime(redis, config(), strategy_config(), Recorder(), clock=clock)
    first = runtime.new_intent_id("es")
    second = runtime.new_intent_id("es")
    assert first != second
    assert info_from_oid(second, OidComponent.TIME) > info_from_oid(
        first, OidComponent.TIME
    )


@pytest.mark.asyncio
async def test_submit_publishes_under_the_prefix(redis: Any):
    """Intents land on the prefixed intents stream and are remembered."""
    runtime = Runtime(redis, config(), strategy_config(), Recorder(), prefix="bt:1")
    intent = runtime.order_intent(
        venue="mexc",
        symbol="ALPH/USDT",
        side=Side.BUY,
        amount="40",
        price="0.34",
        order_identifier="eb",
    )
    await runtime.submit(intent)
    assert await intents_on(redis) == []
    (published,) = await intents_on(redis, prefix="bt:1")
    assert published == intent
    assert runtime.submitted[intent.intent_id] == intent


@pytest.mark.asyncio
async def test_cancel_names_the_order_it_was_given(redis: Any):
    """A cancel rebuilds venue, symbol and slot key from the submitted intent."""
    runtime = Runtime(redis, config(), strategy_config(), Recorder())
    intent = await runtime.submit(
        runtime.order_intent(
            venue="bitget",
            symbol="ALPH/USDT",
            side=Side.BUY,
            amount=40,
            price=0.34,
            order_identifier="eb",
        )
    )
    cancel = await runtime.cancel(intent.intent_id)
    assert isinstance(cancel, CancelIntent)
    assert cancel.target_intent_id == intent.intent_id
    assert (cancel.venue, cancel.symbol, cancel.strategy) == (
        "bitget",
        "ALPH/USDT",
        "fmb_eb",
    )
    _intent, published = await intents_on(redis)
    assert published == cancel


@pytest.mark.asyncio
async def test_cancel_of_an_unknown_intent_is_an_error(redis: Any):
    """A strategy cannot cancel what it did not submit here."""
    runtime = Runtime(redis, config(), strategy_config(), Recorder())
    with pytest.raises(KeyError):
        await runtime.cancel("t-1_fmb_es")


@pytest.mark.asyncio
async def test_cancel_resting_has_an_empty_target(redis: Any):
    """Cancelling a slot after a restart names no order."""
    runtime = Runtime(redis, config(), strategy_config(), Recorder())
    first = await runtime.cancel_resting("mexc", "ALPH/USDT", "es")
    second = await runtime.cancel_resting("mexc", "ALPH/USDT", "es")
    assert first.target_intent_id == ""
    assert first.strategy == "fmb_es"
    assert first.intent_id != second.intent_id


# Reading --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_events_are_dispatched_by_type_and_ownership(redis: Any):
    """Every subscribed event reaches its hook; foreign order events do not."""
    handler = Recorder()
    runtime = Runtime(redis, config(), strategy_config(), handler)
    await runtime.start()
    assert handler.started is runtime

    await publish(
        redis,
        book(),
        trade(),
        balance(),
        order_event("fmb_es"),
        order_event("lmb_es"),
        order_event(""),
    )
    delivered = await runtime.step()

    assert delivered == 6
    assert [type(event).__name__ for event in handler.events] == [
        "BookEvent",
        "TradeEvent",
        "BalanceEvent",
        "OrderEvent",
    ]
    assert handler.events[-1].strategy == "fmb_es"  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_reading_starts_at_the_tail(redis: Any):
    """Events published before the runtime started are not replayed to it."""
    await publish(redis, book(seq=1))
    handler = Recorder()
    runtime = Runtime(redis, config(), strategy_config(), handler)
    await runtime.start()
    await publish(redis, book(seq=2))
    await runtime.step()
    assert [event.seq for event in handler.events] == [2]  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_nothing_published_between_reads_is_lost(redis: Any):
    """The cursor is a concrete id, so an entry added between reads is read."""
    handler = Recorder()
    runtime = Runtime(redis, config(), strategy_config(), handler)
    await runtime.start()
    await publish(redis, book(seq=1))
    await runtime.step()
    await publish(redis, book(seq=2), book(venue="bitget", seq=3))
    await runtime.step()
    assert sorted(event.seq for event in handler.events) == [1, 2, 3]  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_the_clock_follows_delivered_events(redis: Any):
    """Each delivered event advances the runtime's clock."""
    handler = Recorder()
    runtime = Runtime(
        redis, config(), strategy_config(), handler, clock=Clock.replay()
    )
    await runtime.start()
    await publish(redis, book(ts_recv=T0), book(ts_recv=T0 + 5))
    await runtime.step()
    assert runtime.clock.now() == T0 + 5


@pytest.mark.asyncio
async def test_an_undecodable_entry_is_skipped(redis: Any):
    """A bad entry is logged and the cursor moves past it."""
    handler = Recorder()
    runtime = Runtime(redis, config(), strategy_config(), handler)
    await runtime.start()
    await redis.xadd("md:book:mexc:ALPH/USDT", {"type": "book", "data": "{"})
    await publish(redis, book(seq=2))
    await runtime.step()
    assert [event.seq for event in handler.events] == [2]  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_streams_under_a_prefix_are_the_ones_read(redis: Any):
    """A prefixed runtime ignores the live streams entirely."""
    handler = Recorder()
    runtime = Runtime(redis, config(), strategy_config(), handler, prefix="bt:1")
    await runtime.start()
    await publish(redis, book(seq=1))
    await publish(redis, book(seq=2), prefix="bt:1")
    await runtime.step()
    assert [event.seq for event in handler.events] == [2]  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_run_stops_when_asked(redis: Any):
    """``stop`` ends ``run`` after the read in progress."""
    handler = Recorder()
    runtime = Runtime(redis, config(), strategy_config(), handler)
    task = asyncio.create_task(runtime.run())
    await asyncio.sleep(0.02)
    runtime.stop()
    await asyncio.wait_for(task, timeout=1)
    assert handler.started is runtime


# Timer ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_timers_fire_by_recorded_time(redis: Any):
    """Under replay the timer advances with event time, not wall time."""
    handler = TimedRecorder()
    clock = Clock.replay()
    clock.advance(T0)
    runtime = Runtime(redis, config(), strategy_config(), handler, clock=clock)
    await runtime.start()

    await runtime.step()
    assert handler.timers == 0

    await publish(redis, book(ts_recv=T0 + 999_999_999))
    await runtime.step()
    assert handler.timers == 0

    await publish(redis, book(ts_recv=T0 + 1_000_000_000))
    await runtime.step()
    assert handler.timers == 1

    await publish(redis, book(ts_recv=T0 + 1_500_000_000))
    await runtime.step()
    assert handler.timers == 1


@pytest.mark.asyncio
async def test_live_timers_fire_by_wall_time(redis: Any):
    """Live, an idle runtime still fires its timer once the interval elapses."""

    class Fast(Recorder):
        timer_interval_s = 0.02

    handler = Fast()
    runtime = Runtime(redis, config(), strategy_config(), handler, block_ms=1000)
    await runtime.start()
    deadline = asyncio.get_running_loop().time() + 1
    while handler.timers == 0 and asyncio.get_running_loop().time() < deadline:
        await runtime.step()
    assert handler.timers == 1


@pytest.mark.asyncio
async def test_no_timer_without_an_interval(redis: Any):
    """A strategy without ``timer_interval_s`` never sees ``on_timer``."""
    handler = Recorder()
    runtime = Runtime(redis, config(), strategy_config(), handler, block_ms=10)
    await runtime.start()
    await publish(redis, book(ts_recv=T0), book(ts_recv=T0 + 10**12))
    await runtime.step()
    assert handler.timers == 0
    assert runtime._block_ms() == 10
