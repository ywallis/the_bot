"""Tests for the order manager."""

import asyncio
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest
import pytest_asyncio
from fakeredis import aioredis as fakeredis
from pytest_mock import MockerFixture

from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.errors import BrokerError
from apps.maker.src.message_processor import (
    LEGACY_ORDER_TYPE_TAG,
    OrderManager,
    broker_order_from_intent,
    legacy_order_type,
    replaces,
    venue_ack_ns,
)
from apps.maker.src.structs import OrderBatchMessage, OrderMessage, Response
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    OmsConfig,
    RedisConfig,
)
from apps.shared.src.events import (
    INTENTS_STREAM,
    LATENCY_STREAM,
    OMS_CONSUMER_GROUP,
    ORDER_EVENTS_STREAM,
    REPLACE_RESTING,
    CancelIntent,
    LatencyRecord,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    from_stream_fields,
    now_ns,
    to_stream_fields,
)
from apps.shared.src.streams import StreamPublisher

VENUE_TIMESTAMP_MS = 1757160000000


def config() -> AppConfig:
    """Return a configuration with fast, deterministic order manager settings."""
    return AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(),
        strategies=(),
        oms=OmsConfig(block_ms=10, batch=10, max_intent_age_s=5.0),
    )


def order_intent(
    intent_id: str = "t-250906120000_lmb_eb",
    strategy: str = "lmb_eb",
    venue: str = "mexc",
    legacy: OrderType | None = OrderType.REPLACE,
    replace_of: str | None = None,
    ts_recv: int | None = None,
) -> OrderIntent:
    """Return an order intent, by default a legacy replace order."""
    tags = {} if legacy is None else {LEGACY_ORDER_TYPE_TAG: legacy.value}
    return OrderIntent(
        ts_recv=now_ns() if ts_recv is None else ts_recv,
        intent_id=intent_id,
        strategy=strategy,
        venue=venue,
        symbol="ALPH/USDT",
        side=Side.SELL,
        order_type=OrderKind.LIMIT,
        amount=Decimal("40"),
        price=Decimal("0.35"),
        replace_of=replace_of,
        tags=tags,
    )


class FakeBroker:
    """A broker that confirms everything and records what it was asked."""

    def __init__(self) -> None:
        """Initialize the fake broker."""
        self.sent: list[dict[str, Any]] = []
        self.gate: asyncio.Event | None = None

    async def __call__(self, msg: Any) -> Response:
        """Confirm one message, blocking first if a gate is set."""
        self.sent.append(dict(msg))
        if self.gate is not None:
            await self.gate.wait()
        if msg["kind"] == MessageType.CANCELLATION:
            return Response(kind=MessageType.CANCELLATION, text="cancelled")
        return Response(
            kind=MessageType.ORDER,
            text=f"{{'id': 'venue-{msg['id']}', 'timestamp': {VENUE_TIMESTAMP_MS}}}",
        )

    @property
    def orders(self) -> list[dict[str, Any]]:
        """Return the order messages the broker received."""
        return [m for m in self.sent if m["kind"] == MessageType.ORDER]

    @property
    def cancellations(self) -> list[dict[str, Any]]:
        """Return the cancellation messages the broker received."""
        return [m for m in self.sent if m["kind"] == MessageType.CANCELLATION]


@pytest_asyncio.fixture(params=[True, False], ids=["decoded", "bytes"])
async def manager(request: pytest.FixtureRequest):
    """
    Return an order manager on a fake Redis, ready to read new intents.

    The first read of a real manager drains its own pending list, so one
    read is spent here to move the cursor onto new entries. The transition
    itself is tested separately.

    Every test runs against both a decoding and a non-decoding client.
    Which one production gets is decided by the connection pool rather than
    by the ``Redis`` constructor, and the pools in this app do not decode,
    so a manager that only works against decoded replies works in every
    test and in none of the live processes.
    """
    redis = fakeredis.FakeRedis(decode_responses=request.param)
    order_manager = OrderManager(config(), redis)
    await order_manager.ensure_group()
    await order_manager.consume_once()
    return order_manager


@pytest.fixture
def broker(manager: OrderManager) -> FakeBroker:
    """Attach a fake broker to the manager and return it."""
    fake = FakeBroker()
    manager.send_to_broker = fake  # type: ignore[method-assign]
    return fake


async def settle(manager: OrderManager) -> None:
    """Wait for every task the manager started, including ones they start."""
    for _ in range(10):
        if not manager.tasks:
            return
        tasks, manager.tasks = manager.tasks, []
        await asyncio.gather(*tasks)
    raise AssertionError("Order manager tasks did not settle")


async def submit(manager: OrderManager, *intents: OrderIntent | CancelIntent) -> None:
    """Publish intents and let the manager consume them."""
    publisher = StreamPublisher(maxlen=1000)
    for intent in intents:
        await publisher.publish(manager.redis, intent)
    await manager.consume_once()
    await settle(manager)


async def events_on(manager: OrderManager, stream: str) -> list[Any]:
    """Return every event decoded from a stream."""
    entries = cast(list[Any], await manager.redis.xrange(stream))
    return [from_stream_fields(fields) for _id, fields in entries]


async def pending_count(manager: OrderManager) -> int:
    """Return how many intents the consumer group has read but not acked."""
    summary = await manager.redis.xpending(INTENTS_STREAM, OMS_CONSUMER_GROUP)
    return int(summary["pending"])


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [True, False], ids=["decoded", "bytes"])
async def test_the_cursor_survives_a_non_decoding_client(decode: bool):
    """
    An entry id read back must be usable as a command argument again.

    The connection pool decides whether ids come back as bytes, and
    ``str(b"1-0")`` is a string Redis rejects. This is what left every
    intent unacknowledged and killed the matcher on its second read the
    first time the phase ran against a live Redis.
    """
    redis = fakeredis.FakeRedis(decode_responses=decode)
    order_manager = OrderManager(config(), redis)
    await order_manager.ensure_group()
    await order_manager.consume_once()

    publisher = StreamPublisher(maxlen=1000)
    await publisher.publish(redis, order_intent(intent_id="one"))
    fake = FakeBroker()
    order_manager.send_to_broker = fake  # type: ignore[assignment]
    await order_manager.consume_once()
    await settle(order_manager)

    assert [m["id"] for m in fake.orders] == ["one"]
    summary = await redis.xpending(INTENTS_STREAM, OMS_CONSUMER_GROUP)
    assert int(summary["pending"]) == 0


# Pure helpers --------------------------------------------------------------


def test_legacy_order_type_prefers_the_tag():
    """A bridged intent keeps the order type the legacy message carried."""
    assert legacy_order_type(order_intent(legacy=OrderType.UNIQUE)) is OrderType.UNIQUE
    assert legacy_order_type(order_intent(legacy=OrderType.REPLACE)) is OrderType.REPLACE


def test_legacy_order_type_derived_for_native_intents():
    """A native intent is classified from its own fields."""
    market = OrderIntent(
        ts_recv=now_ns(),
        intent_id="i1",
        strategy="s",
        venue="mexc",
        symbol="ALPH/USDT",
        side=Side.BUY,
        order_type=OrderKind.MARKET,
        amount=Decimal("1"),
    )
    assert legacy_order_type(market) is OrderType.MARKET
    assert legacy_order_type(order_intent(legacy=None)) is OrderType.UNIQUE
    assert (
        legacy_order_type(order_intent(legacy=None, replace_of="older"))
        is OrderType.REPLACE
    )


def test_replaces_is_opt_in():
    """Only an explicit replace_of or the legacy replace tag supersedes."""
    assert replaces(order_intent(legacy=OrderType.REPLACE))
    assert replaces(order_intent(legacy=None, replace_of="older"))
    assert not replaces(order_intent(legacy=OrderType.UNIQUE))
    assert not replaces(order_intent(legacy=None))


def test_broker_order_from_intent_carries_every_field():
    """The broker message is built from the intent alone."""
    msg = broker_order_from_intent(order_intent())
    assert msg["id"] == "t-250906120000_lmb_eb"
    assert msg["exchange"] == "mexc"
    assert msg["pair"] == "ALPH/USDT"
    assert msg["order_type"] is OrderType.REPLACE
    assert msg["price"] == Decimal("0.35")
    assert msg["amount"] == Decimal("40")


def test_broker_order_prices_a_market_intent_at_zero():
    """An intent with no price still produces a valid broker message."""
    intent = OrderIntent(
        ts_recv=now_ns(),
        intent_id="i1",
        strategy="s",
        venue="mexc",
        symbol="ALPH/USDT",
        side=Side.BUY,
        order_type=OrderKind.MARKET,
        amount=Decimal("1"),
    )
    assert broker_order_from_intent(intent)["price"] == Decimal(0)


def test_venue_ack_ns_converts_milliseconds():
    """A venue timestamp becomes nanoseconds, and a missing one stays None."""
    assert venue_ack_ns({"timestamp": 1757160000000}) == 1757160000000 * 1_000_000
    assert venue_ack_ns({"timestamp": None}) is None
    assert venue_ack_ns({}) is None
    assert venue_ack_ns({"timestamp": "not a time"}) is None


# Placement -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_intent_is_placed_acked_and_reported(
    manager: OrderManager, broker: FakeBroker
):
    """A healthy intent reaches the broker and leaves nothing pending."""
    intent = order_intent()
    await submit(manager, intent)

    assert [m["id"] for m in broker.orders] == [intent.intent_id]
    states = [e.state for e in await events_on(manager, ORDER_EVENTS_STREAM)]
    assert states == [OrderState.ACCEPTED, OrderState.OPEN]
    assert manager.orders[("mexc", intent.intent_id)].venue_order_id == (
        f"venue-{intent.intent_id}"
    )
    assert manager.resting["lmb_eb"] == ("mexc", intent.intent_id)
    assert await pending_count(manager) == 0


@pytest.mark.asyncio
async def test_placement_publishes_a_latency_record(
    manager: OrderManager, broker: FakeBroker
):
    """Every placement is timed end to end."""
    intent = order_intent()
    await submit(manager, intent)

    records = await events_on(manager, LATENCY_STREAM)
    assert len(records) == 1
    record = records[0]
    assert isinstance(record, LatencyRecord)
    assert record.intent_id == intent.intent_id
    assert record.ts_created == intent.ts_recv
    assert record.ts_oms_recv is not None
    assert record.ts_broker_send is not None
    assert record.ts_broker_ack is not None
    assert record.ts_created <= record.ts_oms_recv <= record.ts_broker_send
    assert record.ts_broker_send <= record.ts_broker_ack
    assert record.ts_venue_ack == VENUE_TIMESTAMP_MS * 1_000_000


@pytest.mark.asyncio
async def test_a_replacing_intent_cancels_the_resting_order(
    manager: OrderManager, broker: FakeBroker
):
    """The second quote of a strategy cancels the first before it is placed."""
    first = order_intent(intent_id="first")
    second = order_intent(intent_id="second")
    await submit(manager, first)
    await submit(manager, second)

    assert [m["id"] for m in broker.cancellations] == [f"venue-{first.intent_id}"]
    assert ("mexc", "first") not in manager.orders
    assert manager.resting["lmb_eb"] == ("mexc", "second")


@pytest.mark.asyncio
async def test_replace_of_names_the_order_it_supersedes(
    manager: OrderManager, broker: FakeBroker
):
    """A native intent can supersede an order without going through resting."""
    first = order_intent(intent_id="first", legacy=None)
    await submit(manager, first)
    assert manager.resting == {}

    second = order_intent(intent_id="second", legacy=None, replace_of="first")
    await submit(manager, second)

    assert [m["id"] for m in broker.cancellations] == ["venue-first"]
    assert ("mexc", "first") not in manager.orders


@pytest.mark.asyncio
async def test_a_first_quote_rests_under_its_slot(
    manager: OrderManager, broker: FakeBroker
):
    """``REPLACE_RESTING`` makes a quote rest even though it names no order."""
    first = order_intent(intent_id="first", legacy=None, replace_of=REPLACE_RESTING)
    await submit(manager, first)
    assert broker.cancellations == []
    assert manager.resting["lmb_eb"] == ("mexc", "first")

    second = order_intent(intent_id="second", legacy=None, replace_of=REPLACE_RESTING)
    await submit(manager, second)
    assert [m["id"] for m in broker.cancellations] == ["venue-first"]
    assert manager.resting["lmb_eb"] == ("mexc", "second")


@pytest.mark.asyncio
async def test_superseding_clears_the_slot_even_when_naming_a_gone_order(
    manager: OrderManager, broker: FakeBroker
):
    """A quote that names a rejected predecessor still cancels what rests.

    The strategy's view lags: it names the intent it last submitted, which
    was rejected unplaced in the queue, while the venue still holds the
    quote before it.
    """
    first = order_intent(intent_id="first", legacy=None, replace_of=REPLACE_RESTING)
    await submit(manager, first)
    third = order_intent(intent_id="third", legacy=None, replace_of="second")
    await submit(manager, third)

    assert [m["id"] for m in broker.cancellations] == ["venue-first"]
    assert ("mexc", "first") not in manager.orders
    assert manager.resting["lmb_eb"] == ("mexc", "third")


@pytest.mark.asyncio
async def test_independent_intents_do_not_cancel_each_other(
    manager: OrderManager, broker: FakeBroker
):
    """Unique orders of one strategy all reach the venue."""
    await submit(
        manager,
        order_intent(intent_id="one", legacy=OrderType.UNIQUE),
        order_intent(intent_id="two", legacy=OrderType.UNIQUE),
    )

    assert [m["id"] for m in broker.orders] == ["one", "two"]
    assert broker.cancellations == []
    assert manager.resting == {}
    assert ("mexc", "one") in manager.orders
    assert ("mexc", "two") in manager.orders


@pytest.mark.asyncio
async def test_market_orders_leave_the_book_immediately(
    manager: OrderManager, broker: FakeBroker
):
    """A market order is never tracked: it cannot rest and cannot be cancelled."""
    intent = OrderIntent(
        ts_recv=now_ns(),
        intent_id="hedge",
        strategy="matching",
        venue="bitget",
        symbol="ALPH/USDT",
        side=Side.BUY,
        order_type=OrderKind.MARKET,
        amount=Decimal("40"),
        price=Decimal("0.35"),
    )
    await submit(manager, intent)

    assert [m["id"] for m in broker.orders] == ["hedge"]
    assert manager.orders == {}


@pytest.mark.asyncio
async def test_a_rejected_order_is_reported_and_forgotten(manager: OrderManager):
    """A broker error becomes a rejection rather than a stuck order."""

    async def failing(_msg: Any) -> Response:
        return Response(kind=MessageType.ERROR, text="venue said no")

    manager.send_to_broker = failing  # type: ignore[assignment]
    await submit(manager, order_intent())

    states = [e.state for e in await events_on(manager, ORDER_EVENTS_STREAM)]
    assert states == [OrderState.ACCEPTED, OrderState.REJECTED]
    assert manager.orders == {}
    assert manager.resting == {}
    assert len(await events_on(manager, LATENCY_STREAM)) == 1
    assert await pending_count(manager) == 0


@pytest.mark.asyncio
async def test_stale_intents_never_reach_the_broker(
    manager: OrderManager, broker: FakeBroker
):
    """An intent older than max_intent_age_s is rejected, not placed."""
    stale = order_intent(ts_recv=now_ns() - 60 * 1_000_000_000)
    await submit(manager, stale)

    assert broker.sent == []
    events = await events_on(manager, ORDER_EVENTS_STREAM)
    assert [e.state for e in events] == [OrderState.REJECTED]
    assert "max_intent_age_s" in (events[0].reason or "")
    assert await pending_count(manager) == 0


# Coalescing ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_busy_strategy_keeps_only_the_latest_quote(
    manager: OrderManager, broker: FakeBroker
):
    """While one quote is at the broker, only the newest of the rest survives."""
    broker.gate = asyncio.Event()
    publisher = StreamPublisher(maxlen=1000)
    for intent in (
        order_intent(intent_id="first"),
        order_intent(intent_id="second"),
        order_intent(intent_id="third"),
    ):
        await publisher.publish(manager.redis, intent)

    await manager.consume_once()
    await asyncio.sleep(0)
    # "first" holds the lock at the broker, "second" was superseded by "third".
    assert manager.queued["lmb_eb"].intent.intent_id == "third"
    rejected = [
        e
        for e in await events_on(manager, ORDER_EVENTS_STREAM)
        if e.state is OrderState.REJECTED
    ]
    assert [e.intent_id for e in rejected] == ["second"]
    assert "superseded by third" in (rejected[0].reason or "")

    broker.gate.set()
    await settle(manager)

    assert [m["id"] for m in broker.orders] == ["first", "third"]
    assert manager.resting["lmb_eb"] == ("mexc", "third")
    assert manager.queued == {}
    assert await pending_count(manager) == 0


# Cancellation --------------------------------------------------------------


def cancel_intent(target: str = "", strategy: str = "lmb_eb") -> CancelIntent:
    """Return a cancel intent, by default one that names no order."""
    return CancelIntent(
        ts_recv=now_ns(),
        intent_id=f"cancel-{strategy}",
        strategy=strategy,
        venue="mexc",
        symbol="ALPH/USDT",
        target_intent_id=target,
    )


@pytest.mark.asyncio
async def test_a_cancellation_without_a_target_cancels_what_rests(
    manager: OrderManager, broker: FakeBroker
):
    """Legacy strategies cancel by strategy, not by order id."""
    await submit(manager, order_intent(intent_id="resting"))
    await submit(manager, cancel_intent())

    assert [m["id"] for m in broker.cancellations] == ["venue-resting"]
    assert manager.orders == {}
    assert manager.resting == {}
    assert await pending_count(manager) == 0


@pytest.mark.asyncio
async def test_a_cancellation_can_name_its_target(
    manager: OrderManager, broker: FakeBroker
):
    """A cancel intent naming an order cancels that one."""
    await submit(manager, order_intent(intent_id="one", legacy=OrderType.UNIQUE))
    await submit(manager, cancel_intent(target="one"))

    assert [m["id"] for m in broker.cancellations] == ["venue-one"]
    assert manager.orders == {}


@pytest.mark.asyncio
async def test_a_cancellation_with_nothing_to_cancel_is_acknowledged(
    manager: OrderManager, broker: FakeBroker
):
    """A cancellation for an order that is already gone is not an error."""
    await submit(manager, cancel_intent(target="never placed"))

    assert broker.sent == []
    assert await pending_count(manager) == 0


@pytest.mark.asyncio
async def test_a_failed_cancellation_keeps_the_order(manager: OrderManager):
    """An order the broker could not cancel stays in the book."""
    placements = FakeBroker()
    manager.send_to_broker = placements  # type: ignore[method-assign]
    await submit(manager, order_intent(intent_id="resting"))

    async def refuse(msg: Any) -> Response:
        if msg["kind"] == MessageType.CANCELLATION:
            return Response(kind=MessageType.ERROR, text="cannot cancel")
        return await placements(msg)

    manager.send_to_broker = refuse  # type: ignore[method-assign]
    await submit(manager, cancel_intent())

    assert ("mexc", "resting") in manager.orders
    assert await pending_count(manager) == 0


# Book maintenance ----------------------------------------------------------


def order_event(
    intent_id: str,
    state: OrderState,
    venue: str = "mexc",
    filled: Decimal = Decimal("40"),
) -> OrderEvent:
    """Return an order event as the order watcher would publish it."""
    return OrderEvent(
        ts_recv=now_ns(),
        intent_id=intent_id,
        strategy="lmb_eb",
        venue=venue,
        symbol="ALPH/USDT",
        state=state,
        side=Side.SELL,
        venue_order_id=f"venue-{intent_id}",
        filled=filled,
        remaining=Decimal("40") - filled,
        avg_price=Decimal("0.351"),
    )


@pytest.mark.asyncio
async def test_a_fill_reported_by_the_venue_leaves_the_book(
    manager: OrderManager, broker: FakeBroker
):
    """An order that filled is dropped, so shutdown does not try to cancel it."""
    await submit(manager, order_intent(intent_id="resting"))
    manager.apply_order_event(order_event("resting", OrderState.FILLED))

    assert manager.orders == {}
    assert manager.resting == {}


@pytest.mark.asyncio
async def test_a_partial_fill_updates_the_book_without_leaving_it(
    manager: OrderManager, broker: FakeBroker
):
    """A partial fill is recorded against the order it belongs to."""
    await submit(manager, order_intent(intent_id="resting"))
    manager.apply_order_event(
        order_event("resting", OrderState.PARTIALLY_FILLED, filled=Decimal("10"))
    )

    tracked = manager.orders[("mexc", "resting")]
    assert tracked.filled == Decimal("10")
    assert tracked.remaining == Decimal("30")
    assert tracked.avg_price == Decimal("0.351")
    assert manager.resting["lmb_eb"] == ("mexc", "resting")


@pytest.mark.asyncio
async def test_events_about_other_orders_are_ignored(
    manager: OrderManager, broker: FakeBroker
):
    """An order id we know on a venue we did not use is a different order."""
    await submit(manager, order_intent(intent_id="resting"))
    manager.apply_order_event(
        order_event("resting", OrderState.FILLED, venue="bitget")
    )

    assert ("mexc", "resting") in manager.orders


@pytest.mark.asyncio
async def test_follow_order_events_applies_what_it_reads(
    manager: OrderManager, broker: FakeBroker
):
    """The follower loop feeds the book from the bus."""
    await submit(manager, order_intent(intent_id="resting"))
    follower = asyncio.create_task(manager.follow_order_events())
    await asyncio.sleep(0.05)

    publisher = StreamPublisher(maxlen=1000)
    await publisher.publish(
        manager.redis, order_event("resting", OrderState.FILLED)
    )
    await asyncio.sleep(0.1)
    follower.cancel()

    assert manager.orders == {}


# Intake edge cases ---------------------------------------------------------


@pytest.mark.asyncio
async def test_an_undecodable_entry_is_dropped_not_retried(manager: OrderManager):
    """A poison entry is acknowledged so it cannot wedge every restart."""
    await manager.redis.xadd(INTENTS_STREAM, {"type": "order_intent", "data": "{"})
    await manager.consume_once()
    await settle(manager)

    assert await pending_count(manager) == 0


@pytest.mark.asyncio
async def test_an_event_that_is_not_an_intent_is_dropped(manager: OrderManager):
    """Only intents belong on the intents stream, and the rest are ignored."""
    record = LatencyRecord(
        ts_recv=now_ns(), intent_id="i", venue="mexc", ts_created=now_ns()
    )
    await manager.redis.xadd(INTENTS_STREAM, cast(Any, to_stream_fields(record)))
    await manager.consume_once()
    await settle(manager)

    assert await pending_count(manager) == 0


@pytest.mark.asyncio
async def test_pending_intents_are_replayed_after_a_crash(
    manager: OrderManager, broker: FakeBroker
):
    """An intent read but never acknowledged is acted on by the next process."""
    broker.gate = asyncio.Event()
    publisher = StreamPublisher(maxlen=1000)
    await publisher.publish(manager.redis, order_intent(intent_id="in flight"))
    await manager.consume_once()
    await asyncio.sleep(0)
    assert await pending_count(manager) == 1

    # The process dies mid-flight: its task never finishes and never acks.
    for task in manager.tasks:
        task.cancel()
    manager.tasks = []

    restarted = OrderManager(config(), manager.redis)
    await restarted.ensure_group()
    replayed = FakeBroker()
    restarted.send_to_broker = replayed  # type: ignore[method-assign]
    # The first read is the pending list, which is exactly what was in flight.
    await restarted.consume_once()
    await settle(restarted)

    assert [m["id"] for m in replayed.orders] == ["in flight"]
    assert await pending_count(restarted) == 0


@pytest.mark.asyncio
async def test_the_cursor_moves_from_pending_to_new():
    """A manager drains its pending list first, then follows new entries."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    fresh = OrderManager(config(), redis)
    await fresh.ensure_group()

    assert fresh.cursor == "0"
    await fresh.consume_once()
    assert fresh.cursor == ">"


# Startup and shutdown ------------------------------------------------------


@pytest.mark.asyncio
async def test_adopted_orders_rest_without_being_announced(manager: OrderManager):
    """Orders found at the venue are tracked but produce no order events."""
    recollected = OrderMessage(
        kind=MessageType.ORDER,
        strategy="lmb_eb",
        exchange="mexc",
        id="t-250906120000_lmb_eb",
        exchange_id="venue-7",
        pair="ALPH/USDT",
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal("0.35"),
        amount=Decimal("40"),
    )
    manager.adopt({"lmb_eb": recollected})

    key = ("mexc", "t-250906120000_lmb_eb")
    assert manager.orders[key].venue_order_id == "venue-7"
    assert manager.resting["lmb_eb"] == key
    assert await events_on(manager, ORDER_EVENTS_STREAM) == []


@pytest.mark.asyncio
async def test_shutdown_cancels_resting_orders_only(
    manager: OrderManager, broker: FakeBroker
):
    """A unique order is deliberately left alone when the engine stops."""
    await submit(manager, order_intent(intent_id="resting"))
    await submit(manager, order_intent(intent_id="unique", legacy=OrderType.UNIQUE))

    await manager.cancel_all_open()

    assert [m["id"] for m in broker.cancellations] == ["venue-resting"]
    assert ("mexc", "unique") in manager.orders


@pytest.mark.asyncio
async def test_place_order_raises_broker_error(manager: OrderManager):
    """An error reply from the broker is raised, not swallowed."""

    async def failing(_msg: Any) -> Response:
        return Response(kind=MessageType.ERROR, text="error")

    manager.send_to_broker = failing  # type: ignore[assignment]
    with pytest.raises(BrokerError):
        await manager.place_order(broker_order_from_intent(order_intent()))


@pytest.mark.asyncio
async def test_place_order_rejects_an_unreadable_confirmation(manager: OrderManager):
    """A confirmation without an order id cannot be treated as a placement."""

    async def nonsense(_msg: Any) -> Response:
        return Response(kind=MessageType.ORDER, text="{'no': 'id'}")

    manager.send_to_broker = nonsense  # type: ignore[assignment]
    with pytest.raises(BrokerError):
        await manager.place_order(broker_order_from_intent(order_intent()))


@pytest.mark.asyncio
async def test_get_open_orders(
    manager: OrderManager, order_batch_raw: dict, mocker: MockerFixture
):
    """Open orders are recollected from the broker at startup."""
    mock_redis = MagicMock()
    mock_pubsub = MagicMock()
    mock_redis.__aenter__.return_value = mock_redis
    mock_pubsub.__aenter__.return_value = mock_pubsub
    mock_redis.publish = AsyncMock()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()

    import json

    async def mock_listen():
        yield {"type": "message", "data": json.dumps(order_batch_raw).encode("utf-8")}

    mock_pubsub.listen = mock_listen
    mock_redis.pubsub.return_value = mock_pubsub
    mocker.patch("apps.maker.src.message_processor.Redis", return_value=mock_redis)

    open_orders = await manager.get_open_orders()

    assert "ALPH_gate" in open_orders
    mock_redis.publish.assert_called_once()
    mock_pubsub.subscribe.assert_called_once_with("INIT")
    mock_pubsub.unsubscribe.assert_called_once_with("INIT")


@pytest.mark.asyncio
async def test_get_open_orders_empty(
    manager: OrderManager, empty_open_orders: OrderBatchMessage, mocker: MockerFixture
):
    """Recollection with nothing open returns nothing."""
    mock_redis = MagicMock()
    mock_pubsub = MagicMock()
    mock_redis.__aenter__.return_value = mock_redis
    mock_pubsub.__aenter__.return_value = mock_pubsub
    mock_redis.publish = AsyncMock()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()

    import json

    async def mock_listen():
        yield {
            "type": "message",
            "data": json.dumps(dict(empty_open_orders), default=str).encode("utf-8"),
        }

    mock_pubsub.listen = mock_listen
    mock_redis.pubsub.return_value = mock_pubsub
    mocker.patch("apps.maker.src.message_processor.Redis", return_value=mock_redis)

    assert await manager.get_open_orders() == {}


@pytest.mark.asyncio
async def test_get_lock_is_created_once(manager: OrderManager):
    """One lock per strategy, created on demand."""
    lock = manager.get_lock("lmb_eb")
    assert isinstance(lock, asyncio.Lock)
    assert manager.get_lock("lmb_eb") is lock


@pytest.mark.asyncio
async def test_a_quote_queued_behind_a_cancellation_still_runs(
    manager: OrderManager, broker: FakeBroker
):
    """Whoever releases a strategy's lock has to start what queued behind it."""
    await submit(manager, order_intent(intent_id="resting"))

    broker.gate = asyncio.Event()
    publisher = StreamPublisher(maxlen=1000)
    await publisher.publish(manager.redis, cancel_intent())
    await publisher.publish(manager.redis, order_intent(intent_id="next quote"))
    await manager.consume_once()
    await asyncio.sleep(0)
    assert manager.queued["lmb_eb"].intent.intent_id == "next quote"

    broker.gate.set()
    await settle(manager)

    assert [m["id"] for m in broker.orders] == ["resting", "next quote"]
    assert manager.resting["lmb_eb"] == ("mexc", "next quote")
    assert manager.queued == {}
    assert await pending_count(manager) == 0


@pytest.mark.asyncio
async def test_a_pending_entry_is_acted_on_once_while_draining(
    manager: OrderManager, broker: FakeBroker
):
    """
    Draining the pending list must not act on the same entry repeatedly.

    An entry stays pending until its task acknowledges it, so a drain that
    kept reading from the start of the list handed the same intents back on
    every pass and placed the order again each time. Live, that looped until
    the connection pool was exhausted.

    The intent here is a unique one on purpose. A superseding intent would
    queue behind its strategy's lock and hide the duplicate; a unique one
    has nothing serialising it, so a re-read reaches the venue twice, which
    is the case that costs money.
    """
    broker.gate = asyncio.Event()
    publisher = StreamPublisher(maxlen=1000)
    await publisher.publish(
        manager.redis, order_intent(intent_id="in flight", legacy=OrderType.UNIQUE)
    )
    await manager.consume_once()
    await asyncio.sleep(0)
    for task in manager.tasks:
        task.cancel()
    manager.tasks = []
    assert await pending_count(manager) == 1

    restarted = OrderManager(config(), manager.redis)
    await restarted.ensure_group()
    replayed = FakeBroker()
    replayed.gate = asyncio.Event()
    restarted.send_to_broker = replayed  # type: ignore[assignment]

    # Several passes while the entry is still unacknowledged, as `run` does.
    for _ in range(5):
        await restarted.consume_once()
        await asyncio.sleep(0)

    assert [m["id"] for m in replayed.orders] == ["in flight"]

    replayed.gate.set()
    await settle(restarted)
    assert await pending_count(restarted) == 0


@pytest.mark.asyncio
async def test_a_queued_intent_keeps_its_read_time(
    manager: OrderManager, broker: FakeBroker
):
    """
    Time spent in the coalescing queue belongs to the order manager.

    ``ts_oms_recv`` means "read from the stream", so a queue wait has to
    appear between it and ``ts_broker_send``. Stamping it afresh on drain
    moved that wait onto the leg from the strategy, where anything building
    section 9's latency model out of ``oms:latency`` would read it as
    transport. A live run put up to a second of internal queueing there.
    """
    broker.gate = asyncio.Event()
    publisher = StreamPublisher(maxlen=1000)
    first = order_intent(intent_id="first")
    second = order_intent(intent_id="second")
    await publisher.publish(manager.redis, first)
    await publisher.publish(manager.redis, second)
    await manager.consume_once()
    await asyncio.sleep(0)
    read_at = manager.queued["lmb_eb"].ts_oms_recv
    assert read_at > 0

    broker.gate.set()
    await settle(manager)

    records = [
        r for r in await events_on(manager, LATENCY_STREAM) if r.intent_id == "second"
    ]
    assert len(records) == 1
    record = records[0]
    assert record.ts_oms_recv == read_at
    # The wait shows up inside the order manager, not on the way to it.
    assert record.ts_oms_recv - record.ts_created < 1_000_000_000
    assert record.ts_broker_send >= record.ts_oms_recv


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [True, False], ids=["decoded", "bytes"])
async def test_an_intent_that_goes_stale_while_queued_is_rejected(decode: bool):
    """
    Staleness is judged against the clock now, not the moment of reading.

    A quote can be fresh when it is read and far too old by the time its
    strategy's lock frees up. Judging it on its read time would send it, and
    the read time has to be preserved for the latency record, so the two
    cannot share a timestamp.
    """
    redis = fakeredis.FakeRedis(decode_responses=decode)
    settings = OmsConfig(block_ms=10, batch=10, max_intent_age_s=0.05)
    order_manager = OrderManager(
        msgspec.structs.replace(config(), oms=settings), redis
    )
    await order_manager.ensure_group()
    await order_manager.consume_once()

    broker = FakeBroker()
    broker.gate = asyncio.Event()
    order_manager.send_to_broker = broker  # type: ignore[assignment]

    publisher = StreamPublisher(maxlen=1000)
    await publisher.publish(redis, order_intent(intent_id="first"))
    await publisher.publish(redis, order_intent(intent_id="queued"))
    await order_manager.consume_once()
    await asyncio.sleep(0)
    assert order_manager.queued["lmb_eb"].intent.intent_id == "queued"

    # Both were fresh when read; "queued" ages past the limit while waiting.
    await asyncio.sleep(0.2)
    broker.gate.set()
    await settle(order_manager)

    assert [m["id"] for m in broker.orders] == ["first"]
    events = [
        e
        for e in cast(list[Any], await redis.xrange(ORDER_EVENTS_STREAM))
    ]
    rejected = [
        e
        for e in (from_stream_fields(f) for _id, f in events)
        if isinstance(e, OrderEvent)
        and e.state is OrderState.REJECTED
        and e.intent_id == "queued"
    ]
    assert len(rejected) == 1
    assert "max_intent_age_s" in (rejected[0].reason or "")
