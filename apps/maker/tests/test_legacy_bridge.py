"""Tests for the legacy pubsub to intents shim."""

import asyncio
import json
from decimal import Decimal

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import legacy_bridge
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.legacy_bridge import (
    LEGACY_ORDER_TYPE_TAG,
    LEGACY_SOURCE,
    LEGACY_SOURCE_TAG,
    cancel_intent_from_message,
    intent_from_order_message,
    intents_from_message,
    order_kind_of,
    publish_legacy_message,
)
from apps.maker.src.structs import CancellationMessage, OrderMessage
from apps.shared.src.events import (
    INTENTS_STREAM,
    CancelIntent,
    OrderIntent,
    OrderKind,
    Side,
    from_stream_fields,
    now_ns,
)
from apps.shared.src.streams import StreamPublisher


def legacy_order(
    order_type: OrderType = OrderType.REPLACE, id: str = "t-250906_lmb_eb"
) -> OrderMessage:
    """Return a legacy order message as a strategy would publish it."""
    return OrderMessage(
        kind=MessageType.ORDER,
        strategy="lmb_eb",
        exchange="mexc",
        id=id,
        exchange_id="_",
        pair="ALPH/USDT",
        side=OrderSide.SELL,
        order_type=order_type,
        price=Decimal("0.35"),
        amount=Decimal("40"),
    )


def legacy_cancellation(id: str = "") -> CancellationMessage:
    """Return a legacy cancellation, by default one that names no order."""
    return CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy="lmb_eb",
        exchange="mexc",
        id=id,
        pair="ALPH/USDT",
    )


def flatten(msg: dict) -> str:
    """Serialize a legacy message the way strategies do."""
    return json.dumps(dict(msg), default=str)


# Conversion ----------------------------------------------------------------


def test_order_kind_only_separates_market_from_limit():
    """Replace and unique are both limit orders as far as a venue is concerned."""
    assert order_kind_of(OrderType.MARKET) is OrderKind.MARKET
    assert order_kind_of(OrderType.REPLACE) is OrderKind.LIMIT
    assert order_kind_of(OrderType.UNIQUE) is OrderKind.LIMIT


def test_an_order_becomes_an_intent_field_for_field():
    """Everything the legacy message carried survives the translation."""
    intent = intent_from_order_message(legacy_order(), 12345)

    assert intent.intent_id == "t-250906_lmb_eb"
    assert intent.strategy == "lmb_eb"
    assert intent.venue == "mexc"
    assert intent.symbol == "ALPH/USDT"
    assert intent.side is Side.SELL
    assert intent.order_type is OrderKind.LIMIT
    assert intent.price == Decimal("0.35")
    assert intent.amount == Decimal("40")
    assert intent.ts_recv == 12345


def test_the_legacy_order_type_is_carried_as_a_tag():
    """Replace semantics has no field on the intent, so it travels as a tag."""
    for order_type in OrderType:
        intent = intent_from_order_message(legacy_order(order_type), 1)
        assert intent.tags[LEGACY_ORDER_TYPE_TAG] == order_type.value
        assert intent.tags[LEGACY_SOURCE_TAG] == LEGACY_SOURCE


def test_a_cancellation_without_an_id_names_no_target():
    """Legacy strategies cancel by strategy, which the order manager resolves."""
    intent = cancel_intent_from_message(legacy_cancellation(), 999)

    assert intent.target_intent_id == ""
    assert intent.strategy == "lmb_eb"
    assert intent.venue == "mexc"
    assert intent.intent_id == "cancel-lmb_eb-999"


def test_a_cancellation_can_still_name_its_target():
    """A cancellation that names an order keeps naming it."""
    intent = cancel_intent_from_message(legacy_cancellation(id="venue-7"), 1)
    assert intent.target_intent_id == "venue-7"


def test_a_batch_becomes_one_intent_per_order():
    """Order batches are unrolled in the order the batch listed them."""
    batch = {
        "kind": MessageType.ORDERBATCH,
        "strategy": "lmb_eb",
        "id": "batch",
        "orders": [legacy_order(id="buy"), legacy_order(id="sell")],
    }
    intents = intents_from_message(batch, 1)  # type: ignore[arg-type]

    assert [i.intent_id for i in intents] == ["buy", "sell"]


def test_an_unknown_kind_translates_to_nothing():
    """A message the shim does not understand is dropped, not guessed at."""
    assert intents_from_message({"kind": "nonsense"}, 1) == []  # type: ignore[arg-type]


# Publication ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_legacy_order_reaches_the_intents_stream():
    """The shim publishes what the order manager consumes."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    published = await publish_legacy_message(
        redis, StreamPublisher(maxlen=100), flatten(legacy_order())
    )

    assert published == 1
    entries = await redis.xrange(INTENTS_STREAM)
    intent = from_stream_fields(entries[0][1])
    assert isinstance(intent, OrderIntent)
    assert intent.intent_id == "t-250906_lmb_eb"
    assert intent.tags[LEGACY_ORDER_TYPE_TAG] == "replace"


@pytest.mark.asyncio
async def test_a_legacy_cancellation_reaches_the_intents_stream():
    """Cancellations travel on the same stream as orders."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    await publish_legacy_message(
        redis, StreamPublisher(maxlen=100), flatten(legacy_cancellation())
    )

    entries = await redis.xrange(INTENTS_STREAM)
    assert isinstance(from_stream_fields(entries[0][1]), CancelIntent)


@pytest.mark.asyncio
async def test_bytes_payloads_are_accepted():
    """A Redis client without response decoding hands the shim bytes."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    published = await publish_legacy_message(
        redis, StreamPublisher(maxlen=100), flatten(legacy_order()).encode()
    )

    assert published == 1


@pytest.mark.asyncio
async def test_unparseable_payloads_publish_nothing():
    """Garbage on the legacy channel is dropped rather than raised."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    assert await publish_legacy_message(redis, publisher, "not json") == 0
    assert await publish_legacy_message(redis, publisher, '{"kind": "unknown"}') == 0
    assert await redis.xlen(INTENTS_STREAM) == 0


@pytest.mark.asyncio
async def test_the_bridge_forwards_what_the_channel_carries():
    """A message published on the legacy channel arrives as an intent."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    bridge = asyncio.create_task(legacy_bridge.run(redis, publisher, "test-channel"))
    await asyncio.sleep(0.05)
    await redis.publish("test-channel", flatten(legacy_order()))
    await asyncio.sleep(0.1)
    bridge.cancel()

    entries = await redis.xrange(INTENTS_STREAM)
    assert len(entries) == 1


@pytest.mark.asyncio
async def test_the_bridge_survives_a_bad_message():
    """One unusable message must not take the order manager's input down."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    bridge = asyncio.create_task(legacy_bridge.run(redis, publisher, "test-channel"))
    await asyncio.sleep(0.05)
    await redis.publish("test-channel", "not json at all")
    await redis.publish("test-channel", flatten(legacy_order()))
    await asyncio.sleep(0.1)

    assert not bridge.done()
    bridge.cancel()
    assert await redis.xlen(INTENTS_STREAM) == 1


@pytest.mark.asyncio
async def test_bridged_intents_are_stamped_on_arrival():
    """The shim stamps its own receive time, since the sender carries none."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    before = now_ns()
    await publish_legacy_message(
        redis, StreamPublisher(maxlen=100), flatten(legacy_order())
    )
    after = now_ns()

    entries = await redis.xrange(INTENTS_STREAM)
    intent = from_stream_fields(entries[0][1])
    assert before <= intent.ts_recv <= after
