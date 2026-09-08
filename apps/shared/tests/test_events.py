"""Tests for event schemas, codec and stream naming."""

import json
from decimal import Decimal

import msgspec
import pytest

from apps.shared.src import events
from apps.shared.src.events import (
    AssetBalance,
    BalanceEvent,
    BookEvent,
    CancelIntent,
    EventType,
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
)

TS = 1_757_160_000_000_000_000


def sample_events():
    """Return one instance of every event type."""
    return [
        BookEvent(
            ts_recv=TS,
            venue="gate",
            symbol="BTC/USDT",
            seq=1,
            ts_exch=1_757_160_000_000,
            bids=[(100.0, 1.5), (99.5, 2.0)],
            asks=[(100.5, 1.0)],
        ),
        TradeEvent(
            ts_recv=TS,
            venue="mexc",
            symbol="SOL/USDT",
            seq=7,
            ts_exch=None,
            trade_id="abc",
            side=Side.BUY,
            price=150.25,
            amount=3.0,
        ),
        BalanceEvent(
            ts_recv=TS,
            venue="gate",
            seq=2,
            ts_exch=None,
            balances={"USDT": AssetBalance(free=10.0, used=5.0, total=15.0)},
        ),
        OrderIntent(
            ts_recv=TS,
            intent_id="t-1_lat_a",
            strategy="lat",
            venue="mexc",
            symbol="SOL/USDT",
            side=Side.SELL,
            order_type=OrderKind.LIMIT,
            amount=Decimal("1.23456789"),
            price=Decimal("150.10"),
            time_in_force=TimeInForce.IOC,
            replace_of="t-0_lat_a",
            tags={"leg": "exit"},
        ),
        CancelIntent(
            ts_recv=TS,
            intent_id="c-1",
            strategy="lat",
            venue="mexc",
            symbol="SOL/USDT",
            target_intent_id="t-1_lat_a",
        ),
        OrderEvent(
            ts_recv=TS,
            intent_id="t-1_lat_a",
            strategy="lat",
            venue="mexc",
            symbol="SOL/USDT",
            state=OrderState.PARTIALLY_FILLED,
            venue_order_id="987",
            filled=Decimal("0.5"),
            remaining=Decimal("0.73456789"),
            avg_price=Decimal("150.10"),
            last_fill=Fill(
                price=Decimal("150.10"),
                amount=Decimal("0.5"),
                fee=Decimal("0.075"),
                fee_currency="USDT",
                liquidity=Liquidity.TAKER,
            ),
        ),
        LatencyRecord(
            ts_recv=TS,
            intent_id="t-1_lat_a",
            venue="mexc",
            ts_created=TS,
            ts_oms_recv=TS + 50_000,
            ts_broker_send=TS + 120_000,
            ts_broker_ack=TS + 90_000_000,
        ),
    ]


@pytest.mark.parametrize("event", sample_events(), ids=lambda e: type(e).__name__)
def test_roundtrip(event):
    """Every event survives encode and decode unchanged."""
    assert events.decode(events.encode(event)) == event


@pytest.mark.parametrize("event", sample_events(), ids=lambda e: type(e).__name__)
def test_type_tag_and_version(event):
    """The JSON carries the type tag and schema version."""
    payload = json.loads(events.encode(event))
    assert payload["type"] == events.event_type(event).value
    assert payload["v"] == events.SCHEMA_VERSION
    assert EventType(payload["type"]) is events.event_type(event)


@pytest.mark.parametrize("event", sample_events(), ids=lambda e: type(e).__name__)
def test_stream_fields_roundtrip(event):
    """XADD field maps decode whether keys are str or bytes."""
    fields = events.to_stream_fields(event)
    assert fields["type"] == events.event_type(event).value
    assert events.from_stream_fields(fields) == event
    as_bytes = {
        k.encode(): (v if isinstance(v, bytes) else v.encode())
        for k, v in fields.items()
    }
    assert events.from_stream_fields(as_bytes) == event


def test_decimals_are_strings_on_the_wire():
    """Money fields are transported exactly, never as floats."""
    intent = sample_events()[3]
    payload = json.loads(events.encode(intent))
    assert payload["amount"] == "1.23456789"
    assert payload["price"] == "150.10"
    decoded = events.decode(events.encode(intent))
    assert isinstance(decoded, OrderIntent)
    assert decoded.amount == Decimal("1.23456789")


def test_decode_from_foreign_json():
    """A payload written by hand in another language decodes."""
    raw = (
        '{"type":"book","ts_recv":1,"v":1,"venue":"gate","symbol":"BTC/USDT",'
        '"seq":3,"ts_exch":null,"bids":[[1.0,2.0]],"asks":[]}'
    )
    event = events.decode(raw)
    assert isinstance(event, BookEvent)
    assert event.bids == [(1.0, 2.0)]


def test_decode_rejects_unknown_type():
    """An unknown tag is a validation error, not a silent None."""
    with pytest.raises(msgspec.ValidationError):
        events.decode('{"type":"nope","ts_recv":1}')


def test_decode_rejects_missing_field():
    """A missing required field is a validation error."""
    with pytest.raises(msgspec.ValidationError):
        events.decode('{"type":"trade","ts_recv":1,"venue":"gate"}')


def test_intent_defaults():
    """Optional intent fields have sensible defaults."""
    intent = OrderIntent(
        ts_recv=TS,
        intent_id="i",
        strategy="s",
        venue="gate",
        symbol="BTC/USDT",
        side=Side.BUY,
        order_type=OrderKind.MARKET,
        amount=Decimal(1),
    )
    assert intent.price is None
    assert intent.time_in_force is TimeInForce.GTC
    assert intent.replace_of is None
    assert intent.tags == {}


def test_stream_names():
    """Stream naming follows the documented scheme."""
    assert events.book_stream("gate", "BTC/USDT") == "md:book:gate:BTC/USDT"
    assert events.trade_stream("mexc", "SOL/USDT") == "md:trade:mexc:SOL/USDT"
    assert events.balance_stream("gate") == "acct:balance:gate"
    assert events.snapshot_key("gate", "BTC/USDT") == "BTC/USDT-gate"
    assert events.prefixed("bt:run1", "oms:intents") == "bt:run1:oms:intents"
    assert events.prefixed("", "oms:intents") == "oms:intents"


def test_stream_for_routes_every_event():
    """Every event type maps to exactly the stream it belongs on."""
    book, trade, balance, intent, cancel, order_event, latency = sample_events()
    assert events.stream_for(book) == "md:book:gate:BTC/USDT"
    assert events.stream_for(trade) == "md:trade:mexc:SOL/USDT"
    assert events.stream_for(balance) == "acct:balance:gate"
    assert events.stream_for(intent) == events.INTENTS_STREAM
    assert events.stream_for(cancel) == events.INTENTS_STREAM
    assert events.stream_for(order_event) == events.ORDER_EVENTS_STREAM
    assert events.stream_for(latency) == events.LATENCY_STREAM


def test_now_ns_is_nanoseconds():
    """The clock helper returns nanosecond precision epoch time."""
    assert events.now_ns() > 1_700_000_000 * 10**9
