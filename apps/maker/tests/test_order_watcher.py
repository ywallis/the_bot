"""Tests for the order feed handler."""

import time
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import watcher
from apps.maker.src.order_watcher import (
    ORDER_TAG,
    RECONCILE_MARGIN_MS,
    STRATEGY_TAG,
    BoundedDict,
    build_tasks,
    event_from_order,
    fetch_orders_since,
    oid_components,
    strategy_key,
    update_key,
    watch_orders,
)
from apps.maker.tests.conftest import FakeClient, StopWatching
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
)
from apps.shared.src.errors import NetworkError
from apps.shared.src.events import (
    ORDER_EVENTS_STREAM,
    OrderState,
    Side,
    from_stream_fields,
)
from apps.shared.src.streams import StreamPublisher

OID = "t-250906120000123456_lmb_eb"


def ccxt_order(**overrides: Any) -> dict[str, Any]:
    """Return a CCXT unified order, overridable field by field."""
    order = {
        "id": "venue-99",
        "clientOrderId": OID,
        "symbol": "ALPH/USDT",
        "status": "open",
        "side": "sell",
        "price": 0.35,
        "average": None,
        "amount": 40,
        "filled": 0,
        "remaining": 40,
        "timestamp": 1757160000000,
        "fee": None,
        "takerOrMaker": None,
    }
    order.update(overrides)
    return order


# Order id conventions ------------------------------------------------------


def test_oid_components_splits_our_ids():
    """Our client order ids carry the strategy and the order identifier."""
    assert oid_components(OID) == ("lmb", "eb")


@pytest.mark.parametrize("value", [None, "", "manual", "no-underscores", 12345])
def test_oid_components_tolerates_foreign_ids(value: Any):
    """An order placed outside this system must not take the loop down."""
    assert oid_components(value) == ("", "")


def test_strategy_key_matches_what_strategies_submit():
    """Events carry the same strategy key the intents did."""
    assert strategy_key("lmb", "eb") == "lmb_eb"
    assert strategy_key("", "") == ""


# Deduplication and fill deltas ---------------------------------------------


def test_update_key_distinguishes_transitions():
    """The same order in two states is two updates; the same state is one."""
    first = update_key(ccxt_order())
    assert update_key(ccxt_order()) == first
    assert update_key(ccxt_order(status="closed", filled=40)) != first


def test_bounded_dict_forgets_its_oldest_key():
    """The fill memory stays bounded in a process that runs for weeks."""
    memory = BoundedDict(2)
    memory.set("a", Decimal(1))
    memory.set("b", Decimal(2))
    memory.set("c", Decimal(3))

    assert len(memory) == 2
    assert memory.get("a", Decimal(-1)) == Decimal(-1)
    assert memory.get("c", Decimal(-1)) == Decimal(3)


def test_bounded_dict_refreshes_a_key_it_already_holds():
    """Updating an order does not count as a new key."""
    memory = BoundedDict(2)
    memory.set("a", Decimal(1))
    memory.set("b", Decimal(2))
    memory.set("a", Decimal(3))
    memory.set("c", Decimal(4))

    assert memory.get("a", Decimal(-1)) == Decimal(3)
    assert memory.get("b", Decimal(-1)) == Decimal(-1)


# Conversion ----------------------------------------------------------------


def test_event_from_order_reports_an_open_order():
    """An untouched order is open and carries no fill."""
    event = event_from_order("mexc", ccxt_order(), 1, BoundedDict(10))

    assert event.state is OrderState.OPEN
    assert event.side is Side.SELL
    assert event.venue == "mexc"
    assert event.intent_id == OID
    assert event.venue_order_id == "venue-99"
    assert event.strategy == "lmb_eb"
    assert event.tags == {STRATEGY_TAG: "lmb", ORDER_TAG: "eb"}
    assert event.last_fill is None
    assert event.ts_exch == 1757160000000


def test_event_from_order_reports_a_partial_fill():
    """A CCXT order still called open is partially filled once anything trades."""
    event = event_from_order(
        "mexc", ccxt_order(filled=10, remaining=30, average=0.351), 1, BoundedDict(10)
    )

    assert event.state is OrderState.PARTIALLY_FILLED
    assert event.filled == Decimal("10")
    assert event.remaining == Decimal("30")
    assert event.avg_price == Decimal("0.351")
    assert event.last_fill is not None
    assert event.last_fill.amount == Decimal("10")


def test_successive_updates_report_the_fill_between_them():
    """CCXT reports cumulative fills, so the delta is what actually traded."""
    memory = BoundedDict(10)
    event_from_order("mexc", ccxt_order(filled=10, average=0.351), 1, memory)
    second = event_from_order(
        "mexc",
        ccxt_order(status="closed", filled=40, remaining=0, average=0.352),
        2,
        memory,
    )

    assert second.state is OrderState.FILLED
    assert second.last_fill is not None
    assert second.last_fill.amount == Decimal("30")


def test_a_cancelled_order_is_reported_as_cancelled():
    """Both CCXT spellings of cancelled map to the same state."""
    for status in ("canceled", "cancelled"):
        event = event_from_order("mexc", ccxt_order(status=status), 1, BoundedDict(10))
        assert event.state is OrderState.CANCELLED


def test_amounts_avoid_binary_float_noise():
    """Money-touching figures go through Decimal by way of the string form."""
    event = event_from_order(
        "mexc", ccxt_order(filled=0.1, remaining=0.2), 1, BoundedDict(10)
    )

    assert event.filled == Decimal("0.1")
    assert event.remaining == Decimal("0.2")


# The loop ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_watch_orders_publishes_every_transition():
    """Each order update becomes one event on the order events stream."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient(
        "mexc", [[ccxt_order()], [ccxt_order(status="closed", filled=40, remaining=0)]]
    )

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, publisher)

    events = [
        from_stream_fields(fields)
        for _id, fields in await redis.xrange(ORDER_EVENTS_STREAM)
    ]
    assert [e.state for e in events] == [OrderState.OPEN, OrderState.FILLED]


@pytest.mark.asyncio
async def test_watch_orders_drops_a_replayed_update():
    """Venues replay their cache on resubscribe; the same state is published once."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient(
        "mexc", [[ccxt_order()], [ccxt_order()], [ccxt_order(status="closed")]]
    )

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, publisher)

    events = [
        from_stream_fields(fields)
        for _id, fields in await redis.xrange(ORDER_EVENTS_STREAM)
    ]
    assert [e.state for e in events] == [OrderState.OPEN, OrderState.FILLED]


@pytest.mark.asyncio
async def test_watch_orders_recovers_from_a_network_error(monkeypatch):
    """A dropped connection closes the client and the loop continues."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("mexc", [NetworkError("gone"), [ccxt_order()]])

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, publisher)

    assert client.closed == 1
    assert await redis.xlen(ORDER_EVENTS_STREAM) == 1


@pytest.mark.asyncio
async def test_watch_orders_publishes_an_unattributed_order(monkeypatch):
    """An order placed by hand is reported, with no strategy to route it to."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("mexc", [[ccxt_order(clientOrderId="placed by hand")]])

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, publisher)

    entries = await redis.xrange(ORDER_EVENTS_STREAM)
    event = from_stream_fields(entries[0][1])
    assert event.strategy == ""
    assert event.tags == {STRATEGY_TAG: "", ORDER_TAG: ""}


# Wiring --------------------------------------------------------------------


def watching_config() -> AppConfig:
    """Return a config subscribing to two venues, one of them unauthenticated."""
    return AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(),
        strategies=(
            StrategyConfig(
                identifier="lmb",
                type="single_edge_liquidity",
                production=False,
                subscriptions=(
                    Subscription(venue="mexc", symbol="ALPH/USDT"),
                    Subscription(venue="bitget", symbol="ALPH/USDT"),
                ),
                params={},
            ),
        ),
    )


def test_build_tasks_follows_subscriptions():
    """One loop per venue and symbol the strategies declare."""
    redis = MagicMock()
    clients = {"mexc": MagicMock(), "bitget": MagicMock()}
    tasks = build_tasks(watching_config(), clients, redis, StreamPublisher(1), False)

    assert len(tasks) == 2
    for task in tasks:
        task.close()


def test_build_tasks_skips_venues_without_credentials():
    """A venue declared for market data alone is not watched for orders."""
    redis = MagicMock()
    clients = {"mexc": MagicMock()}
    tasks = build_tasks(watching_config(), clients, redis, StreamPublisher(1), False)

    assert len(tasks) == 1
    for task in tasks:
        task.close()


def test_build_tasks_ignores_the_other_production_group():
    """Only the strategies of the running mode are watched."""
    redis = MagicMock()
    clients = {"mexc": AsyncMock(), "bitget": AsyncMock()}
    tasks = build_tasks(watching_config(), clients, redis, StreamPublisher(1), True)

    assert tasks == []


@pytest.mark.asyncio
async def test_watch_orders_asks_only_for_what_happens_from_now():
    """Orders that predate the loop belong to the order manager's recollection."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    client = FakeClient("mexc", [[ccxt_order()]])
    started_ms = int(time.time() * 1000)

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, StreamPublisher(maxlen=100))

    assert client.since is not None
    assert abs(client.since - started_ms) < 5000


# Reconciliation after a reconnect -------------------------------------------


async def events_on(redis: Any) -> list[Any]:
    """Return every order event published, oldest first."""
    return [from_stream_fields(f) for _id, f in await redis.xrange(ORDER_EVENTS_STREAM)]


@pytest.mark.asyncio
async def test_a_reconnect_publishes_the_fill_the_socket_missed(monkeypatch):
    """An order placed and filled while the socket was down is reported from REST."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    filled_in_the_gap = ccxt_order(
        clientOrderId="t-250906120001000000_lmb_es",
        id="venue-100",
        status="closed",
        filled=40,
        remaining=0,
        average=0.35,
    )
    client = FakeClient(
        "mexc",
        [[ccxt_order()], NetworkError("gone"), [ccxt_order(status="canceled")]],
        rest_results=[[ccxt_order(), filled_in_the_gap]],
    )

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, StreamPublisher(maxlen=100))

    events = await events_on(redis)
    assert [(e.intent_id[-6:], e.state) for e in events] == [
        ("lmb_eb", OrderState.OPEN),
        ("lmb_es", OrderState.FILLED),
        ("lmb_eb", OrderState.CANCELLED),
    ]
    filled = events[1]
    assert filled.filled == Decimal(40)
    assert filled.last_fill is not None and filled.last_fill.amount == Decimal(40)
    assert filled.tags[STRATEGY_TAG] == "lmb"
    assert [name for name, _ in client.rest_calls] == ["fetch_orders"]


@pytest.mark.asyncio
async def test_reconciliation_fetches_from_before_the_last_delivery(monkeypatch):
    """The REST window starts a margin before the last update the socket delivered."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    client = FakeClient("mexc", [[ccxt_order()], NetworkError("gone"), [ccxt_order()]])
    before = int(time.time() * 1000)

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, StreamPublisher(maxlen=100))

    ((_name, since),) = client.rest_calls
    assert (
        before - RECONCILE_MARGIN_MS - 2000
        <= since
        <= before - RECONCILE_MARGIN_MS + 5000
    )


@pytest.mark.asyncio
async def test_no_reconciliation_without_a_reconnect():
    """A healthy socket never triggers a REST fetch."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    client = FakeClient("mexc", [[ccxt_order()], [ccxt_order(status="closed")]])

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, StreamPublisher(maxlen=100))

    assert client.rest_calls == []


@pytest.mark.asyncio
async def test_reconciliation_does_not_republish_what_the_socket_reported(monkeypatch):
    """REST returning a state already published adds nothing."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    client = FakeClient(
        "mexc",
        [
            [ccxt_order()],
            NetworkError("gone"),
            [ccxt_order()],
            [ccxt_order(status="closed")],
        ],
        rest_results=[[ccxt_order()]],
    )

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, StreamPublisher(maxlen=100))

    assert [e.state for e in await events_on(redis)] == [
        OrderState.OPEN,
        OrderState.FILLED,
    ]


@pytest.mark.asyncio
async def test_a_failed_reconciliation_does_not_take_the_feed_down(monkeypatch):
    """A REST error is logged and the socket loop carries on."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    client = FakeClient(
        "mexc",
        [NetworkError("gone"), [ccxt_order()]],
        rest_results=[RuntimeError("rest is down too")],
    )

    with pytest.raises(StopWatching):
        await watch_orders(client, "ALPH/USDT", redis, StreamPublisher(maxlen=100))

    assert [e.state for e in await events_on(redis)] == [OrderState.OPEN]


@pytest.mark.asyncio
async def test_fetch_orders_since_uses_one_call_where_the_venue_has_it():
    """MEXC has fetchOrders."""
    client = FakeClient(
        "mexc", [], rest_results=[[ccxt_order()]], has={"fetchOrders": True}
    )
    assert len(await fetch_orders_since(client, "ALPH/USDT", 1)) == 1
    assert [name for name, _ in client.rest_calls] == ["fetch_orders"]


@pytest.mark.asyncio
async def test_fetch_orders_since_combines_endpoints_where_it_must():
    """Bitget has no fetchOrders: open plus cancelled-and-closed cover every state."""
    client = FakeClient(
        "bitget",
        [],
        rest_results=[
            [ccxt_order()],
            [ccxt_order(status="closed"), ccxt_order(status="canceled")],
        ],
        has={
            "fetchOrders": False,
            "fetchOpenOrders": True,
            "fetchCanceledAndClosedOrders": True,
        },
    )
    orders = await fetch_orders_since(client, "ALPH/USDT", 1)
    assert len(orders) == 3
    assert [name for name, _ in client.rest_calls] == [
        "fetch_open_orders",
        "fetch_canceled_and_closed_orders",
    ]


@pytest.mark.asyncio
async def test_fetch_orders_since_falls_back_to_closed_and_cancelled():
    """A venue with separate closed and cancelled endpoints gets both asked."""
    client = FakeClient(
        "other",
        [],
        rest_results=[
            [],
            [ccxt_order(status="closed")],
            [ccxt_order(status="canceled")],
        ],
        has={
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            "fetchCanceledOrders": True,
        },
    )
    assert len(await fetch_orders_since(client, "ALPH/USDT", 1)) == 2
    assert [name for name, _ in client.rest_calls] == [
        "fetch_open_orders",
        "fetch_closed_orders",
        "fetch_canceled_orders",
    ]
