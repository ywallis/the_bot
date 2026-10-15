"""Tests for the market data feed handlers."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import watcher
from apps.maker.tests.conftest import HANG, FakeClient, StopWatching
from apps.shared.src import events
from apps.shared.src.config import parse_app_config
from apps.shared.src.errors import (
    ChecksumError,
    ExchangeError,
    NetworkError,
    UnsubscribeError,
)
from apps.shared.src.events import BookEvent, TradeEvent
from apps.shared.src.streams import StreamPublisher

VENUES = [{"id": "venue_z", "name": "Venue Z"}, {"id": "venue_x", "name": "Venue X"}]


def order_book(best_bid: float) -> dict:
    """Return a CCXT-shaped order book with three levels per side."""
    return {
        "symbol": "BASE/USDT",
        "timestamp": 1_757_160_000_000,
        "bids": [[best_bid, 10.0], [best_bid - 0.01, 20.0], [best_bid - 0.02, 30.0]],
        "asks": [
            [best_bid + 0.01, 5.0],
            [best_bid + 0.02, 6.0],
            [best_bid + 0.03, 7.0],
        ],
    }


@pytest.mark.asyncio
async def test_watch_ob_publishes_event_and_snapshot():
    """Each book update writes the legacy snapshot and a BookEvent."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("venue_x", [order_book(1.10), order_book(1.11)])

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/USDT", redis, publisher, depth=2)

    snapshot = json.loads(await redis.get("BASE/USDT-venue_x"))
    assert snapshot["bids"][0][0] == 1.11
    assert len(snapshot["bids"]) == 3, "snapshot keeps the full CCXT book"

    entries = await redis.xrange("md:book:venue_x:BASE/USDT")
    assert len(entries) == 2
    first = events.from_stream_fields(entries[0][1])
    second = events.from_stream_fields(entries[1][1])
    assert isinstance(first, BookEvent) and isinstance(second, BookEvent)
    assert (first.seq, second.seq) == (1, 2)
    assert first.bids == [(1.10, 10.0), (1.09, 20.0)]
    assert second.bids[0] == (1.11, 10.0)
    assert first.ts_exch == 1_757_160_000_000
    assert second.ts_recv >= first.ts_recv > 1_700_000_000 * 10**9


@pytest.mark.asyncio
async def test_watch_ob_recovers_from_network_errors(monkeypatch):
    """A NetworkError closes the client and the loop continues."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient(
        "venue_x", [order_book(1.0), NetworkError("blip"), order_book(2.0)]
    )

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/USDT", redis, publisher, depth=20)

    assert client.closed == 1
    assert await redis.xlen("md:book:venue_x:BASE/USDT") == 2


@pytest.mark.asyncio
async def test_watch_ob_resubscribes_without_closing(monkeypatch):
    """Checksum and unsubscribe errors retry but leave the shared client open."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient(
        "venue_y",
        [ChecksumError("bad"), UnsubscribeError("venue_y orderbook"), order_book(1.0)],
    )
    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/USDT", redis, publisher, depth=20)
    assert client.closed == 0
    assert await redis.xlen("md:book:venue_y:BASE/USDT") == 1


def crossed_book() -> dict:
    """Return a book whose stale best ask sits below its best bid."""
    book = order_book(1.10)
    book["asks"] = [[1.05, 1.0], *book["asks"]]
    return book


@pytest.mark.asyncio
async def test_watch_ob_never_publishes_a_crossed_book(monkeypatch):
    """A crossed book is skipped, and the loop goes on with the next update."""
    monkeypatch.setattr(watcher, "CROSSED_RESYNC_S", 60.0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("venue_a", [order_book(1.0), crossed_book(), order_book(1.2)])

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/QUOTE", redis, publisher, depth=2)

    entries = await redis.xrange("md:book:venue_a:BASE/QUOTE")
    bids = [events.from_stream_fields(e[1]).bids[0][0] for e in entries]
    assert bids == [1.0, 1.2]
    assert client.unwatched == []


@pytest.mark.asyncio
async def test_watch_ob_resyncs_a_book_that_stays_crossed(monkeypatch):
    """A book crossed for longer than the window is dropped and resubscribed."""
    monkeypatch.setattr(watcher, "CROSSED_RESYNC_S", 0.0)
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("venue_a", [crossed_book(), order_book(1.0)])
    client.orderbooks["BASE/QUOTE"] = object()

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/QUOTE", redis, publisher, depth=2)

    assert client.unwatched == ["BASE/QUOTE"]
    assert "BASE/QUOTE" not in client.orderbooks
    assert await redis.xlen("md:book:venue_a:BASE/QUOTE") == 1


@pytest.mark.asyncio
async def test_watch_ob_resyncs_a_book_that_goes_silent(monkeypatch):
    """A watch that never returns is dropped and resubscribed, not awaited forever."""
    monkeypatch.setattr(watcher, "BOOK_STALL_S", 0.01)
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("venue_a", [order_book(1.0), HANG, order_book(1.1)])
    client.orderbooks["BASE/QUOTE"] = object()

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/QUOTE", redis, publisher, depth=2)

    assert client.unwatched == ["BASE/QUOTE"]
    assert "BASE/QUOTE" not in client.orderbooks
    assert client.closed == 0
    assert await redis.xlen("md:book:venue_a:BASE/QUOTE") == 2


@pytest.mark.asyncio
async def test_watch_ob_reconnects_after_consecutive_stalls(monkeypatch):
    """A book still silent after a resubscription has its client reconnected."""
    monkeypatch.setattr(watcher, "BOOK_STALL_S", 0.01)
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("venue_a", [HANG, HANG, order_book(1.0)])

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/QUOTE", redis, publisher, depth=2)

    assert client.unwatched == ["BASE/QUOTE", "BASE/QUOTE"]
    assert client.closed == 1
    assert await redis.xlen("md:book:venue_a:BASE/QUOTE") == 1


@pytest.mark.asyncio
async def test_watch_ob_restarts_the_crossed_window_after_a_stall(monkeypatch):
    """A book crossed before a stall is not resynced again on the first update."""
    monkeypatch.setattr(watcher, "BOOK_STALL_S", 0.05)
    monkeypatch.setattr(watcher, "CROSSED_RESYNC_S", 0.02)
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("venue_a", [crossed_book(), HANG, crossed_book()])

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/QUOTE", redis, publisher, depth=2)

    assert client.unwatched == ["BASE/QUOTE"], "only the stall resubscribed"


@pytest.mark.asyncio
async def test_watch_ob_counts_only_consecutive_stalls(monkeypatch):
    """An update between two stalls means the connection works: no reconnect."""
    monkeypatch.setattr(watcher, "BOOK_STALL_S", 0.01)
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("venue_a", [HANG, order_book(1.0), HANG, order_book(1.1)])

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/QUOTE", redis, publisher, depth=2)

    assert len(client.unwatched) == 2
    assert client.closed == 0
    assert await redis.xlen("md:book:venue_a:BASE/QUOTE") == 2


@pytest.mark.asyncio
async def test_watch_trades_drops_replayed_cache(monkeypatch):
    """Trades replayed after a resubscription are published only once."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    cache = [
        {"id": str(i), "timestamp": i, "side": "buy", "price": 1.0, "amount": 1.0}
        for i in range(50)
    ]
    newer = {"id": "50", "timestamp": 50, "side": "sell", "price": 1.0, "amount": 1.0}
    client = FakeClient("venue_y", [cache, UnsubscribeError("x"), cache + [newer]])
    with pytest.raises(StopWatching):
        await watcher.watch_trades(client, "BASE/USDT", redis, publisher)
    entries = await redis.xrange("md:trade:venue_y:BASE/USDT")
    ids = [events.from_stream_fields(f).trade_id for _, f in entries]
    assert ids == [str(i) for i in range(51)]


def test_trade_key_falls_back_without_id():
    """Venues without trade ids are keyed on timestamp, price and amount."""
    assert watcher.trade_key({"id": 7}) == ("id", "7")
    assert watcher.trade_key({"timestamp": 1, "price": 2.0, "amount": 3.0}) == (
        "fields",
        1,
        2.0,
        3.0,
    )


@pytest.mark.asyncio
async def test_watch_ob_reconnects_when_the_venue_asks(monkeypatch):
    """A venue error announcing a socket reset reconnects instead of crashing."""
    monkeypatch.setattr(watcher, "RETRY_DELAY_S", 0)
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    notice = ExchangeError('venue_x {"event":"error","msg":"Please reconnect."}')
    client = FakeClient("venue_x", [order_book(1.0), notice, order_book(2.0)])

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "BASE/USDT", redis, publisher, depth=20)

    assert client.closed == 1
    assert await redis.xlen("md:book:venue_x:BASE/USDT") == 2


class FakeClock:
    """A ``time`` stand-in whose ``monotonic`` only moves when told to."""

    def __init__(self) -> None:
        """Start the clock at an arbitrary instant."""
        self.now = 1000.0

    def monotonic(self) -> float:
        """Return the current fake instant."""
        return self.now


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    """Drive the watcher's backoff clock by hand."""
    fake = FakeClock()
    monkeypatch.setattr(watcher, "time", SimpleNamespace(monotonic=fake.monotonic))
    return fake


NOTICE = ExchangeError('venue_x {"event":"error","msg":"Please reconnect."}')


def test_reconnect_requests_in_a_row_back_off_up_to_a_cap(clock):
    """Each request doubles the wait, which stops at the cap."""
    client = FakeClient("venue_x", [])
    delays = []
    for _ in range(9):
        assert watcher.hold_for_reconnect(NOTICE, client)
        delays.append(watcher.retry_delay(client))
        clock.now += delays[-1]
    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]


def test_reconnect_requests_past_the_limit_are_raised(clock):
    """A venue that keeps asking still stops the system in the end."""
    client = FakeClient("venue_x", [])
    for _ in range(watcher.RECONNECT_HINT_LIMIT):
        watcher.hold_for_reconnect(NOTICE, client)
        clock.now += watcher.RECONNECT_MAX_DELAY_S
    with pytest.raises(ExchangeError):
        watcher.hold_for_reconnect(NOTICE, client)


def test_a_quiet_spell_resets_the_backoff(clock):
    """A request long after the previous one starts from the shortest wait."""
    client = FakeClient("venue_x", [])
    for _ in range(4):
        watcher.hold_for_reconnect(NOTICE, client)
        clock.now += watcher.RECONNECT_MAX_DELAY_S
    clock.now += watcher.RECONNECT_RESET_S
    watcher.hold_for_reconnect(NOTICE, client)
    assert watcher.retry_delay(client) == watcher.RETRY_DELAY_S


@pytest.mark.asyncio
async def test_every_loop_on_a_client_waits_out_its_backoff(clock, monkeypatch):
    """Sibling loops neither close the client again nor reopen it early."""
    slept = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(watcher.asyncio, "sleep", sleep)
    client = FakeClient("venue_x", [])
    for _ in range(3):
        watcher.hold_for_reconnect(NOTICE, client)
        clock.now += watcher.retry_delay(client)

    await watcher.handle_feed_error(NOTICE, client, "watch_ob", "BASE/USDT")
    clock.now += 1.0
    await watcher.handle_feed_error(NOTICE, client, "watch_trades", "BASE/USDT")
    await watcher.handle_feed_error(
        UnsubscribeError("dropped"), client, "watch_trades", "BASE/USDT"
    )

    assert client.closed == 1
    assert slept == [4.0, 3.0, 3.0]


@pytest.mark.asyncio
async def test_watch_ob_reraises_other_venue_errors():
    """A venue error that does not ask for a reconnect still crashes the handler."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    client = FakeClient("venue_x", [ExchangeError("venue_x symbol not found")])
    with pytest.raises(ExchangeError):
        await watcher.watch_ob(client, "BASE/USDT", redis, StreamPublisher(10), 20)


@pytest.mark.asyncio
async def test_watch_ob_reraises_unknown_errors():
    """Anything not in the recoverable list crashes the handler."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    client = FakeClient("venue_x", [ValueError("bad")])
    with pytest.raises(ValueError):
        await watcher.watch_ob(client, "BASE/USDT", redis, StreamPublisher(10), 20)


@pytest.mark.asyncio
async def test_watch_trades_publishes_one_event_per_trade():
    """A CCXT batch becomes consecutive TradeEvents; a replayed trade is dropped."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    batch = [
        {"id": "1", "timestamp": 1, "side": "buy", "price": 1.0, "amount": 2.0},
        {"id": "2", "timestamp": 2, "side": "sell", "price": 1.1, "amount": 3.0},
    ]
    client = FakeClient("venue_x", [batch, [], [batch[1]]])

    with pytest.raises(StopWatching):
        await watcher.watch_trades(client, "BASE/USDT", redis, publisher)

    entries = await redis.xrange("md:trade:venue_x:BASE/USDT")
    decoded = [events.from_stream_fields(f) for _, f in entries]
    assert [e.seq for e in decoded] == [1, 2]
    assert all(isinstance(e, TradeEvent) for e in decoded)
    assert decoded[0].ts_recv == decoded[1].ts_recv
    assert [e.trade_id for e in decoded] == ["1", "2"]


def test_build_tasks_follows_subscriptions():
    """One handler per subscribed feed, for the selected production flag."""
    config = parse_app_config(
        {
            "venues": VENUES,
            "market_data": {"book_depth": 5},
            "strategies": [
                {
                    "identifier": "lat",
                    "type": "x",
                    "production": True,
                    "subscriptions": [
                        {
                            "venue": "venue_z",
                            "symbol": "BTC/USDT",
                            "feeds": ["book", "trade"],
                        },
                        {"venue": "venue_x", "symbol": "SOL/USDT"},
                    ],
                },
                {
                    "identifier": "tst",
                    "type": "x",
                    "production": False,
                    "subscriptions": [{"venue": "venue_x", "symbol": "ETH/USDT"}],
                },
            ],
        }
    )
    clients = {
        "venue_z": FakeClient("venue_z", []),
        "venue_x": FakeClient("venue_x", []),
    }
    tasks = watcher.build_tasks(config, clients, None, StreamPublisher(10), True)
    names = sorted(t.__name__ for t in tasks)
    for t in tasks:
        t.close()
    assert names == ["watch_ob", "watch_ob", "watch_trades"]


@pytest.mark.asyncio
async def test_publish_trades_skips_a_trade_without_price():
    """A trade CCXT parsed without a price or size is dropped, not fatal."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    good = {"id": "1", "timestamp": 1, "side": "buy", "price": 1.0, "amount": 2.0}
    bad = {"id": "2", "timestamp": 2, "side": "buy", "price": None, "amount": 2.0}
    published = await watcher.publish_trades(
        redis, publisher, "venue_a", "BASE/QUOTE", [bad, good], 1, None
    )
    assert published == 1
    assert await redis.xlen("md:trade:venue_a:BASE/QUOTE") == 1


@pytest.mark.asyncio
async def test_publish_trades_reads_the_raw_price_ccxt_missed():
    """A unified-account trade whose price CCXT left empty takes it from info."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    trade = {
        "id": "1",
        "timestamp": 1,
        "side": "sell",
        "price": None,
        "amount": 4.0,
        "info": {"i": "1", "p": "1.5", "v": "4", "S": "sell", "T": "1"},
    }
    assert (
        await watcher.publish_trades(
            redis, publisher, "venue_a", "BASE/QUOTE", [trade], 1, None
        )
        == 1
    )
    entries = await redis.xrange("md:trade:venue_a:BASE/QUOTE")
    assert events.from_stream_fields(entries[0][1]).price == 1.5


@pytest.mark.asyncio
async def test_close_within_gives_up_on_a_hung_close(monkeypatch):
    """A close that never returns is abandoned so the process can exit."""
    monkeypatch.setattr(watcher, "CLOSE_TIMEOUT_S", 0.01)

    async def hang() -> None:
        await asyncio.sleep(3600)

    await watcher.close_within(hang(), "hung client")


@pytest.mark.asyncio
async def test_a_cancelled_loop_ends_instead_of_resubscribing():
    """Cancelling a watch loop ends it; only CCXT's own CancelledError resubscribes."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)

    class Blocking(FakeClient):
        async def watch_order_book(self, symbol: str):
            await asyncio.sleep(3600)

    client = Blocking("venue_a", [])
    task = asyncio.create_task(
        watcher.watch_ob(client, "BASE/QUOTE", redis, publisher, depth=2)
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
