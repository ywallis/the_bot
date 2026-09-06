"""Tests for the market data feed handlers."""

import json

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import watcher
from apps.shared.src import events
from apps.shared.src.config import parse_app_config
from apps.shared.src.errors import NetworkError
from apps.shared.src.events import BookEvent, TradeEvent
from apps.shared.src.streams import StreamPublisher

VENUES = [{"id": "gate", "name": "Gate.io"}, {"id": "mexc", "name": "Mexc"}]


class StopWatching(Exception):
    """Raised by fake clients to break out of a watch loop."""


class FakeClient:
    """A CCXT-like client that replays scripted results then stops."""

    def __init__(self, venue: str, results: list):
        """Store the scripted results, which may be dicts, lists or exceptions."""
        self.id = venue
        self.name = venue.title()
        self._results = list(results)
        self.closed = 0

    async def _next(self):
        if not self._results:
            raise StopWatching()
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def watch_order_book(self, symbol: str):
        """Return the next scripted order book."""
        return await self._next()

    async def watch_trades(self, symbol: str):
        """Return the next scripted trade batch."""
        return await self._next()

    async def close(self):
        """Count close calls."""
        self.closed += 1


def order_book(best_bid: float) -> dict:
    """Return a CCXT-shaped order book with three levels per side."""
    return {
        "symbol": "ALPH/USDT",
        "timestamp": 1_757_160_000_000,
        "bids": [[best_bid, 10.0], [best_bid - 0.01, 20.0], [best_bid - 0.02, 30.0]],
        "asks": [[best_bid + 0.01, 5.0], [best_bid + 0.02, 6.0], [best_bid + 0.03, 7.0]],
    }


@pytest.mark.asyncio
async def test_watch_ob_publishes_event_and_snapshot():
    """Each book update writes the legacy snapshot and a BookEvent."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient("mexc", [order_book(1.10), order_book(1.11)])

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "ALPH/USDT", redis, publisher, depth=2)

    snapshot = json.loads(await redis.get("ALPH/USDT-mexc"))
    assert snapshot["bids"][0][0] == 1.11
    assert len(snapshot["bids"]) == 3, "snapshot keeps the full CCXT book"

    entries = await redis.xrange("md:book:mexc:ALPH/USDT")
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
async def test_watch_ob_recovers_from_network_errors():
    """A NetworkError closes the client and the loop continues."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    client = FakeClient(
        "mexc", [order_book(1.0), NetworkError("blip"), order_book(2.0)]
    )

    with pytest.raises(StopWatching):
        await watcher.watch_ob(client, "ALPH/USDT", redis, publisher, depth=20)

    assert client.closed == 1
    assert await redis.xlen("md:book:mexc:ALPH/USDT") == 2


@pytest.mark.asyncio
async def test_watch_ob_reraises_unknown_errors():
    """Anything not in the recoverable list crashes the handler."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    client = FakeClient("mexc", [ValueError("bad")])
    with pytest.raises(ValueError):
        await watcher.watch_ob(client, "ALPH/USDT", redis, StreamPublisher(10), 20)


@pytest.mark.asyncio
async def test_watch_trades_publishes_one_event_per_trade():
    """A CCXT batch becomes consecutive TradeEvents sharing ts_recv."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    publisher = StreamPublisher(maxlen=100)
    batch = [
        {"id": "1", "timestamp": 1, "side": "buy", "price": 1.0, "amount": 2.0},
        {"id": "2", "timestamp": 2, "side": "sell", "price": 1.1, "amount": 3.0},
    ]
    client = FakeClient("mexc", [batch, [], [batch[1]]])

    with pytest.raises(StopWatching):
        await watcher.watch_trades(client, "ALPH/USDT", redis, publisher)

    entries = await redis.xrange("md:trade:mexc:ALPH/USDT")
    decoded = [events.from_stream_fields(f) for _, f in entries]
    assert [e.seq for e in decoded] == [1, 2, 3]
    assert all(isinstance(e, TradeEvent) for e in decoded)
    assert decoded[0].ts_recv == decoded[1].ts_recv
    assert [e.trade_id for e in decoded] == ["1", "2", "2"]


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
                        {"venue": "gate", "symbol": "BTC/USDT", "feeds": ["book", "trade"]},
                        {"venue": "mexc", "symbol": "SOL/USDT"},
                    ],
                },
                {
                    "identifier": "tst",
                    "type": "x",
                    "production": False,
                    "subscriptions": [{"venue": "mexc", "symbol": "ETH/USDT"}],
                },
            ],
        }
    )
    clients = {"gate": FakeClient("gate", []), "mexc": FakeClient("mexc", [])}
    tasks = watcher.build_tasks(config, clients, None, StreamPublisher(10), True)
    names = sorted(t.__name__ for t in tasks)
    for t in tasks:
        t.close()
    assert names == ["watch_ob", "watch_ob", "watch_trades"]
