"""Tests for the standalone public market recorder."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from apps.maker.src.tools import public_recorder as pr
from apps.maker.src.tools.fill_sim import stream_events
from apps.shared.src.events import (
    BookEvent,
    Side,
    TradeEvent,
    book_stream,
    trade_stream,
)
from apps.shared.src.streams import recording_files, stream_path


def order_book(best_bid: float) -> dict:
    """Return a CCXT-shaped order book with two levels per side."""
    return {
        "timestamp": 1_757_160_000_000,
        "bids": [[best_bid, 10.0], [best_bid - 0.01, 20.0]],
        "asks": [[best_bid + 0.01, 5.0], [best_bid + 0.02, 6.0]],
    }


def trade(trade_id: str, side: str | None = "buy", price: float | None = 1.0) -> dict:
    """Return a CCXT-shaped trade."""
    return {
        "id": trade_id,
        "timestamp": 1_757_160_000_000,
        "side": side,
        "price": price,
        "amount": 2.0,
    }


class FakeClock:
    """A monotonic clock moved by hand."""

    def __init__(self) -> None:
        """Start at zero."""
        self.now = 0.0

    def __call__(self) -> float:
        """Return the current time."""
        return self.now


def read(root: Path, stream: str) -> list:
    """Return every event recorded on a stream, through the replay reader."""
    return list(stream_events(stream_path(root, stream), 0, 2**63 - 1))


def test_books_are_recorded_in_the_trading_recorders_layout(tmp_path):
    """A book lands in an hourly file under its stream, readable by the simulators."""
    recorder = pr.PublicRecorder(tmp_path, "venue_a", ["BASE/QUOTE"])
    assert recorder.record_book("BASE/QUOTE", order_book(1.0))
    assert recorder.record_book("BASE/QUOTE", order_book(1.1))
    recorder.close()

    stream = book_stream("venue_a", "BASE/QUOTE")
    files = recording_files(stream_path(tmp_path, stream))
    assert [f.parent.relative_to(tmp_path) for f in files] == [
        Path("md/book/venue_a/BASE-QUOTE")
    ]
    events = read(tmp_path, stream)
    assert [type(e) for e in events] == [BookEvent, BookEvent]
    assert [e.seq for e in events] == [1, 2]
    assert events[1].bids[0] == (1.1, 10.0)
    assert events[0].ts_exch == 1_757_160_000_000


def test_a_crossed_or_one_sided_book_is_not_recorded(tmp_path):
    """Only a book with both sides and the bid below the ask is written."""
    recorder = pr.PublicRecorder(tmp_path, "venue_a", ["BASE/QUOTE"])
    crossed = order_book(1.0)
    crossed["asks"] = [[0.99, 1.0]]
    one_sided = {"bids": [[1.0, 1.0]], "asks": []}

    assert not recorder.record_book("BASE/QUOTE", crossed)
    assert not recorder.record_book("BASE/QUOTE", one_sided)
    assert recorder.last_book == {}
    assert not stream_path(tmp_path, book_stream("venue_a", "BASE/QUOTE")).exists()


def test_trades_are_filtered_as_the_trading_watcher_filters_them(tmp_path):
    """A trade without a price or size is dropped, one without a side kept."""
    recorder = pr.PublicRecorder(tmp_path, "venue_a", ["BASE/QUOTE"])
    no_size = trade("3")
    no_size["amount"] = None
    written = recorder.record_trades(
        "BASE/QUOTE",
        [trade("1"), trade("2", side=None), no_size, trade("4", price=None)],
    )
    recorder.close()

    assert written == 2
    events = read(tmp_path, trade_stream("venue_a", "BASE/QUOTE"))
    assert all(isinstance(e, TradeEvent) for e in events)
    assert [(e.trade_id, e.side, e.seq) for e in events] == [
        ("1", Side.BUY, 1),
        ("2", None, 2),
    ]


def test_a_trade_ccxt_left_without_a_price_takes_it_from_info(tmp_path):
    """The raw price fallback of the trading watcher applies here too."""
    recorder = pr.PublicRecorder(tmp_path, "venue_a", ["BASE/QUOTE"])
    missed = trade("1", price=None)
    missed["info"] = {"p": "1.5"}
    assert recorder.record_trades("BASE/QUOTE", [missed]) == 1
    recorder.close()

    events = read(tmp_path, trade_stream("venue_a", "BASE/QUOTE"))
    assert [e.price for e in events] == [1.5]


def test_replayed_trades_are_written_once(tmp_path):
    """A cache CCXT replays after a resubscription does not double the prints."""
    recorder = pr.PublicRecorder(tmp_path, "venue_a", ["BASE/QUOTE", "OTHER/QUOTE"])
    assert recorder.record_trades("BASE/QUOTE", [trade("1"), trade("2")]) == 2
    assert recorder.record_trades("BASE/QUOTE", [trade("2"), trade("3")]) == 1
    assert recorder.record_trades("OTHER/QUOTE", [trade("1")]) == 1
    recorder.close()

    events = read(tmp_path, trade_stream("venue_a", "BASE/QUOTE"))
    assert [e.trade_id for e in events] == ["1", "2", "3"]


class ScriptedClient:
    """A client whose watches return or raise scripted results, then stop."""

    def __init__(self, books: list, trades: list) -> None:
        """Script the book and trade results in order."""
        self.books = books
        self.trades = trades

    @staticmethod
    def _next(script: list):
        """Return or raise the next result, cancelling once the script ends."""
        if not script:
            raise asyncio.CancelledError()
        result = script.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def watch_order_book(self, symbol: str) -> dict:
        """Return the next scripted book."""
        return self._next(self.books)

    async def watch_trades(self, symbol: str) -> list:
        """Return the next scripted batch of trades."""
        return self._next(self.trades)


@pytest.mark.asyncio
async def test_watch_books_keeps_recording_after_an_error(tmp_path, monkeypatch):
    """A feed error is logged and the book is watched again."""
    monkeypatch.setattr(pr, "RETRY_DELAY_S", 0)
    recorder = pr.PublicRecorder(tmp_path, "venue_a", ["BASE/QUOTE"])
    client = ScriptedClient(
        [order_book(1.0), RuntimeError("drop"), order_book(1.1)], []
    )

    with pytest.raises(asyncio.CancelledError):
        await pr.watch_books(client, recorder, "BASE/QUOTE")
    recorder.close()

    events = read(tmp_path, book_stream("venue_a", "BASE/QUOTE"))
    assert [e.bids[0][0] for e in events] == [1.0, 1.1]


@pytest.mark.asyncio
async def test_watch_trades_drops_the_cache_replayed_after_an_error(
    tmp_path, monkeypatch
):
    """Trades replayed after a feed error are written only once."""
    monkeypatch.setattr(pr, "RETRY_DELAY_S", 0)
    recorder = pr.PublicRecorder(tmp_path, "venue_a", ["BASE/QUOTE"])
    client = ScriptedClient(
        [],
        [[trade("1")], RuntimeError("drop"), [trade("1"), trade("2")]],
    )

    with pytest.raises(asyncio.CancelledError):
        await pr.watch_trades(client, recorder, "BASE/QUOTE")
    recorder.close()

    events = read(tmp_path, trade_stream("venue_a", "BASE/QUOTE"))
    assert [e.trade_id for e in events] == ["1", "2"]


def test_a_silent_book_raises_once_past_the_limit(tmp_path):
    """Each book is held to the limit from its last write."""
    clock = FakeClock()
    recorder = pr.PublicRecorder(
        tmp_path, "venue_a", ["BASE/QUOTE", "OTHER/QUOTE"], stall_s=180, clock=clock
    )
    clock.now = 100
    recorder.record_book("BASE/QUOTE", order_book(1.0))
    recorder.record_book("OTHER/QUOTE", order_book(2.0))
    clock.now = 250
    recorder.record_book("OTHER/QUOTE", order_book(2.0))
    clock.now = 280
    recorder.check_stalls()

    clock.now = 281
    with pytest.raises(pr.BookStalled, match="BASE/QUOTE book silent for 181s"):
        recorder.check_stalls()


def test_a_book_that_never_starts_counts_from_the_start(tmp_path):
    """A subscription that never delivers is a stall, not a quiet market."""
    clock = FakeClock()
    recorder = pr.PublicRecorder(tmp_path, "venue_a", ["BASE/QUOTE"], 180, clock)
    clock.now = 180
    recorder.check_stalls()
    clock.now = 181
    with pytest.raises(pr.BookStalled):
        recorder.check_stalls()


class HangingClient:
    """A client whose books deliver a few updates, then never again."""

    def __init__(self, books: int) -> None:
        """Script how many book updates arrive before the watch hangs."""
        self.books = books
        self.closed = False

    async def load_markets(self) -> None:
        """Pretend to load markets."""

    async def watch_order_book(self, symbol: str) -> dict:
        """Return a book until the script runs out, then wait forever."""
        if self.books == 0:
            await asyncio.Event().wait()
        self.books -= 1
        await asyncio.sleep(0)
        return order_book(1.0)

    async def watch_trades(self, symbol: str) -> list:
        """Wait forever, like a market without prints."""
        await asyncio.Event().wait()
        return []

    async def close(self) -> None:
        """Record the close."""
        self.closed = True


@pytest.mark.asyncio
async def test_run_exits_with_the_stall_status_when_a_book_hangs(tmp_path, monkeypatch):
    """A watch CCXT leaves pending ends the process instead of recording nothing."""
    client = HangingClient(books=3)
    monkeypatch.setattr(pr, "make_client", lambda venue, symbols: client)

    status = await asyncio.wait_for(
        pr.run(tmp_path, "venue_a", ["BASE/QUOTE"], stall_s=0.05), 5
    )

    assert status == pr.STALL_EXIT_CODE
    assert client.closed
    assert len(read(tmp_path, book_stream("venue_a", "BASE/QUOTE"))) == 3


def test_make_client_sends_contracts_to_the_futures_markets(monkeypatch):
    """Contract symbols get the swap default type, spot symbols none."""
    monkeypatch.setattr(pr, "ccxt", SimpleNamespace(venue_a=lambda config: config))

    spot = pr.make_client("venue_a", ["BASE/QUOTE"])
    swap = pr.make_client("venue_a", ["BASE/QUOTE:QUOTE"])

    assert spot == {"newUpdates": True, "options": {}}
    assert swap["options"] == {"defaultType": "swap"}


def test_make_client_rejects_mixed_markets_and_unknown_venues(monkeypatch):
    """One process records one kind of market on a venue CCXT knows."""
    monkeypatch.setattr(pr, "ccxt", SimpleNamespace(venue_a=lambda config: config))

    with pytest.raises(ValueError, match="separate processes"):
        pr.make_client("venue_a", ["BASE/QUOTE", "BASE/QUOTE:QUOTE"])
    with pytest.raises(ValueError, match="no exchange"):
        pr.make_client("venue_b", ["BASE/QUOTE"])
