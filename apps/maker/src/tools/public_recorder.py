"""Record public books and trades straight from a venue, without Redis.

The trading system records what its feed handlers publish (``recorder.py``).
This tool records markets the system does not trade: it is what the screener
shortlist is replayed from by ``fill_sim`` and the other simulators before a
market is quoted, and it runs on hosts that hold no keys and no Redis, close
to the venues. It writes the same layout as the trading recorder,
``<root>/md/<book|trade>/<venue>/<BASE-QUOTE>/<UTC hour>.jsonl``, with the
same event schema, so every tool reads either recording and the shipper
moves both.

Usage
-----
    uv run -m apps.maker.src.tools.public_recorder data venue_a BASE/QUOTE OTHER/QUOTE
    uv run -m apps.maker.src.tools.public_recorder data venue_a BASE/QUOTE:QUOTE

Contract symbols (``BASE/QUOTE:SETTLE``) point the client at the venue's
futures markets; spot and contract symbols cannot share one process. One
process holds one connection, and some venues silently drop subscriptions
past a handful per connection, so record a long list in groups of ten.

A book that stays silent for ``--stall-s`` seconds makes the process exit
with status 3, for its supervisor to restart it with fresh connections and
snapshots. After a reconnect CCXT can leave a book's watch pending forever
without raising while the trades on the same connection resume: a recording
lost one venue's book for twelve hours that way on 2026-10-14. Healthy books
were seen silent for at most about two minutes, hence the three minute
default. Run it under systemd with ``Restart=always``; see
``docs/runbooks/public-recorder.md``.
"""

import argparse
import asyncio
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import ccxt.pro as ccxt  # pyright: ignore[reportMissingTypeStubs]

from apps.maker.src.structs import LimitedSet
from apps.shared.src.ccxt_events import (
    book_event_from_ccxt,
    trade_event_from_ccxt,
    trade_key,
    usable_trades,
)
from apps.shared.src.config import SWAP_MARKET, is_contract
from apps.shared.src.events import (
    BookEvent,
    TradeEvent,
    book_stream,
    encode,
    event_type,
    now_ns,
    trade_stream,
)
from apps.shared.src.streams import StreamWriter, stream_path

# Levels per side kept from each book update, as the trading recorder keeps.
BOOK_DEPTH = 20
# Seconds a book may stay silent before the process exits for a restart.
STALL_S = 180.0
# Exit status of a stalled process, distinct from a crash in the journal.
STALL_EXIT_CODE = 3
# Seconds between watchdog checks, at most.
WATCHDOG_TICK_S = 10.0
# Seconds past the end of an hour before a quiet stream's file is closed.
SEAL_GRACE_S = 5.0
# Seconds to wait after a feed error before watching again.
RETRY_DELAY_S = 2.0
# Trades remembered per symbol to drop the cache CCXT replays on resubscribe,
# as the trading watcher does.
TRADE_DEDUPE_SIZE = 2000


class BookStalled(Exception):
    """A book went without an update for longer than the stall limit."""


class PublicRecorder:
    """
    Write the books and trades of one venue's symbols to hourly files.

    Attributes
    ----------
    root : Path
        Recording root directory.
    venue : str
        CCXT id, used as the venue in stream names.
    symbols : list[str]
        CCXT symbols recorded.
    stall_s : float
        Seconds a book may stay silent before ``check_stalls`` raises.
    last_book : dict[str, float]
        Monotonic time of each symbol's last written book.
    """

    def __init__(
        self,
        root: Path,
        venue: str,
        symbols: list[str],
        stall_s: float = STALL_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """
        Initialize the recorder.

        Parameters
        ----------
        root : Path
            Recording root directory.
        venue : str
            CCXT id, used as the venue in stream names.
        symbols : list[str]
            CCXT symbols to record.
        stall_s : float
            Seconds a book may stay silent before ``check_stalls`` raises.
        clock : Callable[[], float]
            Monotonic clock in seconds, replaced in tests.
        """
        self.root = root
        self.venue = venue
        self.symbols = symbols
        self.stall_s = stall_s
        self._clock = clock
        self._started = clock()
        self.last_book: dict[str, float] = {}
        self._writers: dict[str, StreamWriter] = {}
        self._seq: dict[str, int] = {}
        self._seen: dict[str, LimitedSet] = {}

    def next_seq(self, stream: str) -> int:
        """
        Return the next sequence number of a stream, starting at 1.

        Parameters
        ----------
        stream : str
            Stream name.

        Returns
        -------
        int
            The number.
        """
        self._seq[stream] = self._seq.get(stream, 0) + 1
        return self._seq[stream]

    def write(self, stream: str, event: BookEvent | TradeEvent) -> None:
        """
        Append one event to its stream's file and flush it.

        Each line is flushed so a restart loses nothing already received.
        The line id is the receive time in milliseconds, in the form of a
        Redis stream id, which is what selects the hour bucket.

        Parameters
        ----------
        stream : str
            Stream name.
        event : BookEvent | TradeEvent
            The event.
        """
        writer = self._writers.get(stream)
        if writer is None:
            writer = StreamWriter(stream_path(self.root, stream))
            self._writers[stream] = writer
        entry_id = f"{event.ts_recv // 1_000_000}-0"
        writer.write(entry_id, event_type(event).value, encode(event))
        writer.flush()

    def record_book(self, symbol: str, order_book: dict[str, Any]) -> bool:
        """
        Write a book update unless it is one-sided or crossed.

        Parameters
        ----------
        symbol : str
            CCXT symbol.
        order_book : dict[str, Any]
            CCXT unified order book.

        Returns
        -------
        bool
            True if the update was written.
        """
        bids = order_book.get("bids") or []
        asks = order_book.get("asks") or []
        if not bids or not asks or bids[0][0] >= asks[0][0]:
            return False
        stream = book_stream(self.venue, symbol)
        event = book_event_from_ccxt(
            self.venue,
            symbol,
            order_book,
            seq=self.next_seq(stream),
            ts_recv=now_ns(),
            depth=BOOK_DEPTH,
        )
        self.write(stream, event)
        self.last_book[symbol] = self._clock()
        return True

    def record_trades(self, symbol: str, trades: list[dict[str, Any]]) -> int:
        """
        Write a batch of trades, skipping replayed ones and unusable ones.

        The filter and the deduplication are the trading watcher's, so both
        recordings of a market hold the same trades. A trade without a side
        is kept, as the watcher keeps it; the simulators skip it.

        Parameters
        ----------
        symbol : str
            CCXT symbol.
        trades : list[dict[str, Any]]
            CCXT unified trades, oldest first.

        Returns
        -------
        int
            Number of trades written.
        """
        stream = trade_stream(self.venue, symbol)
        seen = self._seen.setdefault(symbol, LimitedSet(TRADE_DEDUPE_SIZE))
        ts_recv = now_ns()
        usable, _ = usable_trades(trades)
        written = 0
        for trade in usable:
            key = trade_key(trade)
            if key in seen:
                continue
            seen.add(key)
            event = trade_event_from_ccxt(
                self.venue, symbol, trade, seq=self.next_seq(stream), ts_recv=ts_recv
            )
            self.write(stream, event)
            written += 1
        return written

    def check_stalls(self) -> None:
        """
        Raise if a book has been silent for longer than the stall limit.

        A symbol that has never written a book counts from the recorder's
        start, so a book that never starts is caught as well.

        Raises
        ------
        BookStalled
            Naming the first silent symbol and how long it has been silent.
        """
        now = self._clock()
        for symbol in self.symbols:
            quiet = now - self.last_book.get(symbol, self._started)
            if quiet > self.stall_s:
                raise BookStalled(f"{self.venue} {symbol} book silent for {quiet:.0f}s")

    def seal_elapsed(self) -> None:
        """Close the files of streams whose hour ended, quiet ones included."""
        now = time.time()
        for writer in self._writers.values():
            writer.seal_if_elapsed(now, SEAL_GRACE_S)

    def close(self) -> None:
        """Flush and close every open file."""
        for writer in self._writers.values():
            writer.close()


async def watch_books(client: Any, recorder: PublicRecorder, symbol: str) -> None:
    """
    Record every update of one symbol's book, forever.

    Parameters
    ----------
    client : Any
        CCXT pro exchange.
    recorder : PublicRecorder
        Where updates are written.
    symbol : str
        CCXT symbol.
    """
    while True:
        try:
            recorder.record_book(symbol, await client.watch_order_book(symbol))
        except Exception as e:  # noqa: BLE001, the watchdog catches what never ends
            print(f"{recorder.venue} {symbol} book {type(e).__name__}: {e}", flush=True)
            await asyncio.sleep(RETRY_DELAY_S)


async def watch_trades(client: Any, recorder: PublicRecorder, symbol: str) -> None:
    """
    Record every trade of one symbol, forever.

    Parameters
    ----------
    client : Any
        CCXT pro exchange created with ``newUpdates``.
    recorder : PublicRecorder
        Where trades are written.
    symbol : str
        CCXT symbol.
    """
    while True:
        try:
            recorder.record_trades(symbol, await client.watch_trades(symbol))
        except Exception as e:  # noqa: BLE001
            print(
                f"{recorder.venue} {symbol} trade {type(e).__name__}: {e}", flush=True
            )
            await asyncio.sleep(RETRY_DELAY_S)


async def watchdog(recorder: PublicRecorder) -> None:
    """
    Seal finished hours and raise once a book stalls.

    Parameters
    ----------
    recorder : PublicRecorder
        The recorder to watch.

    Raises
    ------
    BookStalled
        When a book has been silent for longer than the stall limit.
    """
    tick = min(WATCHDOG_TICK_S, recorder.stall_s / 2)
    while True:
        await asyncio.sleep(tick)
        recorder.seal_elapsed()
        recorder.check_stalls()


def make_client(venue: str, symbols: list[str]) -> Any:
    """
    Build a public CCXT pro client for spot or for contract symbols.

    Parameters
    ----------
    venue : str
        CCXT id.
    symbols : list[str]
        Symbols the client will watch.

    Returns
    -------
    Any
        The exchange.

    Raises
    ------
    ValueError
        If the venue is unknown or the symbols mix spot and contracts.
    """
    if not hasattr(ccxt, venue):
        raise ValueError(f"ccxt.pro has no exchange {venue!r}")
    contracts = {is_contract(symbol) for symbol in symbols}
    if len(contracts) > 1:
        raise ValueError("spot and contract symbols need separate processes")
    options: dict[str, Any] = {}
    if contracts == {True}:
        options["defaultType"] = SWAP_MARKET
    return getattr(ccxt, venue)({"newUpdates": True, "options": options})


async def run(root: Path, venue: str, symbols: list[str], stall_s: float) -> int:
    """
    Record until a book stalls, then return the stall exit status.

    Parameters
    ----------
    root : Path
        Recording root directory.
    venue : str
        CCXT id.
    symbols : list[str]
        CCXT symbols.
    stall_s : float
        Seconds a book may stay silent.

    Returns
    -------
    int
        ``STALL_EXIT_CODE``.
    """
    client = make_client(venue, symbols)
    recorder = PublicRecorder(root, venue, symbols, stall_s)
    tasks = [watchdog(recorder)]
    for symbol in symbols:
        tasks += [
            watch_books(client, recorder, symbol),
            watch_trades(client, recorder, symbol),
        ]
    try:
        await client.load_markets()
        await asyncio.gather(*tasks)
    except BookStalled as e:
        print(f"{e}, exiting for a restart", flush=True)
    finally:
        recorder.close()
        try:
            await asyncio.wait_for(client.close(), RETRY_DELAY_S)
        except Exception as e:  # noqa: BLE001, exiting anyway
            print(f"{venue} close failed: {type(e).__name__}: {e}", flush=True)
    return STALL_EXIT_CODE


def parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Parse the command line.

    Parameters
    ----------
    argv : list[str]
        Arguments without the program name.

    Returns
    -------
    argparse.Namespace
        ``root``, ``venue``, ``symbols`` and ``stall_s``.
    """
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("root", type=Path, help="recording root directory")
    parser.add_argument("venue", help="CCXT exchange id")
    parser.add_argument("symbols", nargs="+", help="CCXT symbols")
    parser.add_argument(
        "--stall-s",
        type=float,
        default=STALL_S,
        help="seconds a book may stay silent before the process exits",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    """
    Run the recorder and exit with its status.

    Parameters
    ----------
    argv : list[str]
        Arguments without the program name.
    """
    args = parse_args(argv)
    sys.exit(asyncio.run(run(args.root, args.venue, args.symbols, args.stall_s)))


if __name__ == "__main__":
    main(sys.argv[1:])
