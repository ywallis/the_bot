"""Screen the markets two venues share for a harvestable maker edge.

The maker strategies earn the gap between a quote resting on one venue and
the hedge taken on the other, minus the fees of both legs. Whether a market
can pay that depends on two things no parameter can change: how far apart
the venues' touches sit, and how much volume prints on the maker venue past
the taker's touch, since a quote priced off the other venue only fills when a
print reaches it. This tool measures both over public REST for every market
the venues have in common and ranks them, so the choice of market is made on
data rather than on the pair that happened to be configured first.

Usage
-----
    uv run -m apps.maker.src.tools.market_screener --minutes 10
    uv run -m apps.maker.src.tools.market_screener --venues venue_a venue_b
        --quote USDT --top 30 --interval 6 --json screen.json

    uv run -m apps.maker.src.tools.market_screener --recorded data \
        --symbols BASE/QUOTE --start 2026-09-08T12:00Z

Venues default to the ones in ``config.toml``. Fees default to the static
rates configured per venue and fall back to ``--fee-bps`` when a venue has
none. No keys are needed.

With ``--recorded`` the same arithmetic runs over the recorder's files
instead of live REST: every book update and every trade, which is what the
live sample approximates. That is how a shortlisted market, recorded through
the ``observe`` strategy, is measured before a quoting strategy is pointed
at it.

What the numbers mean: ``harvest`` is the quote notional per hour of maker
venue prints that landed at least an edge past the prevailing taker touch,
which is the volume a quote at that edge could ever have filled, with no
queue in front of it. ``net`` is that volume times the edge net of fees. Both
are upper bounds from a REST sample a few seconds apart, so a market is
compared against the others here rather than trusted in absolute terms; the
shortlist is then recorded properly with the feed handlers and re-measured
with the same arithmetic on every book update.
"""

import argparse
import asyncio
import bisect
import json
import logging
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import ccxt.async_support as ccxt  # pyright: ignore[reportMissingTypeStubs]

from apps.shared.src.config import AppConfig, VenueConfig, load_app_config
from apps.shared.src.events import (
    BookEvent,
    TradeEvent,
    book_stream,
    from_stream_fields,
    trade_stream,
)
from apps.shared.src.streams import recording_files, stream_path

logger = logging.getLogger(__name__)

# Edges, in basis points above the fee budget, at which harvestable volume is
# measured. The smallest is what a quote needs to break even with slippage;
# the largest is where the configured strategies quoted.
EDGES_BPS: tuple[int, ...] = (5, 10, 20, 30)

# A print is matched to a taker snapshot no further away than this, so a
# venue whose feed stalled does not lend its stale touch to another's prints.
MAX_SNAPSHOT_GAP_MS = 30_000

# Depth levels summed for the hedge capacity figure.
DEPTH_LEVELS = 5

# Fee budget assumed when neither venue configures a static rate.
DEFAULT_FEE_BPS = 12.0

# A market whose books are crossed this often is not one market: the two
# venues list different assets under one ticker, or one book is halted. A
# real cross that persistent would be taken by someone within seconds.
SUSPECT_CROSSED_SHARE = 0.5


@dataclass(frozen=True)
class Snapshot:
    """
    Top of one venue's book at one moment.

    Attributes
    ----------
    ts_ms : int
        Local time the book was fetched, milliseconds.
    bid : float
        Best bid price.
    ask : float
        Best ask price.
    bid_depth : float
        Quote notional on the first ``DEPTH_LEVELS`` bid levels.
    ask_depth : float
        Quote notional on the first ``DEPTH_LEVELS`` ask levels.
    """

    ts_ms: int
    bid: float
    ask: float
    bid_depth: float
    ask_depth: float


@dataclass(frozen=True)
class Print:
    """
    One public trade.

    Attributes
    ----------
    ts_ms : int
        Venue time of the trade, milliseconds.
    side : str
        ``buy`` when the aggressor lifted an ask, ``sell`` when it hit a bid.
    price : float
        Trade price.
    amount : float
        Trade size in base asset.
    """

    ts_ms: int
    side: str
    price: float
    amount: float


@dataclass
class Sample:
    """
    Everything collected for one symbol on one venue.

    Attributes
    ----------
    snapshots : list[Snapshot]
        Book tops in fetch order.
    prints : list[Print]
        Distinct trades in time order.
    keys : set[tuple[Any, ...]]
        Identity of every print kept, for deduplication across fetches.
    """

    snapshots: list[Snapshot] = field(default_factory=list)
    prints: list[Print] = field(default_factory=list)
    keys: set[tuple[Any, ...]] = field(default_factory=set)


@dataclass(frozen=True)
class PairScore:
    """
    How one symbol scores with one venue as maker and the other as taker.

    Attributes
    ----------
    symbol : str
        CCXT symbol.
    maker : str
        Venue the quote would rest on.
    taker : str
        Venue the hedge would be taken on.
    fee_bps : float
        Fee budget of the round trip, basis points.
    sell_offset_bps : float
        Median of maker ask over taker ask, minus one, in basis points. A
        positive figure means the maker venue is dearer, which is room for
        a sell quote.
    buy_offset_bps : float
        Median of taker bid over maker bid, minus one, in basis points.
    maker_spread_bps : float
        Median spread of the maker venue's own book.
    taker_depth : float
        Median quote notional on the taker's first ``DEPTH_LEVELS`` levels,
        both sides averaged: what a hedge has to eat into.
    maker_volume_per_h : float
        Quote notional printed on the maker venue per hour of sample.
    crossed_share : float
        Share of snapshots where one venue's bid exceeded the other's ask by
        more than the fee budget.
    harvest_per_h : dict[int, float]
        Quote notional per hour of maker prints at least ``fee_bps`` plus
        the key past the prevailing taker touch, both sides.
    net_per_h : dict[int, float]
        ``harvest_per_h`` times the edge net of fees, quote units per hour.
    hours : float
        Length of the sample in hours.
    """

    symbol: str
    maker: str
    taker: str
    fee_bps: float
    sell_offset_bps: float
    buy_offset_bps: float
    maker_spread_bps: float
    taker_depth: float
    maker_volume_per_h: float
    crossed_share: float
    harvest_per_h: dict[int, float]
    net_per_h: dict[int, float]
    hours: float

    @property
    def suspect(self) -> bool:
        """
        Return whether the two books cross too often to be the same market.

        Returns
        -------
        bool
            True above ``SUSPECT_CROSSED_SHARE``.
        """
        return self.crossed_share > SUSPECT_CROSSED_SHARE

    @property
    def best_net_per_h(self) -> float:
        """
        Return the largest net figure across the measured edges.

        Returns
        -------
        float
            Quote units per hour.
        """
        return max(self.net_per_h.values(), default=0.0)


def common_symbols(markets: dict[str, dict[str, Any]], quote: str) -> list[str]:
    """
    Return the active spot symbols every venue lists against one quote asset.

    Parameters
    ----------
    markets : dict[str, dict[str, Any]]
        CCXT ``markets`` per venue id.
    quote : str
        Quote asset, e.g. ``USDT``.

    Returns
    -------
    list[str]
        Sorted symbols.
    """
    eligible: list[set[str]] = []
    for venue_markets in markets.values():
        eligible.append(
            {
                symbol
                for symbol, market in venue_markets.items()
                if market.get("spot")
                and market.get("active", True)
                and market.get("quote") == quote
            }
        )
    if not eligible:
        return []
    return sorted(set.intersection(*eligible))


def print_key(trade: dict[str, Any]) -> tuple[Any, ...]:
    """
    Return what identifies a trade across two fetches of the same window.

    Parameters
    ----------
    trade : dict[str, Any]
        CCXT trade structure.

    Returns
    -------
    tuple[Any, ...]
        The venue's trade id when it has one, else time, price and size.
    """
    if trade.get("id"):
        return ("id", trade["id"])
    return ("t", trade.get("timestamp"), trade.get("price"), trade.get("amount"))


def add_prints(sample: Sample, trades: list[dict[str, Any]]) -> int:
    """
    Add the trades of one fetch to a sample, skipping those already held.

    Parameters
    ----------
    sample : Sample
        The sample, updated in place.
    trades : list[dict[str, Any]]
        CCXT trade structures.

    Returns
    -------
    int
        Number of new prints.
    """
    added = 0
    for trade in trades:
        key = print_key(trade)
        if key in sample.keys:
            continue
        if trade.get("price") is None or trade.get("amount") is None:
            continue
        sample.keys.add(key)
        sample.prints.append(
            Print(
                ts_ms=int(trade.get("timestamp") or 0),
                side=str(trade.get("side") or ""),
                price=float(trade["price"]),
                amount=float(trade["amount"]),
            )
        )
        added += 1
    sample.prints.sort(key=lambda p: p.ts_ms)
    return added


def snapshot_from_book(book: dict[str, Any], ts_ms: int) -> Snapshot | None:
    """
    Reduce a CCXT order book to its top and near depth.

    Parameters
    ----------
    book : dict[str, Any]
        CCXT order book.
    ts_ms : int
        Local fetch time, milliseconds.

    Returns
    -------
    Snapshot | None
        The snapshot, or None for a one-sided book.
    """
    bids, asks = book.get("bids") or [], book.get("asks") or []
    if not bids or not asks:
        return None
    return Snapshot(
        ts_ms=ts_ms,
        bid=float(bids[0][0]),
        ask=float(asks[0][0]),
        bid_depth=sum(float(p) * float(a) for p, a in bids[:DEPTH_LEVELS]),
        ask_depth=sum(float(p) * float(a) for p, a in asks[:DEPTH_LEVELS]),
    )


def nearest_snapshot(
    snapshots: list[Snapshot], ts_ms: int, times: list[int] | None = None
) -> Snapshot | None:
    """
    Return the snapshot closest in time to a moment, within the allowed gap.

    Parameters
    ----------
    snapshots : list[Snapshot]
        Snapshots in time order.
    ts_ms : int
        The moment, milliseconds.
    times : list[int] | None
        The snapshots' times, precomputed by a caller that asks many times:
        a recording holds a hundred thousand snapshots and rebuilding the
        list per lookup made scoring a few hours take longer than the hours.

    Returns
    -------
    Snapshot | None
        The nearest snapshot, or None if none is within
        ``MAX_SNAPSHOT_GAP_MS``.
    """
    if not snapshots:
        return None
    if times is None:
        times = [s.ts_ms for s in snapshots]
    i = bisect.bisect_left(times, ts_ms)
    candidates = [snapshots[j] for j in (i - 1, i) if 0 <= j < len(snapshots)]
    best = min(candidates, key=lambda s: abs(s.ts_ms - ts_ms))
    if abs(best.ts_ms - ts_ms) > MAX_SNAPSHOT_GAP_MS:
        return None
    return best


def bps(ratio: float) -> float:
    """
    Convert a price ratio to basis points away from parity.

    Parameters
    ----------
    ratio : float
        A price divided by another.

    Returns
    -------
    float
        ``(ratio - 1) * 10_000``.
    """
    return (ratio - 1.0) * 10_000.0


def score_pair(
    symbol: str,
    maker: str,
    taker: str,
    maker_sample: Sample,
    taker_sample: Sample,
    fee_bps: float,
    edges_bps: tuple[int, ...] = EDGES_BPS,
) -> PairScore | None:
    """
    Score one symbol with one venue as maker and the other as taker.

    Parameters
    ----------
    symbol : str
        CCXT symbol.
    maker : str
        Maker venue id.
    taker : str
        Taker venue id.
    maker_sample : Sample
        What was collected on the maker venue.
    taker_sample : Sample
        What was collected on the taker venue.
    fee_bps : float
        Fee budget of the round trip, basis points.
    edges_bps : tuple[int, ...]
        Edges above the fee budget to measure harvestable volume at.

    Returns
    -------
    PairScore | None
        The score, or None when either venue has fewer than two snapshots.
    """
    if len(maker_sample.snapshots) < 2 or len(taker_sample.snapshots) < 2:
        return None
    first = min(maker_sample.snapshots[0].ts_ms, taker_sample.snapshots[0].ts_ms)
    last = max(maker_sample.snapshots[-1].ts_ms, taker_sample.snapshots[-1].ts_ms)
    hours = max((last - first) / 3_600_000.0, 1e-9)

    sell_offsets: list[float] = []
    buy_offsets: list[float] = []
    spreads: list[float] = []
    crossed = 0
    taker_times = [s.ts_ms for s in taker_sample.snapshots]
    for snap in maker_sample.snapshots:
        other = nearest_snapshot(taker_sample.snapshots, snap.ts_ms, taker_times)
        if other is None:
            continue
        sell_offsets.append(bps(snap.ask / other.ask))
        buy_offsets.append(bps(other.bid / snap.bid))
        spreads.append(bps(snap.ask / snap.bid))
        if bps(snap.bid / other.ask) > fee_bps or bps(other.bid / snap.ask) > fee_bps:
            crossed += 1
    if not sell_offsets:
        return None

    harvest = {edge: 0.0 for edge in edges_bps}
    volume = 0.0
    for trade in maker_sample.prints:
        notional = trade.price * trade.amount
        volume += notional
        other = nearest_snapshot(taker_sample.snapshots, trade.ts_ms, taker_times)
        if other is None:
            continue
        if trade.side == "buy":
            landed = bps(trade.price / other.ask)
        elif trade.side == "sell":
            landed = bps(other.bid / trade.price)
        else:
            continue
        for edge in edges_bps:
            if landed >= fee_bps + edge:
                harvest[edge] += notional

    depth = statistics.median(
        (s.bid_depth + s.ask_depth) / 2 for s in taker_sample.snapshots
    )
    return PairScore(
        symbol=symbol,
        maker=maker,
        taker=taker,
        fee_bps=fee_bps,
        sell_offset_bps=statistics.median(sell_offsets),
        buy_offset_bps=statistics.median(buy_offsets),
        maker_spread_bps=statistics.median(spreads),
        taker_depth=depth,
        maker_volume_per_h=volume / hours,
        crossed_share=crossed / len(sell_offsets),
        harvest_per_h={e: v / hours for e, v in harvest.items()},
        net_per_h={e: v / hours * (e / 10_000.0) for e, v in harvest.items()},
        hours=hours,
    )


def rank(scores: list[PairScore]) -> list[PairScore]:
    """
    Order scores from the most to the least promising.

    Parameters
    ----------
    scores : list[PairScore]
        Scores in any order.

    Returns
    -------
    list[PairScore]
        Sorted by the best net figure, then by maker volume.
    """
    return sorted(
        scores, key=lambda s: (s.best_net_per_h, s.maker_volume_per_h), reverse=True
    )


def fee_budget_bps(
    maker: VenueConfig | None, taker: VenueConfig | None, fallback: float
) -> float:
    """
    Return the round-trip fee budget for a maker and taker venue.

    Parameters
    ----------
    maker : VenueConfig | None
        The maker venue's config, if it is configured.
    taker : VenueConfig | None
        The taker venue's config, if it is configured.
    fallback : float
        Budget to use when either venue lacks a static rate.

    Returns
    -------
    float
        Maker fee plus taker fee in basis points, or the fallback.
    """
    if (
        maker is not None
        and taker is not None
        and maker.maker_fee is not None
        and taker.taker_fee is not None
    ):
        return (maker.maker_fee + taker.taker_fee) * 10_000.0
    return fallback


def format_table(scores: list[PairScore], limit: int) -> str:
    """
    Render the top scores as a fixed-width table.

    Parameters
    ----------
    scores : list[PairScore]
        Ranked scores.
    limit : int
        Rows to render.

    Returns
    -------
    str
        The table.
    """
    edges = list(EDGES_BPS)
    head = (
        f"{'symbol':<14}{'maker':<8}{'taker':<8}{'sell':>7}{'buy':>7}{'sprd':>6}"
        f"{'depth':>9}{'vol/h':>10}{'cross':>6}"
        + "".join(f"{'h+' + str(e):>9}" for e in edges)
        + f"{'net/h':>8}"
    )
    lines = [head, "-" * len(head)]
    for s in scores[:limit]:
        symbol = ("!" if s.suspect else "") + s.symbol
        lines.append(
            f"{symbol:<14}{s.maker:<8}{s.taker:<8}{s.sell_offset_bps:>7.1f}"
            f"{s.buy_offset_bps:>7.1f}{s.maker_spread_bps:>6.0f}{s.taker_depth:>9.0f}"
            f"{s.maker_volume_per_h:>10.0f}{s.crossed_share * 100:>5.0f}%"
            + "".join(f"{s.harvest_per_h[e]:>9.1f}" for e in edges)
            + f"{s.best_net_per_h:>8.3f}"
        )
    lines.append(
        "offsets and spread in bps (medians); depth and vol/h in quote units; "
        "h+N is harvestable quote/h at fee+N bps; net/h is the best of those "
        "times its edge; ! marks books crossed more than half the time, which "
        "is two assets under one ticker or a halted book, not an edge"
    )
    return "\n".join(lines)


class Screener:
    """
    Collect books and trades for a set of symbols on several venues.

    Attributes
    ----------
    clients : dict[str, Any]
        Public CCXT clients per venue id.
    samples : dict[tuple[str, str], Sample]
        Collected data per venue and symbol.
    """

    def __init__(self, clients: dict[str, Any]) -> None:
        """
        Initialize with ready clients.

        Parameters
        ----------
        clients : dict[str, Any]
            Public CCXT clients per venue id, markets not yet loaded.
        """
        self.clients = clients
        self.samples: dict[tuple[str, str], Sample] = {}

    async def load_markets(self) -> dict[str, dict[str, Any]]:
        """
        Load every venue's markets.

        Returns
        -------
        dict[str, dict[str, Any]]
            CCXT markets per venue id.
        """
        loaded = await asyncio.gather(
            *(client.load_markets() for client in self.clients.values())
        )
        return dict(zip(self.clients, loaded, strict=True))

    async def volumes(self, symbols: list[str]) -> dict[str, dict[str, float]]:
        """
        Fetch the 24 hour quote volume of every symbol on every venue.

        Parameters
        ----------
        symbols : list[str]
            Symbols to look up.

        Returns
        -------
        dict[str, dict[str, float]]
            Quote volume per venue id and symbol, 0 where unknown.
        """
        out: dict[str, dict[str, float]] = {}
        for venue, client in self.clients.items():
            out[venue] = {}
            try:
                tickers = await client.fetch_tickers(symbols)
            except Exception as error:  # noqa: BLE001, a venue may not batch
                logger.warning(f"fetch_tickers failed on {venue}: {error}")
                continue
            for symbol in symbols:
                ticker = tickers.get(symbol) or {}
                volume = ticker.get("quoteVolume")
                if volume is None and ticker.get("baseVolume") and ticker.get("last"):
                    volume = float(ticker["baseVolume"]) * float(ticker["last"])
                out[venue][symbol] = float(volume or 0.0)
        return out

    async def fetch_one(self, venue: str, symbol: str) -> None:
        """
        Fetch one book and the recent trades of one symbol on one venue.

        Parameters
        ----------
        venue : str
            Venue id.
        symbol : str
            CCXT symbol.
        """
        client = self.clients[venue]
        sample = self.samples.setdefault((venue, symbol), Sample())
        try:
            book = await client.fetch_order_book(symbol, limit=DEPTH_LEVELS * 4)
            snapshot = snapshot_from_book(book, int(time.time() * 1000))
            if snapshot is not None:
                sample.snapshots.append(snapshot)
            trades = await client.fetch_trades(symbol, limit=100)
            add_prints(sample, trades)
        except Exception as error:  # noqa: BLE001, one venue's hiccup is not fatal
            logger.warning(f"{venue} {symbol}: {error}")

    async def collect(
        self, symbols: list[str], seconds: float, interval: float
    ) -> None:
        """
        Sample every symbol on every venue for a while.

        Each venue's requests run through its own client, whose rate limiter
        serialises them; the venues run side by side.

        Parameters
        ----------
        symbols : list[str]
            Symbols to sample.
        seconds : float
            How long to sample.
        interval : float
            Seconds between rounds, as a floor: a round that takes longer
            starts the next one at once.
        """
        deadline = time.monotonic() + seconds
        rounds = 0
        while True:
            started = time.monotonic()
            await asyncio.gather(
                *(
                    self.fetch_one(venue, symbol)
                    for venue in self.clients
                    for symbol in symbols
                )
            )
            rounds += 1
            logger.info(
                f"round {rounds} took {time.monotonic() - started:.1f}s, "
                f"{max(0.0, deadline - time.monotonic()):.0f}s left"
            )
            if time.monotonic() >= deadline:
                return
            await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))

    def scores(
        self, symbols: list[str], fees: dict[tuple[str, str], float]
    ) -> list[PairScore]:
        """
        Score every symbol in both maker and taker directions.

        Parameters
        ----------
        symbols : list[str]
            Symbols sampled.
        fees : dict[tuple[str, str], float]
            Fee budget in basis points per (maker, taker) venue pair.

        Returns
        -------
        list[PairScore]
            Ranked scores.
        """
        out: list[PairScore] = []
        venues = list(self.clients)
        for symbol in symbols:
            for maker in venues:
                for taker in venues:
                    if maker == taker:
                        continue
                    score = score_pair(
                        symbol,
                        maker,
                        taker,
                        self.samples.get((maker, symbol), Sample()),
                        self.samples.get((taker, symbol), Sample()),
                        fees[(maker, taker)],
                    )
                    if score is not None:
                        out.append(score)
        return rank(out)

    async def close(self) -> None:
        """Close every client."""
        await asyncio.gather(*(client.close() for client in self.clients.values()))


def recorded_events(directory: Path, start_ns: int, end_ns: int) -> list[Any]:
    """
    Read the events the recorder wrote for one stream inside a range.

    Parameters
    ----------
    directory : Path
        The stream's recording directory.
    start_ns : int
        Earliest receive time kept, inclusive.
    end_ns : int
        Latest receive time kept, exclusive.

    Returns
    -------
    list[Any]
        Decoded events in receive order; none if the directory is missing.
    """
    out: list[Any] = []
    if not directory.is_dir():
        return out
    for path in recording_files(directory):
        with open(path) as fh:
            for line in fh:
                try:
                    record = json.loads(line)
                    event = from_stream_fields(
                        {"type": record["type"], "data": json.dumps(record["data"])}
                    )
                except Exception:  # noqa: BLE001, a torn last line is normal
                    continue
                if start_ns <= event.ts_recv < end_ns:
                    out.append(event)
    out.sort(key=lambda e: e.ts_recv)
    return out


def sample_from_recording(
    root: Path, venue: str, symbol: str, start_ns: int, end_ns: int
) -> Sample:
    """
    Build a sample from a venue's recorded books and trades.

    Parameters
    ----------
    root : Path
        Recording root.
    venue : str
        Venue id.
    symbol : str
        CCXT symbol.
    start_ns : int
        Earliest receive time kept, inclusive.
    end_ns : int
        Latest receive time kept, exclusive.

    Returns
    -------
    Sample
        One snapshot per book update and one print per trade.
    """
    sample = Sample()
    for event in recorded_events(
        stream_path(root, book_stream(venue, symbol)), start_ns, end_ns
    ):
        if isinstance(event, BookEvent) and event.bids and event.asks:
            sample.snapshots.append(
                Snapshot(
                    ts_ms=event.ts_recv // 1_000_000,
                    bid=event.bids[0][0],
                    ask=event.asks[0][0],
                    bid_depth=sum(p * a for p, a in event.bids[:DEPTH_LEVELS]),
                    ask_depth=sum(p * a for p, a in event.asks[:DEPTH_LEVELS]),
                )
            )
    for event in recorded_events(
        stream_path(root, trade_stream(venue, symbol)), start_ns, end_ns
    ):
        if isinstance(event, TradeEvent):
            sample.prints.append(
                Print(
                    ts_ms=event.ts_recv // 1_000_000,
                    side=event.side.value if event.side is not None else "",
                    price=event.price,
                    amount=event.amount,
                )
            )
    return sample


def parse_when(value: str | None, default_ns: int) -> int:
    """
    Parse an ISO 8601 time into nanoseconds, UTC unless a zone is given.

    Parameters
    ----------
    value : str | None
        The time, or None for the default.
    default_ns : int
        Returned for None.

    Returns
    -------
    int
        Nanoseconds since the epoch.
    """
    if value is None:
        return default_ns
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1e9)


def score_recording(
    root: Path,
    venue_ids: list[str],
    symbols: list[str],
    fees: dict[tuple[str, str], float],
    start_ns: int,
    end_ns: int,
) -> list[PairScore]:
    """
    Score symbols from a recording, every venue as maker against every other.

    Parameters
    ----------
    root : Path
        Recording root.
    venue_ids : list[str]
        Venues to score.
    symbols : list[str]
        Symbols to score.
    fees : dict[tuple[str, str], float]
        Fee budget in basis points per (maker, taker) venue pair.
    start_ns : int
        Earliest receive time kept, inclusive.
    end_ns : int
        Latest receive time kept, exclusive.

    Returns
    -------
    list[PairScore]
        Ranked scores.
    """
    samples = {
        (venue, symbol): sample_from_recording(root, venue, symbol, start_ns, end_ns)
        for venue in venue_ids
        for symbol in symbols
    }
    out: list[PairScore] = []
    for symbol in symbols:
        for maker in venue_ids:
            for taker in venue_ids:
                if maker == taker:
                    continue
                score = score_pair(
                    symbol,
                    maker,
                    taker,
                    samples[(maker, symbol)],
                    samples[(taker, symbol)],
                    fees[(maker, taker)],
                )
                if score is not None:
                    out.append(score)
    return rank(out)


def public_clients(
    config: AppConfig, venue_ids: list[str], rate_limit_ms: int | None = None
) -> dict[str, Any]:
    """
    Build unauthenticated CCXT clients for a list of venues.

    Parameters
    ----------
    config : AppConfig
        The application configuration, for per-venue CCXT options.
    venue_ids : list[str]
        CCXT ids. A venue absent from the config gets default options.
    rate_limit_ms : int | None
        Milliseconds between requests per client, overriding CCXT's default
        for the venue. A 20 minute screen of 150 symbols drew 429s from one
        venue at its default; 100 ms or more is a safer floor there.

    Returns
    -------
    dict[str, Any]
        Clients per venue id.
    """
    configured = {venue.id: venue for venue in config.venues}
    clients: dict[str, Any] = {}
    for venue_id in venue_ids:
        params: dict[str, Any] = {"enableRateLimit": True}
        if rate_limit_ms is not None:
            params["rateLimit"] = rate_limit_ms
        venue = configured.get(venue_id)
        if venue is not None and venue.options:
            params["options"] = dict(venue.options)
        clients[venue_id] = getattr(ccxt, venue_id)(params)
    return clients


def select_symbols(
    symbols: list[str],
    volumes: dict[str, dict[str, float]],
    min_volume: float,
    top: int,
) -> list[str]:
    """
    Keep the symbols liquid enough on every venue, the busiest first.

    Parameters
    ----------
    symbols : list[str]
        Candidate symbols.
    volumes : dict[str, dict[str, float]]
        24 hour quote volume per venue and symbol.
    min_volume : float
        Least volume a symbol must have on its quietest venue.
    top : int
        How many to keep.

    Returns
    -------
    list[str]
        The selection, ordered by the quietest venue's volume descending.
    """
    floor: dict[str, float] = {}
    for symbol in symbols:
        per_venue = [volumes.get(v, {}).get(symbol, 0.0) for v in volumes]
        floor[symbol] = min(per_venue) if per_venue else 0.0
    kept = [s for s in symbols if floor[s] >= min_volume]
    kept.sort(key=lambda s: floor[s], reverse=True)
    return kept[:top]


async def run(args: argparse.Namespace) -> list[PairScore]:
    """
    Screen the configured or given venues and print the ranking.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command line.

    Returns
    -------
    list[PairScore]
        Ranked scores.
    """
    config = load_app_config()
    venue_ids = args.venues or [venue.id for venue in config.venues]
    if len(venue_ids) < 2:
        raise SystemExit("need at least two venues")
    configured = {venue.id: venue for venue in config.venues}
    fees = {
        (maker, taker): fee_budget_bps(
            configured.get(maker), configured.get(taker), args.fee_bps
        )
        for maker in venue_ids
        for taker in venue_ids
        if maker != taker
    }
    if args.recorded:
        if not args.symbols:
            raise SystemExit("--recorded needs --symbols")
        scores = score_recording(
            Path(args.recorded),
            venue_ids,
            args.symbols,
            fees,
            parse_when(args.start, 0),
            parse_when(args.end, 2**63 - 1),
        )
        print(format_table(scores, args.rows))
        if args.json:
            with open(args.json, "w") as fh:
                json.dump([asdict(s) for s in scores], fh, indent=1)
        return scores
    screener = Screener(public_clients(config, venue_ids, args.rate_limit_ms))
    try:
        markets = await screener.load_markets()
        symbols = common_symbols(markets, args.quote)
        logger.info(f"{len(symbols)} {args.quote} spot symbols in common")
        if args.symbols:
            symbols = [s for s in args.symbols if s in symbols]
        else:
            volumes = await screener.volumes(symbols)
            symbols = select_symbols(symbols, volumes, args.min_volume, args.top)
        logger.info(f"sampling {len(symbols)} symbols for {args.minutes} min")
        await screener.collect(symbols, args.minutes * 60, args.interval)
        scores = screener.scores(symbols, fees)
    finally:
        await screener.close()
    print(format_table(scores, args.rows))
    if args.json:
        with open(args.json, "w") as fh:
            json.dump([asdict(s) for s in scores], fh, indent=1)
        print(f"wrote {args.json}")
    return scores


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
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--venues", nargs="*", help="CCXT ids; default: config")
    parser.add_argument("--quote", default="USDT")
    parser.add_argument("--symbols", nargs="*", help="sample only these symbols")
    parser.add_argument("--top", type=int, default=30, help="symbols to sample")
    parser.add_argument("--min-volume", type=float, default=10_000.0)
    parser.add_argument("--minutes", type=float, default=10.0)
    parser.add_argument("--interval", type=float, default=6.0)
    parser.add_argument("--fee-bps", type=float, default=DEFAULT_FEE_BPS)
    parser.add_argument("--rows", type=int, default=40, help="rows to print")
    parser.add_argument("--rate-limit-ms", type=int, help="per-client request gap")
    parser.add_argument("--json", help="write every score here")
    parser.add_argument("--recorded", help="score this recording root instead")
    parser.add_argument("--start", help="--recorded: ISO 8601, inclusive")
    parser.add_argument("--end", help="--recorded: ISO 8601, exclusive")
    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    asyncio.run(run(parse_args(sys.argv[1:])))
