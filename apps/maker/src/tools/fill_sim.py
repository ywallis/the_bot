"""Simulate maker fills and their hedges over a recording.

The maker strategies rest a quote on one venue, priced off the other venue's
touch, and hedge on the other venue when the quote fills. This tool replays
the recorder's books and trades for one symbol on the two venues and asks
what such a quote would have earned, per fill and per hour, at each edge.

Usage
-----
    uv run -m apps.maker.src.tools.fill_sim BASE/QUOTE venue_a venue_b \
        --start 2026-09-20T14 --end 2026-09-21T02 --edges 20,30,40,60

The first venue is the maker venue, the second the taker venue. Fees default
to the static ``maker_fee`` of the maker venue and ``taker_fee`` of the taker
venue in ``config.toml``. ``--maker-fee-bps`` and ``--taker-fee-bps``
override them, and are required when a venue configures no static rate.

The model
---------
For each edge and each side a quote sits ``edge`` past the taker venue's
touch: a sell at taker ask times ``1 + edge``, a buy at taker bid times
``1 - edge``. It is priced from the taker book as it was ``--qlat`` seconds
earlier, which stands for the staleness of a live quote. Like the live
strategy, it is only placed when it sits behind the maker venue's own best,
a sell above the maker best ask and a buy below the maker best bid.
``--inside`` drops that rule.

A quote fills only on a maker venue print strictly through its price, at
most once per ``--cool`` seconds per side, for the smaller of ``--size``
quote units at the quote price and the print amount. The hedge executes
``--hlat`` seconds after the fill against the taker book of that moment and
walks its depth. A fill whose hedge the book cannot absorb is counted as
thin and left out of the statistics. The result of a fill is in basis points
of the fill price, net of the maker fee on the fill and the taker fee on the
hedge. A book whose best bid is at or above its best ask is ignored: it is a
stale level, and real recordings contain hours of them.

Limits
------
There is no queue: a print strictly through the quote fills it, a print at
the quote never does, and nothing rests in front of it. One print fills a
quote at most once, whatever its size. Both latencies are constants, not the
measured distribution, so a tail of slow hedges is not represented. The
quote does not move the market, and the other edges and sides do not
compete with it for the same print.

When to use it
--------------
``market_screener --recorded`` ranks markets by the volume that printed past
the taker touch, an upper bound with no latency and no hedge. Use it to
choose which markets to look at. Use this tool on a shortlisted market to
choose the edge: it prices the hedge from real depth after a real delay, so
adverse selection shows up as a mean below the edge. ``--hourly`` shows
whether a result holds across hours or comes from one burst.
"""

import argparse
import heapq
import statistics
import sys
from collections import defaultdict, deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import msgspec
from compression import zstd

from apps.maker.src.tools.market_screener import parse_when
from apps.shared.src.config import VenueConfig, load_app_config
from apps.shared.src.events import (
    BookEvent,
    Level,
    TradeEvent,
    book_stream,
    trade_stream,
)
from apps.shared.src.streams import (
    COMPRESSED_SUFFIX,
    bucket_of_file,
    recording_files,
    stream_path,
)

NS = 1_000_000_000
HOUR_NS = 3600 * NS
BPS = 1e4

# Sides of a resting quote, in output order.
SIDES: tuple[str, ...] = ("sell", "buy")


class _Line(msgspec.Struct):
    """One recorder line, of which only the event is read."""

    data: BookEvent | TradeEvent


_line_decoder = msgspec.json.Decoder(_Line)


@dataclass(frozen=True)
class Fill:
    """
    One simulated maker fill and its hedge.

    Attributes
    ----------
    ts_ns : int
        Receive time of the print that filled the quote, nanoseconds.
    edge_bps : float
        Edge of the quote past the taker touch, basis points.
    side : str
        Side of the maker quote, ``sell`` or ``buy``.
    price : float
        Quote price, at which the fill executes.
    base : float
        Filled amount in base units.
    hedge_price : float | None
        Average hedge price, or None if the taker book was too thin.
    bps : float | None
        Result net of both fees in basis points of ``price``, or None if
        the hedge could not execute.
    """

    ts_ns: int
    edge_bps: float
    side: str
    price: float
    base: float
    hedge_price: float | None
    bps: float | None


@dataclass(frozen=True)
class Params:
    """
    Parameters of one simulation.

    Attributes
    ----------
    edges_bps : tuple[float, ...]
        Edges past the taker touch, basis points.
    maker_fee : float
        Maker fee rate on the fill, as a fraction.
    taker_fee : float
        Taker fee rate on the hedge, as a fraction.
    qlat_s : float
        Age of the taker book a quote is priced from, seconds.
    hlat_s : float
        Delay from fill to hedge, seconds.
    size : float
        Quote size in quote units.
    cool_s : float
        Minimum time between two fills of one side at one edge, seconds.
    inside : bool
        Also quote when the price is inside the maker venue's best.
    """

    edges_bps: tuple[float, ...]
    maker_fee: float
    taker_fee: float
    qlat_s: float = 0.3
    hlat_s: float = 0.6
    size: float = 13.0
    cool_s: float = 1.0
    inside: bool = False


@dataclass
class Result:
    """
    Every fill of a simulation and the time span it covered.

    Attributes
    ----------
    fills : list[Fill]
        Fills in settlement order.
    first_ns : int | None
        Receive time of the first event read.
    last_ns : int | None
        Receive time of the last event read.
    """

    fills: list[Fill]
    first_ns: int | None = None
    last_ns: int | None = None

    @property
    def hours(self) -> float:
        """
        Return the time covered by the events read, in hours.

        Returns
        -------
        float
            Hours from the first to the last event, zero if none were read.
        """
        if self.first_ns is None or self.last_ns is None:
            return 0.0
        return (self.last_ns - self.first_ns) / HOUR_NS


# Pure arithmetic ------------------------------------------------------------


def walk_depth(levels: list[Level], amount: float) -> float | None:
    """
    Return the average price paid to take an amount from book levels.

    Parameters
    ----------
    levels : list[Level]
        Levels of one book side, best first, as ``(price, size)``.
    amount : float
        Base amount to take.

    Returns
    -------
    float | None
        Volume-weighted average price, or None if the levels hold less
        than ``amount``.
    """
    got = 0.0
    cost = 0.0
    for price, size in levels:
        take = min(size, amount - got)
        got += take
        cost += take * price
        if got >= amount - 1e-12:
            return cost / got
    return None


def book_valid(bids: list[Level], asks: list[Level]) -> bool:
    """
    Tell whether a book has both sides and its best bid below its best ask.

    Parameters
    ----------
    bids : list[Level]
        Bid levels, best first.
    asks : list[Level]
        Ask levels, best first.

    Returns
    -------
    bool
        False for an empty side or a book crossed or locked on itself.
    """
    return bool(bids) and bool(asks) and bids[0][0] < asks[0][0]


def quote_side_hit(aggressor: str) -> str:
    """
    Return the side of the resting quote a print can fill.

    Parameters
    ----------
    aggressor : str
        Aggressor side of the print, ``buy`` or ``sell``.

    Returns
    -------
    str
        ``sell`` for an aggressive buy, ``buy`` for an aggressive sell.
    """
    return "sell" if aggressor == "buy" else "buy"


def quote_price(side: str, taker_bid: float, taker_ask: float, edge: float) -> float:
    """
    Return the price of a quote an edge past the taker venue's touch.

    Parameters
    ----------
    side : str
        Side of the quote, ``sell`` or ``buy``.
    taker_bid : float
        Taker venue best bid.
    taker_ask : float
        Taker venue best ask.
    edge : float
        Edge as a fraction.

    Returns
    -------
    float
        ``taker_ask * (1 + edge)`` for a sell, ``taker_bid * (1 - edge)``
        for a buy.
    """
    if side == "sell":
        return taker_ask * (1 + edge)
    return taker_bid * (1 - edge)


def fill_decision(
    side: str,
    quote: float,
    print_price: float,
    maker_bid: float,
    maker_ask: float,
    inside: bool,
    since_last_s: float,
    cool_s: float,
) -> bool:
    """
    Decide whether a print fills a resting quote.

    Parameters
    ----------
    side : str
        Side of the quote, ``sell`` or ``buy``.
    quote : float
        Quote price.
    print_price : float
        Price of the maker venue print on the opposite side.
    maker_bid : float
        Maker venue best bid.
    maker_ask : float
        Maker venue best ask.
    inside : bool
        Whether a quote inside the maker venue's best is placed at all.
    since_last_s : float
        Seconds since this side at this edge last filled.
    cool_s : float
        Minimum seconds between two fills.

    Returns
    -------
    bool
        True if the quote is placed, the print goes strictly through it and
        the side is not cooling down.
    """
    if since_last_s < cool_s:
        return False
    if side == "sell":
        placed = inside or quote > maker_ask
        return placed and print_price > quote
    placed = inside or quote < maker_bid
    return placed and print_price < quote


def fill_base(size: float, quote: float, print_amount: float) -> float:
    """
    Return the base amount a fill takes.

    Parameters
    ----------
    size : float
        Quote size in quote units.
    quote : float
        Quote price.
    print_amount : float
        Base amount of the print.

    Returns
    -------
    float
        The smaller of the quote size in base and the print amount.
    """
    return min(size / quote, print_amount)


def pnl_bps(
    side: str,
    fill_price: float,
    hedge_price: float,
    maker_fee: float,
    taker_fee: float,
) -> float:
    """
    Return the result of a hedged fill in basis points of the fill price.

    Parameters
    ----------
    side : str
        Side of the maker fill, ``sell`` or ``buy``. The hedge is the other.
    fill_price : float
        Maker fill price.
    hedge_price : float
        Average taker hedge price.
    maker_fee : float
        Maker fee rate on the fill, as a fraction.
    taker_fee : float
        Taker fee rate on the hedge, as a fraction.

    Returns
    -------
    float
        Net result per unit of base, in basis points of ``fill_price``.
    """
    if side == "sell":
        pnl = fill_price * (1 - maker_fee) - hedge_price * (1 + taker_fee)
    else:
        pnl = hedge_price * (1 - taker_fee) - fill_price * (1 + maker_fee)
    return pnl / fill_price * BPS


# Reading --------------------------------------------------------------------


def bucket_bounds(bucket: str) -> tuple[int, int] | None:
    """
    Return the time span a recording bucket covers.

    Parameters
    ----------
    bucket : str
        ``YYYY-MM-DDTHH`` for an hourly file or ``YYYY-MM-DD`` for a daily
        one.

    Returns
    -------
    tuple[int, int] | None
        Start inclusive and end exclusive in nanoseconds, or None if the
        name is neither form.
    """
    for fmt, span in (("%Y-%m-%dT%H", timedelta(hours=1)), ("%Y-%m-%d", None)):
        try:
            start = datetime.strptime(bucket, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
        end = start + (span or timedelta(days=1))
        return int(start.timestamp()) * NS, int(end.timestamp()) * NS
    return None


def stream_events(
    directory: Path, start_ns: int, end_ns: int
) -> Iterator[BookEvent | TradeEvent]:
    """
    Yield the recorded book and trade events of one stream, lazily.

    Files whose bucket lies outside the range are not opened. A line that
    does not decode, such as a torn last line, is skipped.

    Parameters
    ----------
    directory : Path
        The stream's recording directory.
    start_ns : int
        Earliest receive time kept, inclusive.
    end_ns : int
        Latest receive time kept, exclusive.

    Yields
    ------
    BookEvent | TradeEvent
        Events in file order, which is receive order.
    """
    for path in recording_files(directory):
        bucket = bucket_of_file(path)
        bounds = bucket_bounds(bucket) if bucket else None
        if bounds is not None and (bounds[1] <= start_ns or bounds[0] >= end_ns):
            continue
        opener = zstd.open if path.name.endswith(COMPRESSED_SUFFIX) else open
        with opener(path, "rb") as fh:
            for line in fh:
                try:
                    event = _line_decoder.decode(line).data
                except msgspec.DecodeError, msgspec.ValidationError:
                    continue
                if start_ns <= event.ts_recv < end_ns:
                    yield event


def merged_events(
    root: Path, symbol: str, maker: str, taker: str, start_ns: int, end_ns: int
) -> Iterator[BookEvent | TradeEvent]:
    """
    Merge the books and trades of both venues into one receive-time stream.

    Parameters
    ----------
    root : Path
        Recording root.
    symbol : str
        CCXT symbol.
    maker : str
        Maker venue id.
    taker : str
        Taker venue id.
    start_ns : int
        Earliest receive time kept, inclusive.
    end_ns : int
        Latest receive time kept, exclusive.

    Returns
    -------
    Iterator[BookEvent | TradeEvent]
        Events ordered by receive time, books before trades on a tie.
    """
    streams = [
        stream_events(stream_path(root, name(venue, symbol)), start_ns, end_ns)
        for name in (book_stream, trade_stream)
        for venue in (maker, taker)
    ]
    return heapq.merge(*streams, key=lambda event: event.ts_recv)


# Simulation -----------------------------------------------------------------


def simulate(
    events: Iterable[BookEvent | TradeEvent], maker: str, taker: str, params: Params
) -> Result:
    """
    Replay books and trades and record every fill and its hedge.

    Parameters
    ----------
    events : Iterable[BookEvent | TradeEvent]
        Both venues' books and trades in receive order.
    maker : str
        Venue the quotes rest on.
    taker : str
        Venue the hedges execute on.
    params : Params
        Simulation parameters.

    Returns
    -------
    Result
        The fills, in the order their hedges settled, and the span read.
    """
    qlat_ns = int(params.qlat_s * NS)
    hlat_ns = int(params.hlat_s * NS)
    cool_ns = params.cool_s * NS
    # Taker books from the newest one at least qlat old up to now. Prints come
    # in time order, so a book older than that is never looked up again.
    taker_books: deque[BookEvent] = deque()
    maker_book: BookEvent | None = None
    # Hedges due: (time due, edge bps, side, fill price, base, print time).
    pending: list[tuple[int, float, str, float, float, int]] = []
    last_fill: dict[tuple[float, str], int] = {}
    result = Result(fills=[])
    for event in events:
        now = event.ts_recv
        if result.first_ns is None:
            result.first_ns = now
        result.last_ns = now
        while pending and pending[0][0] <= now:
            _, edge_bps, side, price, base, ts_fill = heapq.heappop(pending)
            book = taker_books[-1]
            levels = book.asks if side == "sell" else book.bids
            hedge = walk_depth(levels, base)
            bps = (
                None
                if hedge is None
                else pnl_bps(side, price, hedge, params.maker_fee, params.taker_fee)
            )
            result.fills.append(Fill(ts_fill, edge_bps, side, price, base, hedge, bps))
        while len(taker_books) > 1 and taker_books[1].ts_recv <= now - qlat_ns:
            taker_books.popleft()
        if isinstance(event, BookEvent):
            if not book_valid(event.bids, event.asks):
                continue
            if event.venue == taker:
                taker_books.append(event)
            elif event.venue == maker:
                maker_book = event
            continue
        if event.venue != maker or event.side is None or maker_book is None:
            continue
        if not taker_books or taker_books[0].ts_recv > now - qlat_ns:
            continue
        priced_from = taker_books[0]
        side = quote_side_hit(event.side.value)
        for edge_bps in params.edges_bps:
            quote = quote_price(
                side,
                priced_from.bids[0][0],
                priced_from.asks[0][0],
                edge_bps / BPS,
            )
            key = (edge_bps, side)
            since = now - last_fill.get(key, -(2**62))
            if not fill_decision(
                side,
                quote,
                event.price,
                maker_book.bids[0][0],
                maker_book.asks[0][0],
                params.inside,
                since / NS,
                cool_ns / NS,
            ):
                continue
            last_fill[key] = now
            base = fill_base(params.size, quote, event.amount)
            heapq.heappush(pending, (now + hlat_ns, edge_bps, side, quote, base, now))
    return result


# Reporting ------------------------------------------------------------------


@dataclass(frozen=True)
class Summary:
    """
    Statistics of the hedged fills of one edge and side.

    Attributes
    ----------
    fills : int
        Hedged fills.
    thin : int
        Fills whose hedge the taker book could not absorb.
    fills_per_h : float
        Hedged fills per hour.
    median_bps : float
        Median result.
    mean_bps : float
        Mean result.
    p10_bps : float
        Tenth percentile result.
    win_pct : float
        Share of fills with a positive result, percent.
    quote_per_h : float
        Mean result times fills per hour times the quote size, in quote
        units per hour, as if every fill took the full size.
    """

    fills: int
    thin: int
    fills_per_h: float
    median_bps: float
    mean_bps: float
    p10_bps: float
    win_pct: float
    quote_per_h: float


def summarize(fills: list[Fill], hours: float, size: float) -> Summary | None:
    """
    Summarize the fills of one edge and side.

    Parameters
    ----------
    fills : list[Fill]
        Fills of one edge and side, hedged or not.
    hours : float
        Time covered, hours.
    size : float
        Quote size in quote units.

    Returns
    -------
    Summary | None
        Statistics, or None if no fill was hedged.
    """
    results = sorted(f.bps for f in fills if f.bps is not None)
    if not results:
        return None
    mean = statistics.fmean(results)
    per_h = len(results) / hours if hours > 0 else 0.0
    return Summary(
        fills=len(results),
        thin=len(fills) - len(results),
        fills_per_h=per_h,
        median_bps=statistics.median(results),
        mean_bps=mean,
        p10_bps=results[len(results) // 10],
        win_pct=100 * sum(r > 0 for r in results) / len(results),
        quote_per_h=per_h * mean / BPS * size,
    )


def group(fills: list[Fill]) -> dict[tuple[float, str], list[Fill]]:
    """
    Group fills by edge and side.

    Parameters
    ----------
    fills : list[Fill]
        Any fills.

    Returns
    -------
    dict[tuple[float, str], list[Fill]]
        Fills per ``(edge_bps, side)``.
    """
    out: dict[tuple[float, str], list[Fill]] = defaultdict(list)
    for fill in fills:
        out[(fill.edge_bps, fill.side)].append(fill)
    return out


def format_table(result: Result, params: Params) -> str:
    """
    Render the per edge and side summary.

    Parameters
    ----------
    result : Result
        Simulation result.
    params : Params
        Simulation parameters.

    Returns
    -------
    str
        One line per edge and side.
    """
    grouped = group(result.fills)
    lines = [
        "edge side  fills/h  med_bps  mean_bps  p10_bps  win%  quote/h  thin",
    ]
    for edge in params.edges_bps:
        for side in SIDES:
            s = summarize(grouped.get((edge, side), []), result.hours, params.size)
            if s is None:
                lines.append(f"{edge:4.0f} {side:4}      0.0")
                continue
            lines.append(
                f"{edge:4.0f} {side:4} {s.fills_per_h:8.1f} {s.median_bps:8.1f} "
                f"{s.mean_bps:9.1f} {s.p10_bps:8.1f} {s.win_pct:5.0f} "
                f"{s.quote_per_h:8.3f} {s.thin:5d}"
            )
    return "\n".join(lines)


def format_hourly(result: Result, params: Params) -> str:
    """
    Render mean result and fill count per UTC hour, edge and side.

    Parameters
    ----------
    result : Result
        Simulation result.
    params : Params
        Simulation parameters.

    Returns
    -------
    str
        One line per hour with ``mean/fills`` per edge and side; a dash
        where nothing was hedged.
    """
    keys = [(edge, side) for edge in params.edges_bps for side in SIDES]
    per_hour: dict[int, dict[tuple[float, str], list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for fill in result.fills:
        if fill.bps is not None:
            hour = fill.ts_ns // HOUR_NS
            per_hour[hour][(fill.edge_bps, fill.side)].append(fill.bps)
    header = "hour (UTC)    " + " ".join(f"{e:>5.0f}{s:>5}" for e, s in keys)
    lines = [header]
    for hour in sorted(per_hour):
        label = datetime.fromtimestamp(hour * 3600, UTC).strftime("%Y-%m-%dT%H")
        cells = []
        for key in keys:
            values = per_hour[hour].get(key)
            cells.append(
                f"{statistics.fmean(values):>5.0f}/{len(values):<4d}"
                if values
                else f"{'-':>10}"
            )
        lines.append(f"{label:13} " + " ".join(cells))
    return "\n".join(lines)


# Entry point ----------------------------------------------------------------


def resolve_fees(
    venues: Iterable[VenueConfig],
    maker: str,
    taker: str,
    maker_fee_bps: float | None,
    taker_fee_bps: float | None,
) -> tuple[float, float]:
    """
    Return the maker fee of the maker venue and the taker fee of the taker.

    Parameters
    ----------
    venues : Iterable[VenueConfig]
        Configured venues, read for their static rates.
    maker : str
        Maker venue id.
    taker : str
        Taker venue id.
    maker_fee_bps : float | None
        Override of the maker fee, basis points.
    taker_fee_bps : float | None
        Override of the taker fee, basis points.

    Returns
    -------
    tuple[float, float]
        Maker and taker fee rates as fractions.

    Raises
    ------
    ValueError
        If a rate has neither an override nor a static rate in config.
    """
    by_id = {venue.id: venue for venue in venues}
    maker_cfg = by_id.get(maker)
    taker_cfg = by_id.get(taker)
    maker_fee = (
        maker_fee_bps / BPS
        if maker_fee_bps is not None
        else (maker_cfg.maker_fee if maker_cfg else None)
    )
    taker_fee = (
        taker_fee_bps / BPS
        if taker_fee_bps is not None
        else (taker_cfg.taker_fee if taker_cfg else None)
    )
    if maker_fee is None:
        raise ValueError(f"No static maker fee for {maker}: pass --maker-fee-bps")
    if taker_fee is None:
        raise ValueError(f"No static taker fee for {taker}: pass --taker-fee-bps")
    return maker_fee, taker_fee


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
    parser.add_argument("symbol", help="CCXT symbol")
    parser.add_argument("maker", help="venue the quotes rest on")
    parser.add_argument("taker", help="venue the hedges execute on")
    parser.add_argument("--root", default="data", help="recording root")
    parser.add_argument("--start", help="ISO 8601, inclusive")
    parser.add_argument("--end", help="ISO 8601, exclusive")
    parser.add_argument("--edges", default="20,30,40,60", help="bps, comma separated")
    parser.add_argument("--qlat", type=float, default=0.3, help="quote age, s")
    parser.add_argument("--hlat", type=float, default=0.6, help="fill to hedge, s")
    parser.add_argument("--size", type=float, default=13.0, help="quote units")
    parser.add_argument("--cool", type=float, default=1.0, help="s between fills")
    parser.add_argument(
        "--inside", action="store_true", help="also quote inside the maker best"
    )
    parser.add_argument("--hourly", action="store_true", help="add a per-hour table")
    parser.add_argument("--maker-fee-bps", type=float)
    parser.add_argument("--taker-fee-bps", type=float)
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    """
    Run the simulation and print its tables.

    Parameters
    ----------
    argv : list[str]
        Arguments without the program name.
    """
    args = parse_args(argv)
    try:
        maker_fee, taker_fee = resolve_fees(
            load_app_config().venues,
            args.maker,
            args.taker,
            args.maker_fee_bps,
            args.taker_fee_bps,
        )
    except ValueError as error:
        sys.exit(str(error))
    params = Params(
        edges_bps=tuple(float(x) for x in args.edges.split(",")),
        maker_fee=maker_fee,
        taker_fee=taker_fee,
        qlat_s=args.qlat,
        hlat_s=args.hlat,
        size=args.size,
        cool_s=args.cool,
        inside=args.inside,
    )
    events = merged_events(
        Path(args.root),
        args.symbol,
        args.maker,
        args.taker,
        parse_when(args.start, 0),
        parse_when(args.end, 2**63 - 1),
    )
    result = simulate(events, args.maker, args.taker, params)
    print(
        f"{args.symbol} maker={args.maker} taker={args.taker} "
        f"hours={result.hours:.1f} qlat={params.qlat_s} hlat={params.hlat_s} "
        f"maker_fee={maker_fee * BPS:g}bps taker_fee={taker_fee * BPS:g}bps"
        f"{' inside' if params.inside else ''}"
    )
    print(format_table(result, params))
    if args.hourly:
        print()
        print(format_hourly(result, params))


if __name__ == "__main__":
    main(sys.argv[1:])
