"""Realized profit and loss per hedged fill, from the recorded order events.

The maker strategies make money only if a fill on the maker venue and its
hedge on the taker venue, net of both fees, leave more quote currency than
they started with. Nothing in the system said whether that was true: the
accountant reports balances, the backtest is an upper bound by its own
account, and the one calibrated afternoon was reconstructed by hand. This
reads ``oms:events`` from the recorder's files, pairs every maker fill with
the hedge that carries the same intent id on the other venue, and values
each pair in quote units, with the residual base position marked at the
hedge price so an unhedged or over-hedged fill shows up as what it is.

Usage
-----
    uv run -m apps.maker.src.tools.pnl
    uv run -m apps.maker.src.tools.pnl --root data --start 2026-09-08T12:00Z
    uv run -m apps.maker.src.tools.pnl --maker-fee-bps 0 --taker-fee-bps 10 --json pnl.json

Fees: a fill that reports its own fee is charged that. Otherwise the rate
comes from the command line, then from the venue's static ``maker_fee`` and
``taker_fee`` in config, then from the last fee schedule the recording holds
for the venue, in that order. A venue with none of those is charged nothing
and the report says so.
"""

import argparse
import json
import statistics
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from apps.maker.src.order_watcher import STRATEGY_TAG
from apps.shared.src.config import AppConfig, VenueConfig, load_app_config
from apps.shared.src.events import (
    ORDER_EVENTS_STREAM,
    FeeScheduleEvent,
    OrderEvent,
    Side,
    fees_stream,
    from_stream_fields,
)
from apps.shared.src.fees import base_quote, fee_in_base, schedule_for
from apps.shared.src.streams import recording_files, stream_path

NS_PER_MS = 1_000_000


@dataclass
class Leg:
    """
    What one order ended up doing, folded from every event about it.

    Attributes
    ----------
    venue : str
        Venue the order was on.
    intent_id : str
        Client order id shared by a maker order and its hedge.
    symbol : str
        CCXT symbol.
    side : Side
        Buy or sell.
    strategy_id : str
        Strategy identifier from the order's tags.
    filled : Decimal
        Largest cumulative fill reported.
    avg_price : Decimal | None
        Average price reported with that fill.
    fee : Decimal
        Fees the fills reported, summed, in ``fee_currency``.
    fee_currency : str | None
        Currency of the reported fees, None when nothing was reported.
    first_fill_ns : int
        Receive time of the first event that carried a fill.
    hedge_of : str | None
        Intent id of the order this leg hedges, from the matcher's tag, or
        None for an order a strategy placed.
    """

    venue: str
    intent_id: str
    symbol: str
    side: Side
    strategy_id: str
    filled: Decimal = Decimal(0)
    avg_price: Decimal | None = None
    fee: Decimal = Decimal(0)
    fee_currency: str | None = None
    first_fill_ns: int = 0
    hedge_of: str | None = None
    _fills_seen: set[Decimal] = field(default_factory=set, repr=False)

    def absorb(self, event: OrderEvent) -> None:
        """
        Fold one event about this order in.

        Parameters
        ----------
        event : OrderEvent
            The event. Only those that carry a fill change anything.
        """
        if event.filled <= 0:
            return
        if self.first_fill_ns == 0 or event.ts_recv < self.first_fill_ns:
            self.first_fill_ns = event.ts_recv
        if event.filled >= self.filled:
            self.filled = event.filled
            if event.avg_price is not None:
                self.avg_price = event.avg_price
        fill = event.last_fill
        # The order manager and the order watcher both report the same
        # transition; the cumulative size tells the two apart from a second
        # real fill.
        if (
            fill is not None
            and fill.fee is not None
            and event.filled not in self._fills_seen
        ):
            self._fills_seen.add(event.filled)
            self.fee += fill.fee
            self.fee_currency = fill.fee_currency or self.fee_currency


@dataclass(frozen=True)
class FeeRate:
    """
    The fee a venue charges a leg when the fill did not report one.

    Attributes
    ----------
    rate : Decimal | None
        Fraction of traded value, None when unknown.
    in_base : bool
        Whether the venue takes it from the base asset.
    source : str
        Where the rate came from, for the report.
    """

    rate: Decimal | None
    in_base: bool
    source: str


@dataclass(frozen=True)
class Pair:
    """
    A maker fill valued together with its hedge.

    Attributes
    ----------
    intent_id : str
        The shared client order id.
    strategy_id : str
        Strategy identifier.
    symbol : str
        CCXT symbol.
    ts_ns : int
        Receive time of the maker fill.
    maker_venue : str
        Where the fill happened.
    maker_side : str
        Side of the maker order.
    maker_filled : float
        Base filled on the maker venue.
    maker_price : float
        Average maker fill price.
    hedge_venue : str | None
        Where the hedge happened, None if no hedge was found.
    hedge_filled : float
        Base filled by the hedge.
    hedge_price : float | None
        Average hedge price.
    gross_bps : float | None
        Hedge price against maker price, in the direction that pays, before
        fees. None without a hedge.
    fees_quote : float
        Both legs' fees in quote units, base fees valued at the fill price.
    base_residual : float
        Base left over after both legs, positive when long.
    pnl_quote : float
        Cash difference plus the residual marked at the hedge price, or at
        the maker price when there is no hedge.
    """

    intent_id: str
    strategy_id: str
    symbol: str
    ts_ns: int
    maker_venue: str
    maker_side: str
    maker_filled: float
    maker_price: float
    hedge_venue: str | None
    hedge_filled: float
    hedge_price: float | None
    gross_bps: float | None
    fees_quote: float
    base_residual: float
    pnl_quote: float


def read_events(directory: Path, start_ns: int, end_ns: int) -> list[OrderEvent]:
    """
    Read every order event the recorder wrote inside a time range.

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
    list[OrderEvent]
        Events in receive order.
    """
    out: list[OrderEvent] = []
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
                if isinstance(event, OrderEvent) and start_ns <= event.ts_recv < end_ns:
                    out.append(event)
    out.sort(key=lambda e: e.ts_recv)
    return out


HEDGE_OF_TAG = "hedge_of"


def fold_legs(events: list[OrderEvent]) -> dict[tuple[str, str], Leg]:
    """
    Reduce order events to one leg per venue and client order id.

    Parameters
    ----------
    events : list[OrderEvent]
        Order events in any order.

    Returns
    -------
    dict[tuple[str, str], Leg]
        Legs that filled at all, keyed by venue and intent id. A leg placed
        by the matcher knows which order it hedges, from the ``hedge_of``
        tag the order manager copies onto its events.
    """
    legs: dict[tuple[str, str], Leg] = {}
    for event in events:
        if event.side is None:
            continue
        key = (event.venue, event.intent_id)
        leg = legs.get(key)
        if leg is None:
            leg = Leg(
                venue=event.venue,
                intent_id=event.intent_id,
                symbol=event.symbol,
                side=event.side,
                strategy_id=event.tags.get(STRATEGY_TAG, event.strategy.split("_")[0]),
            )
            legs[key] = leg
        if HEDGE_OF_TAG in event.tags:
            leg.hedge_of = event.tags[HEDGE_OF_TAG]
        leg.absorb(event)
    return {key: leg for key, leg in legs.items() if leg.filled > 0}


def merge_legs(legs: list[Leg]) -> Leg:
    """
    Combine several hedge legs of one order into one.

    Parameters
    ----------
    legs : list[Leg]
        Legs on one venue, same side, hedging the same order.

    Returns
    -------
    Leg
        One leg with the sizes and fees summed and the price averaged by
        size. A single leg is returned as it is.
    """
    if len(legs) == 1:
        return legs[0]
    first = legs[0]
    filled = sum((leg.filled for leg in legs), Decimal(0))
    priced = [leg for leg in legs if leg.avg_price is not None]
    avg = (
        sum((leg.filled * (leg.avg_price or 0) for leg in priced), Decimal(0))
        / sum((leg.filled for leg in priced), Decimal(0))
        if priced and sum((leg.filled for leg in priced), Decimal(0)) > 0
        else None
    )
    return Leg(
        venue=first.venue,
        intent_id=first.intent_id,
        symbol=first.symbol,
        side=first.side,
        strategy_id=first.strategy_id,
        filled=filled,
        avg_price=avg,
        fee=sum((leg.fee for leg in legs), Decimal(0)),
        fee_currency=next((leg.fee_currency for leg in legs if leg.fee_currency), None),
        first_fill_ns=min(leg.first_fill_ns for leg in legs),
        hedge_of=first.hedge_of,
    )


def fee_quote(leg: Leg, rate: FeeRate, price: Decimal) -> tuple[Decimal, Decimal]:
    """
    Work out what a leg paid in fees, split by the asset it came out of.

    Parameters
    ----------
    leg : Leg
        The leg.
    rate : FeeRate
        The rate to apply when the leg reported no fee.
    price : Decimal
        Price to value a base fee at.

    Returns
    -------
    tuple[Decimal, Decimal]
        Fee taken from the quote balance, fee taken from the base balance.
    """
    base, quote = base_quote(leg.symbol)
    if leg.fee_currency is not None:
        if leg.fee_currency == base:
            return Decimal(0), leg.fee
        if leg.fee_currency == quote:
            return leg.fee, Decimal(0)
        # A venue token: already outside both balances, valued as quote.
        return leg.fee, Decimal(0)
    if rate.rate is None:
        return Decimal(0), Decimal(0)
    if rate.in_base:
        return Decimal(0), leg.filled * rate.rate
    return leg.filled * price * rate.rate, Decimal(0)


def value_pair(
    maker: Leg, hedge: Leg | None, maker_rate: FeeRate, hedge_rate: FeeRate
) -> Pair:
    """
    Value a maker fill and its hedge in quote units.

    Parameters
    ----------
    maker : Leg
        The maker fill.
    hedge : Leg | None
        The hedge, if one was found.
    maker_rate : FeeRate
        Fee rate for the maker leg when it reported none.
    hedge_rate : FeeRate
        Fee rate for the hedge leg when it reported none.

    Returns
    -------
    Pair
        The valued pair.
    """
    maker_price = maker.avg_price or Decimal(0)
    sign = Decimal(1) if maker.side is Side.SELL else Decimal(-1)
    cash = sign * maker.filled * maker_price
    base = -sign * maker.filled
    fee_q, fee_b = fee_quote(maker, maker_rate, maker_price)
    cash -= fee_q
    base -= fee_b
    fees = fee_q + fee_b * maker_price

    hedge_price: Decimal | None = None
    gross: float | None = None
    if hedge is not None and hedge.avg_price is not None:
        hedge_price = hedge.avg_price
        hedge_sign = Decimal(1) if hedge.side is Side.SELL else Decimal(-1)
        cash += hedge_sign * hedge.filled * hedge_price
        base -= hedge_sign * hedge.filled
        fee_q, fee_b = fee_quote(hedge, hedge_rate, hedge_price)
        cash -= fee_q
        base -= fee_b
        fees += fee_q + fee_b * hedge_price
        if hedge_price > 0 and maker_price > 0:
            ratio = (
                maker_price / hedge_price
                if maker.side is Side.SELL
                else hedge_price / maker_price
            )
            gross = float((ratio - 1) * 10_000)

    mark = hedge_price if hedge_price is not None else maker_price
    return Pair(
        intent_id=maker.intent_id,
        strategy_id=maker.strategy_id,
        symbol=maker.symbol,
        ts_ns=maker.first_fill_ns,
        maker_venue=maker.venue,
        maker_side=maker.side.value,
        maker_filled=float(maker.filled),
        maker_price=float(maker_price),
        hedge_venue=hedge.venue if hedge is not None else None,
        hedge_filled=float(hedge.filled) if hedge is not None else 0.0,
        hedge_price=float(hedge_price) if hedge_price is not None else None,
        gross_bps=gross,
        fees_quote=float(fees),
        base_residual=float(base),
        pnl_quote=float(cash + base * mark),
    )


def maker_venues(config: AppConfig) -> dict[str, tuple[str, str]]:
    """
    Read which venue each hedging strategy quotes on and hedges on.

    Parameters
    ----------
    config : AppConfig
        The application configuration.

    Returns
    -------
    dict[str, tuple[str, str]]
        Maker and taker venue per strategy identifier.
    """
    out: dict[str, tuple[str, str]] = {}
    for strategy in config.strategies:
        params = strategy.params
        if params.get("should_match") and "maker_exchange" in params:
            out[strategy.identifier] = (
                str(params["maker_exchange"]),
                str(params["taker_exchange"]),
            )
    return out


def pair_legs(
    legs: dict[tuple[str, str], Leg], venues: dict[str, tuple[str, str]]
) -> list[tuple[Leg, Leg | None]]:
    """
    Match every maker fill with the hedge sharing its client order id.

    Parameters
    ----------
    legs : dict[tuple[str, str], Leg]
        Filled legs keyed by venue and intent id.
    venues : dict[str, tuple[str, str]]
        Maker and taker venue per strategy identifier.

    Returns
    -------
    list[tuple[Leg, Leg | None]]
        Maker leg and hedge leg, in time order of the maker fill. A
        strategy the config does not know is taken to quote wherever its
        first fill happened, and hedged on any other venue with the id.
    """
    pairs: list[tuple[Leg, Leg | None]] = []
    for (venue, intent_id), leg in legs.items():
        if leg.hedge_of is not None:
            continue  # a hedge is never the maker side of a pair
        known = venues.get(leg.strategy_id)
        if known is not None:
            maker_venue, taker_venue = known
            if venue != maker_venue:
                continue
            hedges = [
                other
                for (v, i), other in legs.items()
                if v == taker_venue and (i == intent_id or other.hedge_of == intent_id)
            ]
            hedge = merge_legs(hedges) if hedges else None
        else:
            others = [
                other
                for (v, i), other in legs.items()
                if (i == intent_id or other.hedge_of == intent_id)
                and v != venue
                and other.side is not leg.side
            ]
            if others and min(others, key=lambda o: o.first_fill_ns).first_fill_ns < (
                leg.first_fill_ns
            ):
                continue  # the other venue filled first, so it is the maker
            hedge = merge_legs(others) if others else None
        pairs.append((leg, hedge))
    pairs.sort(key=lambda p: p[0].first_fill_ns)
    return pairs


def latest_schedule(root: Path, venue: str, end_ns: int) -> FeeScheduleEvent | None:
    """
    Return the last fee schedule the recording holds for a venue.

    Parameters
    ----------
    root : Path
        Recording root.
    venue : str
        Venue id.
    end_ns : int
        Schedules received after this are ignored.

    Returns
    -------
    FeeScheduleEvent | None
        The schedule, or None when none was recorded.
    """
    directory = stream_path(root, fees_stream(venue))
    latest: FeeScheduleEvent | None = None
    if not directory.is_dir():
        return None
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
                if isinstance(event, FeeScheduleEvent) and event.ts_recv < end_ns:
                    if latest is None or event.ts_recv > latest.ts_recv:
                        latest = event
    return latest


def resolve_rate(
    venue: VenueConfig | None,
    liquidity: str,
    symbol: str,
    side: Side,
    override_bps: float | None,
    schedule: FeeScheduleEvent | None,
) -> FeeRate:
    """
    Pick the fee rate for a leg, command line first, then config, then recording.

    Parameters
    ----------
    venue : VenueConfig | None
        The venue's config, if configured.
    liquidity : str
        ``maker`` or ``taker``.
    symbol : str
        CCXT symbol, for the fee currency policy and the schedule lookup.
    side : Side
        Side of the leg, for the fee currency policy.
    override_bps : float | None
        Rate given on the command line, basis points.
    schedule : FeeScheduleEvent | None
        The recorded schedule, if any.

    Returns
    -------
    FeeRate
        The rate and where it came from.
    """
    base, quote = base_quote(symbol)
    policy = venue.fee_currency if venue is not None else "quote"
    in_base = fee_in_base(policy, side, base, quote)
    if override_bps is not None:
        return FeeRate(Decimal(str(override_bps)) / 10_000, in_base, "command line")
    if venue is not None:
        static = venue.maker_fee if liquidity == "maker" else venue.taker_fee
        if static is not None:
            return FeeRate(Decimal(str(static)), in_base, "config")
    if schedule is not None:
        terms = schedule_for(schedule, symbol)
        if terms is not None:
            rate = terms.maker if liquidity == "maker" else terms.taker
            if rate is not None:
                return FeeRate(rate, in_base, "recorded schedule")
    return FeeRate(None, in_base, "none")


def summarize(pairs: list[Pair]) -> dict[str, Any]:
    """
    Aggregate valued pairs into totals and an hourly series.

    Parameters
    ----------
    pairs : list[Pair]
        Valued pairs.

    Returns
    -------
    dict[str, Any]
        Totals, medians and quote units per UTC hour.
    """
    hourly: dict[str, float] = defaultdict(float)
    for pair in pairs:
        hour = datetime.fromtimestamp(pair.ts_ns / 1e9, UTC).strftime("%Y-%m-%dT%H")
        hourly[hour] += pair.pnl_quote
    hedged = [p for p in pairs if p.hedge_venue is not None]
    return {
        "pairs": len(pairs),
        "unhedged": len(pairs) - len(hedged),
        "maker_notional": sum(p.maker_filled * p.maker_price for p in pairs),
        "fees_quote": sum(p.fees_quote for p in pairs),
        "pnl_quote": sum(p.pnl_quote for p in pairs),
        "median_gross_bps": (
            statistics.median(p.gross_bps for p in hedged if p.gross_bps is not None)
            if hedged
            else None
        ),
        "hourly": dict(sorted(hourly.items())),
    }


def format_report(
    pairs: list[Pair], summary: dict[str, Any], sources: dict[str, str]
) -> str:
    """
    Render pairs and totals as text.

    Parameters
    ----------
    pairs : list[Pair]
        Valued pairs.
    summary : dict[str, Any]
        Output of ``summarize``.
    sources : dict[str, str]
        Where each venue's fee rate came from.

    Returns
    -------
    str
        The report.
    """
    lines = [
        f"{'time (UTC)':<20}{'strategy':<9}{'side':<5}{'maker fill':>12}{'@':>10}"
        f"{'hedge fill':>12}{'@':>10}{'gross':>7}{'fees':>8}{'resid':>9}{'pnl':>9}"
    ]
    lines.append("-" * len(lines[0]))
    for p in pairs:
        when = datetime.fromtimestamp(p.ts_ns / 1e9, UTC).strftime("%m-%d %H:%M:%S")
        gross = f"{p.gross_bps:>6.1f}" if p.gross_bps is not None else f"{'-':>6}"
        hedge_price = (
            f"{p.hedge_price:>10.6g}" if p.hedge_price is not None else f"{'-':>10}"
        )
        lines.append(
            f"{when:<20}{p.strategy_id:<9}{p.maker_side:<5}{p.maker_filled:>12.4f}"
            f"{p.maker_price:>10.6g}{p.hedge_filled:>12.4f}{hedge_price}{gross} "
            f"{p.fees_quote:>7.4f}{p.base_residual:>9.3f}{p.pnl_quote:>9.4f}"
        )
    lines.append("")
    lines.append(
        f"{summary['pairs']} maker fills, {summary['unhedged']} unhedged, "
        f"maker notional {summary['maker_notional']:.2f}, fees {summary['fees_quote']:.4f}, "
        f"median gross {summary['median_gross_bps']}"
        + (" bps" if summary["median_gross_bps"] is not None else "")
        + f", realized pnl {summary['pnl_quote']:+.4f} quote units"
    )
    for hour, pnl in summary["hourly"].items():
        lines.append(f"  {hour}  {pnl:+.4f}")
    lines.append(
        "fee rates: " + ", ".join(f"{v} {s}" for v, s in sorted(sources.items()))
    )
    lines.append(
        "gross is hedge price against maker price in bps before fees; resid is base "
        "left after both legs, marked at the hedge price inside pnl"
    )
    return "\n".join(lines)


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


def realized_pnl(
    config: AppConfig,
    root: Path,
    start_ns: int,
    end_ns: int,
    maker_fee_bps: float | None,
    taker_fee_bps: float | None,
) -> tuple[list[Pair], dict[str, str]]:
    """
    Value every hedged maker fill in a recording.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    root : Path
        Recording root.
    start_ns : int
        Earliest maker fill kept.
    end_ns : int
        Latest maker fill kept, exclusive.
    maker_fee_bps : float | None
        Maker fee override, basis points.
    taker_fee_bps : float | None
        Taker fee override, basis points.

    Returns
    -------
    tuple[list[Pair], dict[str, str]]
        Valued pairs in time order, and each venue's fee rate source.
    """
    events = read_events(stream_path(root, ORDER_EVENTS_STREAM), start_ns, end_ns)
    legs = fold_legs(events)
    configured = {venue.id: venue for venue in config.venues}
    schedules: dict[str, FeeScheduleEvent | None] = {}
    sources: dict[str, str] = {}
    pairs: list[Pair] = []
    for maker, hedge in pair_legs(legs, maker_venues(config)):
        for venue in {maker.venue} | ({hedge.venue} if hedge else set()):
            if venue not in schedules:
                schedules[venue] = latest_schedule(root, venue, end_ns)
        maker_rate = resolve_rate(
            configured.get(maker.venue),
            "maker",
            maker.symbol,
            maker.side,
            maker_fee_bps,
            schedules[maker.venue],
        )
        sources[maker.venue] = maker_rate.source
        hedge_rate = FeeRate(None, False, "none")
        if hedge is not None:
            hedge_rate = resolve_rate(
                configured.get(hedge.venue),
                "taker",
                hedge.symbol,
                hedge.side,
                taker_fee_bps,
                schedules[hedge.venue],
            )
            sources[hedge.venue] = hedge_rate.source
        pairs.append(value_pair(maker, hedge, maker_rate, hedge_rate))
    return pairs, sources


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
    parser.add_argument("--root", default="data", help="recording root")
    parser.add_argument("--start", help="ISO 8601, inclusive")
    parser.add_argument("--end", help="ISO 8601, exclusive")
    parser.add_argument("--maker-fee-bps", type=float)
    parser.add_argument("--taker-fee-bps", type=float)
    parser.add_argument("--json", help="write pairs and summary here")
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    """
    Run the report.

    Parameters
    ----------
    argv : list[str]
        Arguments without the program name.
    """
    args = parse_args(argv)
    pairs, sources = realized_pnl(
        load_app_config(),
        Path(args.root),
        parse_when(args.start, 0),
        parse_when(args.end, 2**63 - 1),
        args.maker_fee_bps,
        args.taker_fee_bps,
    )
    summary = summarize(pairs)
    print(format_report(pairs, summary, sources))
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(
                {
                    "pairs": [asdict(p) for p in pairs],
                    "summary": summary,
                    "fees": sources,
                },
                fh,
                indent=1,
                default=str,
            )


if __name__ == "__main__":
    main(sys.argv[1:])
