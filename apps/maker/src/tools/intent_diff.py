"""Diff a replayed run's intents against the live ones it stands in for.

A backtest that reproduces the live fill model but not the live *behaviour*
is not testing the same strategy. Section 9 of the design records the case:
from an identical book stream, with every sequence number present on both
venues, the live strategy replaced its quote 467 times and the replayed one
180. Quote cadence is what creates the chance to be filled, so that gap
matters more than any queue arithmetic, and it is a divergence of state
rather than of matching.

This tool puts the two side by side. The live intents come from the
recorder's own files, which are the ground truth for the window; the
replayed ones come from the run's prefix in Redis, so run this before
cleaning the prefix up (section 6 of the runbook). It reports counts per
strategy key and side, the cadence of each, the quotes it could pair within
a tolerance and how their prices differ, and a timeline that says *where* in
the window the two part company. The first unpaired intent on either side is
the place to start reading; everything after it is consequence.

Usage
-----
    uv run -m apps.maker.src.tools.intent_diff <run_id> \
        --start 2026-09-08T12:04Z --end 2026-09-08T20:00Z

    uv run -m apps.maker.src.tools.intent_diff <run_id> \
        --start ... --end ... --strategy <identifier> --bucket 60 --json

``--root`` overrides the recording root, ``--strategy`` restricts to one key
or identifier (repeatable), ``--tolerance-ms`` widens the pairing window and
``--bucket`` sets the timeline resolution.
"""

import argparse
import asyncio
import json
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.replayer import iter_stream, parse_time
from apps.shared.src.config import AppConfig, load_app_config
from apps.shared.src.events import (
    DATA_FIELD,
    INTENTS_STREAM,
    TYPE_FIELD,
    AnyEvent,
    CancelIntent,
    OrderIntent,
    Side,
    backtest_prefix,
    from_stream_fields,
    prefixed,
)
from apps.shared.src.streams import stream_path

logging_config.setup_logging()
logger = logging.getLogger(__name__)

NS_PER_MS = 1_000_000
NS_PER_S = 1_000_000_000
BPS = 10_000

# Quotes this far apart in time are still the same quote seen twice.
DEFAULT_TOLERANCE_MS = 500
# Timeline resolution, in seconds of recorded time.
DEFAULT_BUCKET_S = 300


def at(ts_ns: int) -> str:
    """
    Format a receive time as UTC, to the millisecond.

    Parameters
    ----------
    ts_ns : int
        Nanoseconds since the epoch.

    Returns
    -------
    str
        For example ``2026-09-08T12:04:31.204Z``.
    """
    moment = datetime.fromtimestamp(ts_ns / NS_PER_S, tz=timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


@dataclass
class Series:
    """
    One side of the comparison: the intents one run produced.

    Attributes
    ----------
    label : str
        ``live`` or ``replayed``, for the output.
    quotes : list[OrderIntent]
        Order intents, in recorded order.
    cancels : list[CancelIntent]
        Cancel intents, in recorded order.
    """

    label: str
    quotes: list[OrderIntent] = field(default_factory=list)
    cancels: list[CancelIntent] = field(default_factory=list)

    def add(self, event: AnyEvent) -> None:
        """
        File an intent, ignoring anything else on the stream.

        Parameters
        ----------
        event : AnyEvent
            A decoded stream entry.
        """
        if isinstance(event, OrderIntent):
            self.quotes.append(event)
        elif isinstance(event, CancelIntent):
            self.cancels.append(event)


def matches(strategy: str, filters: list[str]) -> bool:
    """
    Return whether a strategy key passes the ``--strategy`` filters.

    Parameters
    ----------
    strategy : str
        The intent's strategy key, ``<identifier>_<slot>`` or a bare
        identifier.
    filters : list[str]
        Keys or identifiers to keep; empty keeps everything.

    Returns
    -------
    bool
        True if the key should be counted.
    """
    if not filters:
        return True
    return strategy in filters or strategy.partition("_")[0] in filters


def load_recorded(
    root: Path, start_ns: int | None, end_ns: int | None, filters: list[str]
) -> Series:
    """
    Read the live intents of a window from the recorder's files.

    Parameters
    ----------
    root : Path
        Recording root, as the recorder wrote it.
    start_ns : int | None
        Inclusive start, or None for the whole recording.
    end_ns : int | None
        Exclusive end, or None.
    filters : list[str]
        ``--strategy`` filters, see ``matches``.

    Returns
    -------
    Series
        The live intents.
    """
    series = Series("live")
    directory = stream_path(root, INTENTS_STREAM)
    malformed: list[str] = []
    for record in iter_stream(INTENTS_STREAM, directory, start_ns, end_ns, malformed):
        event = from_stream_fields(
            {TYPE_FIELD: record.event_type, DATA_FIELD: record.data}
        )
        if getattr(event, "strategy", None) is not None and matches(
            cast(Any, event).strategy, filters
        ):
            series.add(event)
    if malformed:
        logger.warning(
            f"{len(malformed)} recorder lines did not parse: {malformed[:3]}"
        )
    return series


async def load_replayed(redis: Any, prefix: str, filters: list[str]) -> Series:
    """
    Read the intents a replayed run published under its prefix.

    Parameters
    ----------
    redis : Any
        A ``redis.asyncio.Redis`` client.
    prefix : str
        Backtest prefix, e.g. ``bt:run1``.
    filters : list[str]
        ``--strategy`` filters, see ``matches``.

    Returns
    -------
    Series
        The replayed intents.
    """
    series = Series("replayed")
    entries = cast(
        list[Any], await redis.xrange(prefixed(prefix, INTENTS_STREAM)) or []
    )
    for _entry_id, fields in entries:
        event = from_stream_fields(fields)
        if getattr(event, "strategy", None) is not None and matches(
            cast(Any, event).strategy, filters
        ):
            series.add(event)
    return series


def cadence(times: list[int]) -> dict[str, float | int]:
    """
    Describe how often intents followed one another.

    Parameters
    ----------
    times : list[int]
        Receive times in nanoseconds, in order.

    Returns
    -------
    dict[str, float | int]
        Count, and the median and 90th percentile gap in milliseconds. The
        gaps are 0 for fewer than two intents.
    """
    gaps = [
        (later - earlier) / NS_PER_MS
        for earlier, later in zip(times, times[1:], strict=False)
    ]
    if not gaps:
        return {"count": len(times), "median_ms": 0.0, "p90_ms": 0.0}
    ordered = sorted(gaps)
    return {
        "count": len(times),
        "median_ms": round(statistics.median(ordered), 1),
        "p90_ms": round(ordered[min(len(ordered) - 1, int(0.9 * len(ordered)))], 1),
    }


def pair(
    live: list[OrderIntent], replayed: list[OrderIntent], tolerance_ns: int
) -> tuple[list[tuple[OrderIntent, OrderIntent]], list[OrderIntent], list[OrderIntent]]:
    """
    Pair quotes of the same key and side that fall within a tolerance.

    Walks both sequences in time order and pairs greedily: the earliest
    unpaired quote on the side that is behind either finds a partner within
    the tolerance or is reported alone. Pairing is per key and side, so a
    strategy that quoted one side only is not matched against the other.

    Parameters
    ----------
    live : list[OrderIntent]
        Live quotes.
    replayed : list[OrderIntent]
        Replayed quotes.
    tolerance_ns : int
        How far apart two quotes may be and still be the same quote.

    Returns
    -------
    tuple[list[tuple[OrderIntent, OrderIntent]], list[OrderIntent], list[OrderIntent]]
        Pairs, then the quotes only live had, then those only the replay had.
    """
    pairs: list[tuple[OrderIntent, OrderIntent]] = []
    live_only: list[OrderIntent] = []
    replayed_only: list[OrderIntent] = []
    groups = {(q.strategy, q.side) for q in live} | {
        (q.strategy, q.side) for q in replayed
    }
    for group in sorted(groups, key=lambda g: (g[0], g[1].value)):
        left = sorted(
            (q for q in live if (q.strategy, q.side) == group),
            key=lambda q: q.ts_recv,
        )
        right = sorted(
            (q for q in replayed if (q.strategy, q.side) == group),
            key=lambda q: q.ts_recv,
        )
        i = j = 0
        while i < len(left) and j < len(right):
            if abs(left[i].ts_recv - right[j].ts_recv) <= tolerance_ns:
                pairs.append((left[i], right[j]))
                i += 1
                j += 1
            elif left[i].ts_recv < right[j].ts_recv:
                live_only.append(left[i])
                i += 1
            else:
                replayed_only.append(right[j])
                j += 1
        live_only.extend(left[i:])
        replayed_only.extend(right[j:])
    return (
        sorted(pairs, key=lambda p: p[0].ts_recv),
        sorted(live_only, key=lambda q: q.ts_recv),
        sorted(replayed_only, key=lambda q: q.ts_recv),
    )


def more_aggressive(live: OrderIntent, replayed: OrderIntent) -> bool:
    """
    Return whether the replayed quote sat closer to the market than the live one.

    Parameters
    ----------
    live : OrderIntent
        The live quote.
    replayed : OrderIntent
        The replayed quote it was paired with.

    Returns
    -------
    bool
        True if the replayed price is keener: lower for a sell, higher for a
        buy. False when either quote carries no price, a market order having
        no price to compare.
    """
    if live.price is None or replayed.price is None:
        return False
    if live.side is Side.SELL:
        return replayed.price < live.price
    return replayed.price > live.price


def price_deltas(pairs: list[tuple[OrderIntent, OrderIntent]]) -> dict[str, Any]:
    """
    Compare the prices and sizes of paired quotes.

    Parameters
    ----------
    pairs : list[tuple[OrderIntent, OrderIntent]]
        Live and replayed quotes, paired.

    Returns
    -------
    dict[str, Any]
        Median price difference in basis points (replayed against live), the
        median size difference, and how many replayed quotes were more
        aggressive. Empty when nothing could be compared.
    """
    bps: list[float] = []
    sizes: list[float] = []
    keener = 0
    for live, replayed in pairs:
        if live.price is not None and replayed.price is not None and live.price > 0:
            bps.append(float((replayed.price - live.price) / live.price) * BPS)
        sizes.append(float(replayed.amount - live.amount))
        keener += more_aggressive(live, replayed)
    if not pairs:
        return {}
    return {
        "paired": len(pairs),
        "price_median_bps": round(statistics.median(bps), 2) if bps else None,
        "size_median": round(statistics.median(sizes), 6) if sizes else None,
        "replayed_more_aggressive": keener,
    }


def timeline(
    live: list[OrderIntent], replayed: list[OrderIntent], bucket_ns: int
) -> list[dict[str, Any]]:
    """
    Count quotes per time bucket on both sides.

    Parameters
    ----------
    live : list[OrderIntent]
        Live quotes.
    replayed : list[OrderIntent]
        Replayed quotes.
    bucket_ns : int
        Bucket width in nanoseconds.

    Returns
    -------
    list[dict[str, Any]]
        One row per bucket that either side has a quote in, oldest first,
        with the bucket's start time and the two counts. ``one_sided`` marks
        a bucket where one run quoted and the other did not, which is the
        divergence of state to chase rather than a difference of degree.
    """
    counts: dict[int, list[int]] = {}
    for quote in live:
        counts.setdefault(quote.ts_recv // bucket_ns, [0, 0])[0] += 1
    for quote in replayed:
        counts.setdefault(quote.ts_recv // bucket_ns, [0, 0])[1] += 1
    rows: list[dict[str, Any]] = []
    for bucket, (live_count, replayed_count) in sorted(counts.items()):
        rows.append(
            {
                "ts": bucket * bucket_ns,
                "live": live_count,
                "replayed": replayed_count,
                "one_sided": (live_count == 0) != (replayed_count == 0),
            }
        )
    return rows


def diff(
    live: Series, replayed: Series, tolerance_ns: int, bucket_ns: int
) -> dict[str, Any]:
    """
    Compare two runs' intents.

    Parameters
    ----------
    live : Series
        The live intents.
    replayed : Series
        The replayed intents.
    tolerance_ns : int
        Pairing tolerance, see ``pair``.
    bucket_ns : int
        Timeline resolution, see ``timeline``.

    Returns
    -------
    dict[str, Any]
        Plain data: per-key counts and cadence, the pairing and its price
        deltas, the first unpaired quote on each side and the timeline.
    """
    keys = sorted(
        {q.strategy for q in live.quotes} | {q.strategy for q in replayed.quotes}
    )
    per_key: list[dict[str, Any]] = []
    for key in keys:
        for side in (Side.BUY, Side.SELL):
            live_times = [
                q.ts_recv for q in live.quotes if q.strategy == key and q.side is side
            ]
            replayed_times = [
                q.ts_recv
                for q in replayed.quotes
                if q.strategy == key and q.side is side
            ]
            if not live_times and not replayed_times:
                continue
            per_key.append(
                {
                    "key": key,
                    "side": side.value,
                    "live": cadence(live_times),
                    "replayed": cadence(replayed_times),
                }
            )
    cancels = [
        {
            "key": key,
            "live": sum(1 for c in live.cancels if c.strategy == key),
            "replayed": sum(1 for c in replayed.cancels if c.strategy == key),
        }
        for key in sorted(
            {c.strategy for c in live.cancels} | {c.strategy for c in replayed.cancels}
        )
    ]
    pairs, live_only, replayed_only = pair(live.quotes, replayed.quotes, tolerance_ns)
    return {
        "quotes": {"live": len(live.quotes), "replayed": len(replayed.quotes)},
        "per_key": per_key,
        "cancels": cancels,
        "pairing": price_deltas(pairs)
        | {
            "live_only": len(live_only),
            "replayed_only": len(replayed_only),
            "tolerance_ms": tolerance_ns // NS_PER_MS,
        },
        "first_live_only": _describe(live_only[0]) if live_only else None,
        "first_replayed_only": _describe(replayed_only[0]) if replayed_only else None,
        "timeline": timeline(live.quotes, replayed.quotes, bucket_ns),
    }


def _describe(quote: OrderIntent) -> dict[str, Any]:
    return {
        "ts": quote.ts_recv,
        "at": at(quote.ts_recv),
        "key": quote.strategy,
        "side": quote.side.value,
        "price": None if quote.price is None else str(quote.price),
        "amount": str(quote.amount),
        "intent_id": quote.intent_id,
    }


def render(result: dict[str, Any], window: str, prefix: str) -> str:
    """
    Format a comparison for reading.

    Parameters
    ----------
    result : dict[str, Any]
        What ``diff`` returned.
    window : str
        The window compared, for the heading.
    prefix : str
        The replayed prefix, for the heading.

    Returns
    -------
    str
        The report.
    """
    out: list[str] = [f"Intents: live {window} against {prefix}", ""]
    quotes = result["quotes"]
    ratio = quotes["replayed"] / quotes["live"] if quotes["live"] else 0.0
    out.append(
        f"Quotes: {quotes['live']} live, {quotes['replayed']} replayed "
        f"({ratio:.2f} of live)"
    )
    out.append("")
    out.append(
        f"{'key':<16}{'side':<6}{'live':>7}{'replayed':>10}{'ratio':>8}"
        f"{'live gap':>12}{'replayed gap':>15}"
    )
    for row in result["per_key"]:
        live_count = cast(int, row["live"]["count"])
        replayed_count = cast(int, row["replayed"]["count"])
        share = replayed_count / live_count if live_count else 0.0
        live_gap = cast(float, row["live"]["median_ms"]) / 1000
        replayed_gap = cast(float, row["replayed"]["median_ms"]) / 1000
        out.append(
            f"{row['key']:<16}{row['side']:<6}{live_count:>7}"
            f"{replayed_count:>10}{share:>8.2f}"
            f"{live_gap:>11.1f}s{replayed_gap:>14.1f}s"
        )
    if result["cancels"]:
        out.append("")
        out.append(f"{'cancels by key':<16}{'live':>13}{'replayed':>10}")
        for row in result["cancels"]:
            out.append(f"{row['key']:<16}{row['live']:>13}{row['replayed']:>10}")
    pairing = result["pairing"]
    out.append("")
    out.append(
        f"Paired within {pairing['tolerance_ms']} ms: {pairing.get('paired', 0)}; "
        f"{pairing['live_only']} live only, {pairing['replayed_only']} replayed only"
    )
    if pairing.get("paired"):
        out.append(
            f"  price: median {pairing['price_median_bps']} bps against live, "
            f"replayed keener in {pairing['replayed_more_aggressive']} of "
            f"{pairing['paired']}"
        )
        out.append(f"  size: median difference {pairing['size_median']}")
    for label, headline in (
        ("first_live_only", "First quote only live made"),
        ("first_replayed_only", "First quote only the replay made"),
    ):
        entry = result[label]
        if entry is not None:
            out.append(
                f"{headline}: {entry['at']} {entry['key']} {entry['side']} "
                f"{entry['price']} x {entry['amount']}"
            )
    out.append("")
    out.append("Timeline (* one run quoted and the other did not)")
    out.append(f"{'bucket':<26}{'live':>7}{'replayed':>10}")
    for row in result["timeline"]:
        mark = " *" if row["one_sided"] else ""
        out.append(
            f"{at(cast(int, row['ts'])):<26}{row['live']:>7}{row['replayed']:>10}{mark}"
        )
    return "\n".join(out)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parse the command line.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, ``sys.argv[1:]`` if omitted.

    Returns
    -------
    argparse.Namespace
        ``run_id``, ``root``, ``start``, ``end``, ``strategy``,
        ``tolerance_ms``, ``bucket`` and ``json``.
    """
    parser = argparse.ArgumentParser(
        description="Diff a replayed run's intents against the live recording."
    )
    parser.add_argument("run_id", help="Backtest run id, as given to the replayer")
    parser.add_argument(
        "--root", type=Path, default=None, help="Recording root; config's if omitted"
    )
    parser.add_argument(
        "--start", type=parse_time, default=None, help="Window start, ISO 8601"
    )
    parser.add_argument(
        "--end", type=parse_time, default=None, help="Window end, ISO 8601, exclusive"
    )
    parser.add_argument(
        "--strategy",
        action="append",
        default=[],
        help="Only this strategy key or identifier; repeatable",
    )
    parser.add_argument(
        "--tolerance-ms",
        type=int,
        default=DEFAULT_TOLERANCE_MS,
        help=f"Pairing tolerance in ms (default {DEFAULT_TOLERANCE_MS})",
    )
    parser.add_argument(
        "--bucket",
        type=int,
        default=DEFAULT_BUCKET_S,
        help=f"Timeline bucket in seconds (default {DEFAULT_BUCKET_S})",
    )
    parser.add_argument("--json", action="store_true", help="Print plain data instead")
    return parser.parse_args(argv)


async def main(config: AppConfig, args: argparse.Namespace) -> dict[str, Any]:
    """
    Load both sides, compare them and print the result.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    args : argparse.Namespace
        Parsed command line, see ``parse_args``.

    Returns
    -------
    dict[str, Any]
        The comparison, as ``diff`` returns it.
    """
    prefix = backtest_prefix(args.run_id)
    root = args.root if args.root is not None else Path(config.recorder.root)
    live = load_recorded(root, args.start, args.end, args.strategy)
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=2
    )
    redis = Redis(decode_responses=False, connection_pool=pool)
    try:
        replayed = await load_replayed(redis, prefix, args.strategy)
    finally:
        await redis.aclose()
    if not replayed.quotes and not replayed.cancels:
        logger.warning(
            f"No intents under {prefix}: either the run published none or the "
            "prefix has been cleaned up, and a diff against nothing says nothing"
        )
    result = diff(live, replayed, args.tolerance_ms * NS_PER_MS, args.bucket * NS_PER_S)
    window = (
        f"{at(args.start) if args.start else 'start'} to "
        f"{at(args.end) if args.end else 'end'}"
    )
    print(json.dumps(result, indent=2) if args.json else render(result, window, prefix))
    return result


if __name__ == "__main__":
    asyncio.run(main(load_app_config(), parse_args()))
