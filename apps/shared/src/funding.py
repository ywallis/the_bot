"""Funding rates of linear perpetuals: parsing CCXT's structures and summing them.

A perpetual pays funding every interval, a rate applied to the position's
notional; a positive rate is paid by longs to shorts. The screener and the
funding feed both read the rate through CCXT and both need the same two
things from it: the interval in seconds, which venues spell in their own
way or not at all, and a sum of the rates over a window, which is what a
short position earns or pays over a holding period. Both live here so the
figures the screener selected a candidate on are the figures the feed
publishes once the candidate trades.
"""

import re
import statistics
from dataclasses import dataclass
from typing import Any

# CCXT spells the funding interval as a number and a unit, e.g. ``8h``.
_INTERVAL = re.compile(r"^\s*(\d+)\s*([smhd])\s*$")
_UNIT_S = {"s": 1, "m": 60, "h": 3_600, "d": 86_400}

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
WEEK_MS = 7 * DAY_MS


@dataclass(frozen=True)
class FundingRate:
    """
    The current funding of one perpetual, as one CCXT call reports it.

    Attributes
    ----------
    rate : float
        The rate for the coming interval as a fraction of notional; positive
        pays the short.
    interval_s : int | None
        Seconds between funding payments, None when the venue does not say.
    mark_price : float | None
        Mark price, the price funding and liquidation are computed on.
    index_price : float | None
        Index price, the spot reference the mark tracks.
    next_funding_ms : int | None
        When the next payment is due, milliseconds since the epoch.
    """

    rate: float
    interval_s: int | None
    mark_price: float | None
    index_price: float | None
    next_funding_ms: int | None


def interval_seconds(text: str | None) -> int | None:
    """
    Parse CCXT's funding interval string into seconds.

    Parameters
    ----------
    text : str | None
        The interval as CCXT reports it, e.g. ``8h``; None when absent.

    Returns
    -------
    int | None
        Seconds, or None when the text is absent or not in that form.
    """
    if text is None:
        return None
    match = _INTERVAL.match(text)
    if match is None:
        return None
    return int(match.group(1)) * _UNIT_S[match.group(2)]


def parse_funding_rate(structure: dict[str, Any]) -> FundingRate | None:
    """
    Reduce a CCXT funding rate structure to what is used here.

    Parameters
    ----------
    structure : dict[str, Any]
        What ``fetch_funding_rate`` returned.

    Returns
    -------
    FundingRate | None
        The rate, or None when the structure carries no rate at all.
    """
    rate = structure.get("fundingRate")
    if rate is None:
        return None
    mark = structure.get("markPrice")
    index = structure.get("indexPrice")
    next_ms = structure.get("nextFundingTimestamp") or structure.get("fundingTimestamp")
    return FundingRate(
        rate=float(rate),
        interval_s=interval_seconds(structure.get("interval")),
        mark_price=float(mark) if mark is not None else None,
        index_price=float(index) if index is not None else None,
        next_funding_ms=int(next_ms) if next_ms is not None else None,
    )


def parse_funding_history(
    structures: list[dict[str, Any]],
) -> list[tuple[int, float]]:
    """
    Reduce CCXT's funding rate history to time-ordered (time, rate) pairs.

    Parameters
    ----------
    structures : list[dict[str, Any]]
        What ``fetch_funding_rate_history`` returned.

    Returns
    -------
    list[tuple[int, float]]
        Milliseconds since the epoch and the rate paid then, oldest first,
        one entry per distinct time. Entries without a time or a rate are
        dropped.
    """
    by_time: dict[int, float] = {}
    for entry in structures:
        ts, rate = entry.get("timestamp"), entry.get("fundingRate")
        if ts is None or rate is None:
            continue
        by_time[int(ts)] = float(rate)
    return sorted(by_time.items())


def infer_interval_s(history: list[tuple[int, float]]) -> int | None:
    """
    Infer the funding interval from the spacing of the history.

    Venues that report no interval on the current rate still pay on a
    fixed schedule, which the history shows.

    Parameters
    ----------
    history : list[tuple[int, float]]
        Time-ordered (time, rate) pairs.

    Returns
    -------
    int | None
        The median gap in seconds, or None with fewer than two entries.
    """
    if len(history) < 2:
        return None
    gaps = [(b - a) // 1000 for (a, _), (b, _) in zip(history, history[1:])]
    return int(statistics.median(gaps))


def sum_rates(history: list[tuple[int, float]], since_ms: int, until_ms: int) -> float:
    """
    Sum the rates paid inside a window.

    Parameters
    ----------
    history : list[tuple[int, float]]
        Time-ordered (time, rate) pairs.
    since_ms : int
        Start of the window, inclusive, milliseconds.
    until_ms : int
        End of the window, exclusive, milliseconds.

    Returns
    -------
    float
        The sum, a fraction of notional over the window. What a short
        earned if positive, paid if negative.
    """
    return sum(rate for ts, rate in history if since_ms <= ts < until_ms)


def coverage_hours(history: list[tuple[int, float]]) -> float:
    """
    Return how many hours the history spans.

    Parameters
    ----------
    history : list[tuple[int, float]]
        Time-ordered (time, rate) pairs.

    Returns
    -------
    float
        Hours between the first and the last entry, 0 with fewer than two.
    """
    if len(history) < 2:
        return 0.0
    return (history[-1][0] - history[0][0]) / HOUR_MS


def per_day(rate: float, interval_s: int | None) -> float | None:
    """
    Scale a per-interval rate to a day.

    Parameters
    ----------
    rate : float
        The rate paid per interval.
    interval_s : int | None
        Seconds per interval.

    Returns
    -------
    float | None
        The rate a day at that pace, or None without an interval.
    """
    if not interval_s:
        return None
    return rate * 86_400 / interval_s
