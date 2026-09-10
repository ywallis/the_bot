"""Check a simulated broker's report against the invariants a usable run holds.

A backtest report is long enough that reading it by eye finds what you were
looking for and misses what you were not. These are the properties that make
a run worth quoting at all, each with what to do when it fails:

- every traded venue had a balance, or the strategy never quoted on it;
- the run ended flat, or the closing balances are a position and not a
  result;
- something actually happened, or the window says nothing about the strategy;
- every fill came out of recorded depth, and every venue's latency was
  measured rather than assumed;
- the counts add up, and rejections are not the shape a stuck strategy key
  makes.

FAIL means the run cannot be quoted as it stands. WARN means it can, with
the caveat named. The exit status is 1 if anything failed, so this can gate
a run in a script.

Usage
-----
    uv run -m apps.maker.src.tools.check_report <run_id>.json
    uv run -m apps.maker.src.tools.check_report <run_id>.json --json
    uv run -m apps.maker.src.tools.check_report <run_id>.json --net-tolerance 0.02

See section 5 of ``docs/runbooks/backtest.md`` for how to read what it
checks.
"""

import argparse
import json
import sys
import textwrap
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

FAIL = "FAIL"
WARN = "WARN"
OK = "OK"

# Width the action text is wrapped to, indent excluded.
ACTION_WIDTH = 68

# How far from flat a run may end, as a share of the largest volume traded on
# any venue. The broker warns at the same threshold.
DEFAULT_NET_TOLERANCE = 0.01


def check_unfunded(report: dict[str, Any]) -> tuple[str, str, str]:
    """
    Check that every traded venue had a balance.

    Parameters
    ----------
    report : dict[str, Any]
        The report as JSON.

    Returns
    -------
    tuple[str, str, str]
        Status, measurement and action.
    """
    unfunded = report.get("unfunded") or []
    if not unfunded:
        return OK, "every traded venue had a balance", ""
    return (
        FAIL,
        f"no balance all run: {', '.join(unfunded)}",
        "open them with --balance VENUE:ASSET=AMOUNT, or replay the recorded "
        "balances; a strategy with no balance for a venue quotes nothing on it",
    )


def check_net(report: dict[str, Any], tolerance: float) -> tuple[str, str, str]:
    """
    Check that the run ended near where it opened.

    Parameters
    ----------
    report : dict[str, Any]
        The report as JSON.
    tolerance : float
        Allowed drift as a share of the largest per-venue volume.

    Returns
    -------
    tuple[str, str, str]
        Status, measurement and action.
    """
    net = {asset: Decimal(total) for asset, total in (report.get("net") or {}).items()}
    volumes = [Decimal(v) for v in (report.get("volume") or {}).values()]
    largest = max(volumes, default=Decimal(0))
    allowed = largest * Decimal(str(tolerance))
    beyond = {
        asset: total for asset, total in net.items() if abs(total) > max(allowed, 0)
    }
    if not beyond:
        return OK, f"flat within {allowed} of the {largest} traded", ""
    worst = ", ".join(f"{total} {asset}" for asset, total in sorted(beyond.items()))
    return (
        FAIL,
        f"ended {worst} from flat, over {allowed}",
        "a profit and loss figure taken from the closing balances of a run "
        "holding a position is that position; find the unhedged fill first "
        "(usually one in the last seconds, so check --drain matching)",
    )


def check_traded(report: dict[str, Any]) -> tuple[str, str, str]:
    """
    Check that the run did something.

    Parameters
    ----------
    report : dict[str, Any]
        The report as JSON.

    Returns
    -------
    tuple[str, str, str]
        Status, measurement and action.
    """
    fills = int(report.get("fills") or 0)
    placed = int(report.get("placed") or 0)
    if fills:
        return OK, f"{placed} placements, {fills} fills", ""
    if placed:
        return (
            WARN,
            f"{placed} placements, no fill",
            "the window says the strategy quoted and was never hit; it says "
            "nothing about what a fill would have earned",
        )
    return (
        WARN,
        "nothing was placed",
        "check the window actually holds data (bucket filenames are not "
        "coverage) and that the strategy had balances and books",
    )


def check_depth(report: dict[str, Any]) -> tuple[str, str, str]:
    """
    Check that no fill outran the recorded book.

    Parameters
    ----------
    report : dict[str, Any]
        The report as JSON.

    Returns
    -------
    tuple[str, str, str]
        Status, measurement and action.
    """
    exhausted = int(report.get("depth_exhausted") or 0)
    if not exhausted:
        return OK, "every fill came out of recorded depth", ""
    return (
        WARN,
        f"{exhausted} order{'s' if exhausted > 1 else ''} outran the recorded depth",
        "the remainder filled at the worst recorded level, which is a price "
        "the recording never showed; discount those fills",
    )


def check_latency(report: dict[str, Any]) -> tuple[str, str, str]:
    """
    Check where each venue's latency came from.

    Parameters
    ----------
    report : dict[str, Any]
        The report as JSON.

    Returns
    -------
    tuple[str, str, str]
        Status, measurement and action.
    """
    latency = report.get("latency") or {}
    assumed = sorted(
        venue
        for venue, summary in latency.items()
        if str(summary.get("source")) != "measured"
    )
    if not latency:
        return WARN, "no latency model in the report", "check the run completed"
    if not assumed:
        return OK, f"measured on {', '.join(sorted(latency))}", ""
    return (
        WARN,
        f"assumed on {', '.join(assumed)}",
        "a number for a venue whose latency is assumed is a number about the "
        "assumption; say so wherever it is quoted, or measure the venue",
    )


def check_counts(report: dict[str, Any]) -> tuple[str, str, str]:
    """
    Check that every intent read was either placed or refused.

    Parameters
    ----------
    report : dict[str, Any]
        The report as JSON.

    Returns
    -------
    tuple[str, str, str]
        Status, measurement and action.
    """
    intents = int(report.get("intents") or 0)
    placed = int(report.get("placed") or 0)
    rejected = int(report.get("rejected") or 0)
    residual = intents - placed - rejected
    if residual == 0:
        return OK, f"{intents} intents = {placed} placed + {rejected} rejected", ""
    return (
        WARN,
        f"{intents} intents, {placed} placed, {rejected} rejected, {residual} neither",
        "an intent in flight when the run closed is one the wind-down did not "
        "finish; a large residual means the run ended early",
    )


def check_rejections(report: dict[str, Any]) -> tuple[str, str, str]:
    """
    Check that rejections are not the shape a stuck strategy key makes.

    Parameters
    ----------
    report : dict[str, Any]
        The report as JSON.

    Returns
    -------
    tuple[str, str, str]
        Status, measurement and action.
    """
    placed = int(report.get("placed") or 0)
    rejected = int(report.get("rejected") or 0)
    if rejected <= placed:
        return OK, f"{rejected} rejected against {placed} placed", ""
    return (
        WARN,
        f"{rejected} rejected against only {placed} placed",
        "coalescing rejects a superseded quote, so some of this is normal; a "
        "key whose every quote is superseded is a lock never released, and "
        "the broker's log names it",
    )


def run_checks(
    report: dict[str, Any], net_tolerance: float = DEFAULT_NET_TOLERANCE
) -> list[dict[str, str]]:
    """
    Run every check over a report.

    Parameters
    ----------
    report : dict[str, Any]
        The report as JSON.
    net_tolerance : float
        Allowed drift from flat, see ``check_net``.

    Returns
    -------
    list[dict[str, str]]
        One row per check: ``check``, ``status``, ``measure`` and ``action``.
        Order is the order to read them in, balances first.
    """
    checks: tuple[tuple[str, Callable[[], tuple[str, str, str]]], ...] = (
        ("balances", lambda: check_unfunded(report)),
        ("position", lambda: check_net(report, net_tolerance)),
        ("activity", lambda: check_traded(report)),
        ("depth", lambda: check_depth(report)),
        ("latency", lambda: check_latency(report)),
        ("counts", lambda: check_counts(report)),
        ("rejections", lambda: check_rejections(report)),
    )
    rows: list[dict[str, str]] = []
    for name, check in checks:
        status, measure, action = check()
        rows.append(
            {"check": name, "status": status, "measure": measure, "action": action}
        )
    return rows


def render(rows: list[dict[str, str]], path: Path) -> str:
    """
    Format the checks for reading.

    Parameters
    ----------
    rows : list[dict[str, str]]
        What ``run_checks`` returned.
    path : Path
        The report checked, for the heading.

    Returns
    -------
    str
        The table and a verdict.
    """
    out = [f"Report {path}", ""]
    for row in rows:
        out.append(f"{row['status']:<5}{row['check']:<12}{row['measure']}")
        if row["action"] and row["status"] != OK:
            wrapped = textwrap.wrap(row["action"], ACTION_WIDTH)
            out.append(f"{'':<17}-> {wrapped[0]}")
            out.extend(f"{'':<20}{line}" for line in wrapped[1:])
    failed = [row["check"] for row in rows if row["status"] == FAIL]
    warned = [row["check"] for row in rows if row["status"] == WARN]
    out.append("")
    if failed:
        out.append(f"Not usable as it stands: {', '.join(failed)}")
    elif warned:
        out.append(f"Usable with caveats: {', '.join(warned)}")
    else:
        out.append("Every check passed")
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
        ``report``, ``net_tolerance`` and ``json``.
    """
    parser = argparse.ArgumentParser(
        description="Check a simulated broker's report for the invariants of a "
        "usable run."
    )
    parser.add_argument("report", type=Path, help="The report JSON the broker wrote")
    parser.add_argument(
        "--net-tolerance",
        type=float,
        default=DEFAULT_NET_TOLERANCE,
        help="Allowed drift from flat as a share of the largest venue volume "
        f"(default {DEFAULT_NET_TOLERANCE})",
    )
    parser.add_argument("--json", action="store_true", help="Print plain data instead")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """
    Check a report and return the exit status.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, ``sys.argv[1:]`` if omitted.

    Returns
    -------
    int
        1 if any check failed, else 0.
    """
    args = parse_args(argv)
    report = json.loads(args.report.read_text())
    rows = run_checks(report, args.net_tolerance)
    print(json.dumps(rows, indent=2) if args.json else render(rows, args.report))
    return 1 if any(row["status"] == FAIL for row in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
