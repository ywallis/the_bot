"""Tests for the backtest analysis tools."""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src.recorder import format_line
from apps.maker.src.tools import check_report as cr
from apps.maker.src.tools import intent_diff as idf
from apps.shared.src.events import (
    INTENTS_STREAM,
    AnyEvent,
    CancelIntent,
    OrderIntent,
    OrderKind,
    Side,
    encode,
    event_type,
    prefixed,
)
from apps.shared.src.streams import StreamPublisher, stream_path

# 2026-09-06T14:00:00Z in nanoseconds.
T14 = 1_788_703_200 * idf.NS_PER_S
S = idf.NS_PER_S
MS = idf.NS_PER_MS
VENUE = "venue_a"
SYMBOL = "BASE/QUOTE"
KEY = "s_es"


def quote(
    ts: int,
    price: str = "0.35",
    amount: str = "10",
    side: Side = Side.SELL,
    strategy: str = KEY,
) -> OrderIntent:
    """Return an order intent."""
    return OrderIntent(
        ts_recv=ts,
        intent_id=f"i-{ts}",
        strategy=strategy,
        venue=VENUE,
        symbol=SYMBOL,
        side=side,
        order_type=OrderKind.LIMIT,
        amount=Decimal(amount),
        price=Decimal(price),
    )


def cancellation(ts: int, strategy: str = KEY) -> CancelIntent:
    """Return a cancel intent."""
    return CancelIntent(
        ts_recv=ts,
        intent_id=f"c-{ts}",
        strategy=strategy,
        venue=VENUE,
        symbol=SYMBOL,
        target_intent_id="",
    )


def series(label: str, *events: AnyEvent) -> idf.Series:
    """Return a series holding the given intents."""
    built = idf.Series(label)
    for event in events:
        built.add(event)
    return built


def write_recording(root: Path, bucket: str, *events: AnyEvent) -> None:
    """Write intents into a recorder bucket file, as the recorder would."""
    directory = stream_path(root, INTENTS_STREAM)
    directory.mkdir(parents=True, exist_ok=True)
    lines = [
        format_line(
            f"{event.ts_recv // 1_000_000}-0", event_type(event).value, encode(event)
        )
        for event in events
    ]
    (directory / f"{bucket}.jsonl").write_bytes(b"".join(lines))


# Pairing and statistics -----------------------------------------------------


def test_pairing_matches_quotes_within_the_tolerance():
    """A quote each side of the tolerance pairs; one beyond it stands alone."""
    live = [quote(T14), quote(T14 + 10 * S), quote(T14 + 20 * S)]
    replayed = [quote(T14 + 100 * MS), quote(T14 + 20 * S)]
    pairs, live_only, replayed_only = idf.pair(live, replayed, 500 * MS)
    assert [p[0].ts_recv for p in pairs] == [T14, T14 + 20 * S]
    assert [q.ts_recv for q in live_only] == [T14 + 10 * S]
    assert replayed_only == []


def test_pairing_keeps_sides_and_keys_apart():
    """A buy is never paired with a sell, nor one key's quote with another's."""
    live = [quote(T14, side=Side.BUY), quote(T14, strategy="other_es")]
    replayed = [quote(T14)]
    pairs, live_only, replayed_only = idf.pair(live, replayed, 500 * MS)
    assert pairs == []
    assert len(live_only) == 2 and len(replayed_only) == 1


def test_cadence_reports_the_gaps_between_intents():
    """The median and 90th percentile gap, in milliseconds."""
    assert idf.cadence([]) == {"count": 0, "median_ms": 0.0, "p90_ms": 0.0}
    assert idf.cadence([T14]) == {"count": 1, "median_ms": 0.0, "p90_ms": 0.0}
    measured = idf.cadence([T14, T14 + S, T14 + 3 * S])
    assert measured["count"] == 3 and measured["median_ms"] == 1500.0


def test_price_deltas_say_which_run_quoted_keener():
    """A lower sell and a higher buy are both more aggressive."""
    pairs = [
        (quote(T14, price="0.35"), quote(T14, price="0.3493")),
        (
            quote(T14 + S, price="0.35", side=Side.BUY),
            quote(T14 + S, price="0.3507", side=Side.BUY),
        ),
    ]
    deltas = idf.price_deltas(pairs)
    assert deltas["paired"] == 2
    assert deltas["replayed_more_aggressive"] == 2
    assert deltas["price_median_bps"] == 0.0  # -20 bps on the sell, +20 on the buy
    assert idf.price_deltas([]) == {}


def test_the_timeline_marks_a_bucket_only_one_run_quoted_in():
    """One run quoting where the other did not is the divergence to chase."""
    live = [quote(T14), quote(T14 + 400 * S)]
    replayed = [quote(T14 + 30 * S)]
    rows = idf.timeline(live, replayed, 300 * S)
    assert [(row["live"], row["replayed"], row["one_sided"]) for row in rows] == [
        (1, 1, False),
        (1, 0, True),
    ]


def test_strategy_filters_match_a_key_or_its_identifier():
    """A filter names either the full key or the identifier before the slot."""
    assert idf.matches(KEY, [])
    assert idf.matches(KEY, ["s_es"])
    assert idf.matches(KEY, ["s"])
    assert not idf.matches(KEY, ["other"])


def test_diff_counts_both_sides_and_names_the_first_divergence():
    """The summary carries the counts, the pairing and where they part."""
    live = series(
        "live",
        quote(T14),
        quote(T14 + 10 * S),
        cancellation(T14 + 11 * S),
    )
    replayed = series("replayed", quote(T14 + 50 * MS))
    result = idf.diff(live, replayed, 500 * MS, 300 * S)
    assert result["quotes"] == {"live": 2, "replayed": 1}
    assert result["pairing"]["paired"] == 1
    assert result["pairing"]["live_only"] == 1
    assert result["first_live_only"]["ts"] == T14 + 10 * S
    assert result["first_replayed_only"] is None
    assert result["cancels"] == [{"key": KEY, "live": 1, "replayed": 0}]
    assert result["per_key"][0]["live"]["count"] == 2
    # And it renders without needing a terminal.
    assert "Quotes: 2 live, 1 replayed" in idf.render(result, "a window", "bt:r1")


# Loading --------------------------------------------------------------------


def test_recorded_intents_are_read_from_the_window_only(tmp_path: Path):
    """The recording is filtered by ``ts_recv``, and other keys are dropped."""
    write_recording(
        tmp_path,
        "2026-09-06T14",
        quote(T14 - S),
        quote(T14 + S),
        quote(T14 + 2 * S, strategy="other_es"),
        cancellation(T14 + 3 * S),
    )
    loaded = idf.load_recorded(tmp_path, T14, T14 + 10 * S, ["s"])
    assert [q.ts_recv for q in loaded.quotes] == [T14 + S]
    assert [c.ts_recv for c in loaded.cancels] == [T14 + 3 * S]


@pytest.mark.asyncio
async def test_replayed_intents_are_read_from_the_prefix():
    """The run's own stream is the replayed side, filters applied."""
    redis = fakeredis.FakeRedis(decode_responses=False)
    publisher = StreamPublisher(maxlen=100, prefix="bt:r1")
    await publisher.publish(redis, quote(T14))
    await publisher.publish(redis, quote(T14 + S, strategy="other_es"))
    await publisher.publish(redis, cancellation(T14 + 2 * S))
    loaded = await idf.load_replayed(redis, "bt:r1", ["s_es"])
    assert [q.ts_recv for q in loaded.quotes] == [T14]
    assert [c.ts_recv for c in loaded.cancels] == [T14 + 2 * S]
    assert await redis.xlen(prefixed("bt:r1", INTENTS_STREAM)) == 3


# Report checks --------------------------------------------------------------


def report(**overrides: Any) -> dict[str, Any]:
    """Return a clean report, overridden field by field."""
    base: dict[str, Any] = {
        "intents": 10,
        "cancels": 4,
        "placed": 8,
        "rejected": 2,
        "cancelled": 4,
        "fills": 3,
        "depth_exhausted": 0,
        "volume": {VENUE: "1000"},
        "fees": {f"{VENUE}:QUOTE": "0.35"},
        "latency": {VENUE: {"source": "measured", "samples": 100}},
        "opening": {VENUE: {"BASE": "1000", "QUOTE": "500"}},
        "closing": {VENUE: {"BASE": "1000", "QUOTE": "500"}},
        "unfunded": [],
        "net": {"BASE": "0", "QUOTE": "0"},
    }
    return base | overrides


def status_of(rows: list[dict[str, str]], check: str) -> str:
    """Return one check's status."""
    return next(row["status"] for row in rows if row["check"] == check)


def test_a_clean_report_passes_every_check():
    """Nothing to say about a run that funded, traded and ended flat."""
    rows = cr.run_checks(report())
    assert {row["status"] for row in rows} == {cr.OK}
    assert "Every check passed" in cr.render(rows, Path("r.json"))


def test_an_unfunded_venue_fails():
    """A venue that never had a balance makes the run unquotable."""
    rows = cr.run_checks(report(unfunded=["venue_b"]))
    assert status_of(rows, "balances") == cr.FAIL
    assert "Not usable" in cr.render(rows, Path("r.json"))


def test_a_position_left_open_fails_against_the_volume_traded():
    """A percent of what was traded is the line, as it is for the broker."""
    assert status_of(cr.run_checks(report(net={"BASE": "9"})), "position") == cr.OK
    assert status_of(cr.run_checks(report(net={"BASE": "11"})), "position") == cr.FAIL
    # Nothing traded, so any drift at all is unexplained.
    nothing = report(volume={}, net={"BASE": "1"})
    assert status_of(cr.run_checks(nothing), "position") == cr.FAIL


def test_a_quiet_or_empty_window_warns():
    """No fill is a caveat; nothing placed at all is a window to check."""
    assert status_of(cr.run_checks(report(fills=0)), "activity") == cr.WARN
    rows = cr.run_checks(report(fills=0, placed=0, rejected=10))
    assert status_of(rows, "activity") == cr.WARN


def test_assumed_latency_and_exhausted_depth_warn():
    """Both make a number a number about an assumption."""
    assumed = report(latency={VENUE: {"source": "assumed"}})
    assert status_of(cr.run_checks(assumed), "latency") == cr.WARN
    assert status_of(cr.run_checks(report(depth_exhausted=2)), "depth") == cr.WARN
    assert status_of(cr.run_checks(report(latency={})), "latency") == cr.WARN


def test_counts_and_rejection_shape_warn():
    """An intent that was neither placed nor refused, and a stuck key's shape."""
    assert status_of(cr.run_checks(report(intents=12)), "counts") == cr.WARN
    stuck = report(intents=20, placed=2, rejected=18)
    assert status_of(cr.run_checks(stuck), "rejections") == cr.WARN


def test_main_reads_a_file_and_sets_the_exit_status(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    """The tool is usable as a gate in a script."""
    path = tmp_path / "run.json"
    path.write_text(json.dumps(report()))
    assert cr.main([str(path)]) == 0
    path.write_text(json.dumps(report(unfunded=["venue_b"])))
    assert cr.main([str(path)]) == 1
    capsys.readouterr()
    assert cr.main([str(path), "--json"]) == 1
    assert status_of(json.loads(capsys.readouterr().out), "balances") == cr.FAIL


def test_parse_args_reads_the_window_and_the_options():
    """The diff tool's command line, including ISO times."""
    args = idf.parse_args(
        ["run1", "--start", "2026-09-06T14:00:00Z", "--strategy", "s", "--bucket", "60"]
    )
    assert args.run_id == "run1"
    assert args.start == T14
    assert args.strategy == ["s"]
    assert args.bucket == 60


def test_a_comparison_with_nothing_on_either_side_still_renders():
    """A first run often publishes nothing; the tool must say so, not crash."""
    result = idf.diff(idf.Series("live"), idf.Series("replayed"), 500 * MS, 300 * S)
    assert result["quotes"] == {"live": 0, "replayed": 0}
    assert result["pairing"]["live_only"] == 0
    assert "Quotes: 0 live, 0 replayed" in idf.render(result, "a window", "bt:r1")
