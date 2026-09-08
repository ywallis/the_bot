"""Tests for the latency model."""

from pathlib import Path

import pytest

from apps.maker.src import latency as lat
from apps.maker.src.recorder import format_line
from apps.shared.src import events
from apps.shared.src.events import LATENCY_STREAM, LatencyRecord
from apps.shared.src.streams import stream_path

T0 = 1_788_703_200_000_000_000
MS = 1_000_000


def record(
    venue: str = "venue_a",
    oms_lag_ms: float = 0.6,
    send_lag_ms: float = 0.5,
    rtt_ms: float = 300.0,
    complete: bool = True,
) -> LatencyRecord:
    """Return a latency record with the given legs."""
    created = T0
    oms = created + int(oms_lag_ms * MS)
    send = oms + int(send_lag_ms * MS)
    ack = send + int(rtt_ms * MS)
    return LatencyRecord(
        ts_recv=ack,
        intent_id="i",
        venue=venue,
        ts_created=created,
        ts_oms_recv=oms if complete else None,
        ts_broker_send=send,
        ts_broker_ack=ack,
    )


def test_a_sample_is_the_three_legs_of_a_record():
    """Legs are differences of the record's timestamps; half the round trip is one way."""
    sample = lat.sample_of(record(rtt_ms=300))
    assert sample == lat.LatencySample(600_000, 500_000, 300 * MS)
    assert sample.one_way_ns == 150 * MS
    assert lat.sample_of(record(complete=False)) is None
    backwards = record()
    backwards = LatencyRecord(
        **{
            **{f: getattr(backwards, f) for f in backwards.__struct_fields__},
            "ts_broker_ack": T0,
        }
    )
    assert lat.sample_of(backwards) is None


def test_draws_repeat_for_a_seed_and_stay_within_the_measured_set():
    """Two models with the same seed draw the same sequence, from the samples given."""
    records = [record(rtt_ms=ms) for ms in (290, 300, 310, 800)]
    a, b = lat.LatencyModel(seed=7), lat.LatencyModel(seed=7)
    assert a.add_records(records) == 4
    b.add_records(records)
    draws_a = [a.for_venue("venue_a").draw().rtt_ns for _ in range(20)]
    draws_b = [b.for_venue("venue_a").draw().rtt_ns for _ in range(20)]
    assert draws_a == draws_b
    assert set(draws_a) <= {290 * MS, 300 * MS, 310 * MS, 800 * MS}
    assert a.for_venue("venue_a").source == lat.MEASURED
    other = lat.LatencyModel(seed=8)
    other.add_records(records)
    assert [other.for_venue("venue_a").draw().rtt_ns for _ in range(20)] != draws_a


def test_a_venue_without_data_needs_an_explicit_assumption():
    """Missing venues are an error, an assumption fills in and is labelled as such."""
    model = lat.LatencyModel()
    model.add_records([record("venue_a")])
    with pytest.raises(KeyError):
        model.for_venue("venue_b")
    with pytest.raises(KeyError):
        model.require(["venue_a", "venue_b"])
    model.assume("venue_b", rtt_ms=400)
    model.require(["venue_a", "venue_b"])
    drawn = model.for_venue("venue_b").draw()
    assert drawn.rtt_ns == 400 * MS
    assert drawn.oms_lag_ns == lat.ASSUMED_OMS_LAG_NS
    assert model.for_venue("venue_b").source == lat.ASSUMED
    # Measured data never overrides an explicit assumption.
    model.add_records([record("venue_b", rtt_ms=100)])
    assert model.for_venue("venue_b").draw().rtt_ns == 400 * MS


def test_summary_reports_source_and_round_trip_quantiles():
    """The report shows what the model rests on."""
    model = lat.LatencyModel()
    model.add_records([record(rtt_ms=ms) for ms in range(100, 300, 10)])
    model.assume("venue_b", 500)
    summary = model.summary()
    assert summary["venue_a"]["source"] == "measured"
    assert summary["venue_a"]["samples"] == 20
    assert summary["venue_a"]["rtt_median_ms"] == pytest.approx(195)
    assert summary["venue_a"]["rtt_p95_ms"] == 290
    assert summary["venue_a"]["rtt_max_ms"] == 290
    assert summary["venue_b"] == {
        "source": "assumed",
        "samples": 1,
        "rtt_median_ms": 500,
        "rtt_p95_ms": 500,
        "rtt_max_ms": 500,
    }


def test_latency_records_are_read_from_the_whole_recording(tmp_path: Path):
    """Every bucket of ``oms:latency`` feeds the model, not a backtest's range."""
    assert lat.latency_records(tmp_path) == []
    directory = stream_path(tmp_path, LATENCY_STREAM)
    directory.mkdir(parents=True)
    for hour, ms in (("10", 300), ("11", 310), ("12", 320)):
        rec = record(rtt_ms=ms)
        (directory / f"2026-09-06T{hour}.jsonl").write_bytes(
            format_line(f"{rec.ts_recv // MS}-0", "latency", events.encode(rec))
        )
    records = lat.latency_records(tmp_path)
    samples = [lat.sample_of(r) for r in records]
    assert [s.rtt_ns for s in samples if s is not None] == [
        300 * MS,
        310 * MS,
        320 * MS,
    ]
