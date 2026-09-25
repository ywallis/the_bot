"""Tests for the funding rate helpers."""

import pytest

from apps.shared.src.funding import (
    DAY_MS,
    HOUR_MS,
    coverage_hours,
    infer_interval_s,
    interval_seconds,
    parse_funding_history,
    parse_funding_rate,
    per_day,
    sum_rates,
)


def test_interval_seconds_parses_ccxt_spellings_and_rejects_the_rest():
    """Number plus unit is the form; anything else is unknown, not a guess."""
    assert interval_seconds("8h") == 28_800
    assert interval_seconds("4h") == 14_400
    assert interval_seconds(" 1h ") == 3_600
    assert interval_seconds("30m") == 1_800
    assert interval_seconds("1d") == 86_400
    assert interval_seconds(None) is None
    assert interval_seconds("eight hours") is None
    assert interval_seconds("") is None


def test_parse_funding_rate_reads_the_fields_and_needs_a_rate():
    """The rate, interval, mark, index and next time are kept; no rate is None."""
    rate = parse_funding_rate(
        {
            "fundingRate": "0.0001",
            "interval": "8h",
            "markPrice": 1.5,
            "indexPrice": 1.49,
            "nextFundingTimestamp": 1_700_000_000_000,
            "fundingTimestamp": 1_699_000_000_000,
        }
    )
    assert rate is not None
    assert rate.rate == 0.0001
    assert rate.interval_s == 28_800
    assert rate.mark_price == 1.5 and rate.index_price == 1.49
    assert rate.next_funding_ms == 1_700_000_000_000
    assert parse_funding_rate({"fundingRate": None, "interval": "8h"}) is None
    sparse = parse_funding_rate({"fundingRate": -0.002})
    assert sparse is not None
    assert sparse.interval_s is None and sparse.mark_price is None
    assert sparse.next_funding_ms is None


def test_parse_funding_rate_falls_back_to_the_funding_time():
    """Without a next time, the structure's own funding time is used."""
    rate = parse_funding_rate({"fundingRate": 0.0, "fundingTimestamp": 5})
    assert rate is not None and rate.next_funding_ms == 5


def test_parse_funding_history_orders_dedupes_and_drops_incomplete_entries():
    """History comes back oldest first with one rate per time."""
    history = parse_funding_history(
        [
            {"timestamp": 3000, "fundingRate": 0.0003},
            {"timestamp": 1000, "fundingRate": "0.0001"},
            {"timestamp": 3000, "fundingRate": 0.0003},
            {"timestamp": None, "fundingRate": 0.0009},
            {"timestamp": 2000, "fundingRate": None},
        ]
    )
    assert history == [(1000, 0.0001), (3000, 0.0003)]


def test_infer_interval_from_history_spacing():
    """The median gap is the interval; one entry says nothing."""
    eight_hourly = [(k * 8 * HOUR_MS, 0.0) for k in range(4)]
    assert infer_interval_s(eight_hourly) == 28_800
    with_a_gap = eight_hourly + [(5 * 8 * HOUR_MS, 0.0)]
    assert infer_interval_s(with_a_gap) == 28_800
    assert infer_interval_s([(0, 0.0)]) is None


def test_sum_rates_over_a_half_open_window():
    """Rates at or after the start and before the end are summed."""
    history = [(0, 0.1), (1000, 0.2), (2000, 0.4), (3000, 0.8)]
    assert sum_rates(history, 1000, 3000) == 0.2 + 0.4
    assert sum_rates(history, 0, 4000) == 1.5
    assert sum_rates(history, 5000, 6000) == 0.0
    assert sum_rates([], 0, 10) == 0.0


def test_coverage_and_per_day():
    """Coverage is the span of the history; per day scales by the interval."""
    assert coverage_hours([(0, 0.0), (DAY_MS, 0.0)]) == 24.0
    assert coverage_hours([(0, 0.0)]) == 0.0
    assert per_day(0.0001, 28_800) == pytest.approx(0.0003)
    assert per_day(0.0001, None) is None
    assert per_day(0.0001, 0) is None
