"""Tests for the fill-and-hedge simulator."""

import json
from pathlib import Path

import pytest

from apps.maker.src.tools.fill_sim import (
    NS,
    Fill,
    Params,
    book_valid,
    bucket_bounds,
    fill_base,
    fill_decision,
    min_tick,
    format_hourly,
    format_table,
    main,
    merged_events,
    pnl_bps,
    quote_price,
    quote_side_hit,
    resolve_fees,
    simulate,
    summarize,
    walk_depth,
)
from apps.shared.src.config import VenueConfig
from apps.shared.src.events import book_stream, decode, trade_stream
from apps.shared.src.streams import stream_path

SYMBOL = "BASE/QUOTE"
MAKER = "venue_a"
TAKER = "venue_b"
# 2026-09-20T14:00:00Z
T0 = 1_789_912_800 * NS


def test_walk_depth_averages_over_levels():
    """The average price weights each level by what is taken from it."""
    levels = [(1.0, 2.0), (2.0, 2.0)]
    assert walk_depth(levels, 1.0) == 1.0
    assert walk_depth(levels, 3.0) == pytest.approx(4.0 / 3.0)
    assert walk_depth(levels, 4.0) == pytest.approx(1.5)


def test_walk_depth_is_none_when_too_thin():
    """A book holding less than the amount cannot absorb the hedge."""
    assert walk_depth([(1.0, 2.0)], 2.5) is None
    assert walk_depth([], 1.0) is None


def test_book_valid_rejects_empty_and_self_crossed_books():
    """A stale level that crosses the book disqualifies it."""
    assert book_valid([(1.0, 1.0)], [(1.1, 1.0)])
    assert not book_valid([], [(1.1, 1.0)])
    assert not book_valid([(1.1, 1.0)], [(1.1, 1.0)])
    assert not book_valid([(1.2, 1.0)], [(1.1, 1.0)])


def test_quote_side_and_price():
    """An aggressive buy hits a sell quote priced above the taker ask."""
    assert quote_side_hit("buy") == "sell"
    assert quote_side_hit("sell") == "buy"
    assert quote_price("sell", 99.0, 100.0, 0.003) == pytest.approx(100.3)
    assert quote_price("buy", 100.0, 101.0, 0.003) == pytest.approx(99.7)


@pytest.mark.parametrize(
    ("side", "quote", "print_price", "inside", "since", "expected"),
    [
        # A sell above the maker ask, a print strictly through it.
        ("sell", 10.5, 10.6, False, 5.0, True),
        # A print at the quote does not fill it.
        ("sell", 10.5, 10.5, False, 5.0, False),
        # A sell at or inside the maker ask is not placed.
        ("sell", 10.2, 10.6, False, 5.0, False),
        ("sell", 10.2, 10.6, True, 5.0, True),
        # Cooling down after a fill.
        ("sell", 10.5, 10.6, False, 0.5, False),
        # A buy below the maker bid, a print strictly through it.
        ("buy", 9.5, 9.4, False, 5.0, True),
        ("buy", 9.5, 9.5, False, 5.0, False),
        ("buy", 10.0, 9.4, False, 5.0, False),
        ("buy", 10.0, 9.4, True, 5.0, True),
    ],
)
def test_fill_decision(side, quote, print_price, inside, since, expected):
    """A quote fills only if placed, printed through and not cooling down."""
    assert (
        fill_decision(side, quote, print_price, 10.0, 10.2, inside, since, 1.0)
        is expected
    )


def test_fill_base_is_capped_by_the_print():
    """A fill takes the quote size unless the print is smaller."""
    assert fill_base(10.0, 2.0, 100.0) == 5.0
    assert fill_base(10.0, 2.0, 3.0) == 3.0


def test_pnl_bps_nets_both_fees():
    """The result is the spread captured less the fee of each leg."""
    assert pnl_bps("sell", 100.0, 99.0, 0.0, 0.0) == pytest.approx(100.0)
    assert pnl_bps("buy", 100.0, 101.0, 0.0, 0.0) == pytest.approx(100.0)
    # 100 bps captured, 10 bps maker fee, 5 bps on a hedge at 99.
    expected = (100.0 * 0.999 - 99.0 * 1.0005) / 100.0 * 1e4
    assert pnl_bps("sell", 100.0, 99.0, 0.001, 0.0005) == pytest.approx(expected)


def test_bucket_bounds():
    """Hourly and daily buckets span one hour and one day."""
    assert bucket_bounds("2026-09-20T14") == (T0, T0 + 3600 * NS)
    start, end = bucket_bounds("2026-09-20") or (0, 0)
    assert end - start == 86400 * NS
    assert bucket_bounds("other") is None


def test_resolve_fees_reads_config_and_overrides():
    """Static rates come from config unless overridden on the command line."""
    config = (
        VenueConfig(id=MAKER, name="A", maker_fee=0.001, taker_fee=0.002),
        VenueConfig(id=TAKER, name="B", maker_fee=0.0, taker_fee=0.0005),
    )
    assert resolve_fees(config, MAKER, TAKER, None, None) == (0.001, 0.0005)
    assert resolve_fees(config, MAKER, TAKER, 2.0, 7.0) == (0.0002, 0.0007)
    with pytest.raises(ValueError, match="maker fee"):
        resolve_fees(config, "venue_c", TAKER, None, None)


# End to end -----------------------------------------------------------------


def book(venue: str, t: float, bid: float, ask: float, size: float = 100.0) -> dict:
    """Build a recorder line for a book event at ``t`` seconds after T0."""
    return {
        "id": f"{int(t * 1000)}-0",
        "type": "book",
        "data": {
            "type": "book",
            "venue": venue,
            "symbol": SYMBOL,
            "seq": 1,
            "ts_exch": None,
            "bids": [[bid, size], [bid - 0.1, size]],
            "asks": [[ask, size], [ask + 0.1, size]],
            "ts_recv": T0 + int(t * NS),
            "v": 1,
        },
    }


def trade(venue: str, t: float, side: str, price: float, amount: float) -> dict:
    """Build a recorder line for a trade event at ``t`` seconds after T0."""
    return {
        "id": f"{int(t * 1000)}-1",
        "type": "trade",
        "data": {
            "type": "trade",
            "venue": venue,
            "symbol": SYMBOL,
            "seq": 1,
            "ts_exch": None,
            "trade_id": None,
            "side": side,
            "price": price,
            "amount": amount,
            "ts_recv": T0 + int(t * NS),
            "v": 1,
        },
    }


def write(root: Path, stream: str, records: list[dict]) -> None:
    """Write records to the hourly file of a stream, plus a torn last line."""
    directory = stream_path(root, stream)
    directory.mkdir(parents=True, exist_ok=True)
    with open(directory / "2026-09-20T14.jsonl", "w") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")
        fh.write('{"id": "torn')


@pytest.fixture
def recording(tmp_path: Path) -> Path:
    """
    Write a recording with one sell fill, one buy fill and several misses.

    The taker book sits at 10.0/10.1 and moves to 9.9/10.0 at 2.0 s. The
    maker book sits at 9.95/10.05, then crosses itself at 6.0 s.
    """
    write(
        tmp_path,
        book_stream(TAKER, SYMBOL),
        [book(TAKER, 0.0, 10.0, 10.1), book(TAKER, 2.0, 9.9, 10.0)],
    )
    write(
        tmp_path,
        book_stream(MAKER, SYMBOL),
        [book(MAKER, 0.0, 9.95, 10.05), book(MAKER, 6.0, 9.7, 9.6)],
    )
    write(
        tmp_path,
        trade_stream(MAKER, SYMBOL),
        [
            # Too early: no taker book is qlat old yet.
            trade(MAKER, 0.1, "buy", 11.0, 50.0),
            # Through the 1% sell at 10.201: fills, hedged at 1.5 s.
            trade(MAKER, 1.0, "buy", 10.25, 50.0),
            # Through again but cooling down.
            trade(MAKER, 1.5, "buy", 10.25, 50.0),
            # Priced from the 2.0 s book: buy at 9.801, print above it.
            trade(MAKER, 3.0, "sell", 9.85, 50.0),
            # Through the buy, smaller than the size: fills 0.5 base.
            trade(MAKER, 4.0, "sell", 9.7, 0.5),
            # Fills: the self-crossed maker book of 6.0 s, whose bid would
            # put the buy inside, is ignored.
            trade(MAKER, 7.0, "sell", 9.7, 1.0),
        ],
    )
    write(tmp_path, trade_stream(TAKER, SYMBOL), [trade(TAKER, 8.0, "buy", 10, 1)])
    return tmp_path


def test_simulate_end_to_end(recording: Path):
    """Fills, sizes and results match the arithmetic of the recording."""
    params = Params(
        edges_bps=(100.0,),
        maker_fee=0.001,
        taker_fee=0.0005,
        qlat_s=0.3,
        hlat_s=0.5,
        size=20.0,
        cool_s=1.0,
    )
    events = merged_events(recording, SYMBOL, MAKER, TAKER, T0, T0 + 3600 * NS)
    result = simulate(events, MAKER, TAKER, params)
    assert result.hours == pytest.approx(8.0 / 3600)
    fills = [(f.ts_ns, f.side) for f in result.fills]
    assert fills == [
        (T0 + 1 * NS, "sell"),
        (T0 + 4 * NS, "buy"),
        (T0 + 7 * NS, "buy"),
    ]
    sell, buy, last = result.fills
    assert sell.price == pytest.approx(10.201)
    assert sell.base == pytest.approx(20.0 / 10.201)
    # Hedged at 1.5 s against the first taker book: buy at its ask.
    assert sell.hedge_price == pytest.approx(10.1)
    assert sell.bps == pytest.approx(pnl_bps("sell", 10.201, 10.1, 0.001, 0.0005))
    assert buy.price == pytest.approx(9.801)
    assert buy.base == pytest.approx(0.5)
    # Hedged at 4.5 s against the second taker book: sell at its bid.
    assert buy.hedge_price == pytest.approx(9.9)
    assert buy.bps == pytest.approx(pnl_bps("buy", 9.801, 9.9, 0.001, 0.0005))
    assert last.base == pytest.approx(1.0)

    table = format_table(result, params)
    assert " 100 sell" in table and " 100 buy" in table
    hourly = format_hourly(result, params)
    assert "2026-09-20T14" in hourly


def test_simulate_counts_thin_hedges():
    """A hedge larger than the taker book is counted thin, not valued."""
    lines = [
        book(TAKER, 0.0, 10.0, 10.1),
        book(MAKER, 0.0, 9.95, 10.05),
        trade(MAKER, 1.0, "buy", 10.25, 500.0),
        trade(TAKER, 2.0, "buy", 10.1, 1.0),
    ]
    events = [decode(json.dumps(line["data"])) for line in lines]
    params = Params(edges_bps=(100.0,), maker_fee=0.0, taker_fee=0.0, size=1e9)
    result = simulate(events, MAKER, TAKER, params)
    (fill,) = result.fills
    assert fill.base == pytest.approx(500.0)
    assert fill.hedge_price is None and fill.bps is None
    assert "0.0" in format_table(result, params)


def test_end_drops_later_events_and_unsettled_hedges(recording: Path):
    """Events after the end are not read, and a hedge not yet due is dropped."""
    events = merged_events(recording, SYMBOL, MAKER, TAKER, T0, T0 + 4 * NS + 1)
    result = simulate(events, MAKER, TAKER, Params((100.0,), 0.0, 0.0))
    assert [f.ts_ns for f in result.fills] == [T0 + 1 * NS]


def test_main_prints_tables(recording: Path, capsys: pytest.CaptureFixture[str]):
    """The command line runs with fee overrides and prints both tables."""
    main(
        [
            SYMBOL,
            MAKER,
            TAKER,
            "--root",
            str(recording),
            "--edges",
            "100",
            "--maker-fee-bps",
            "10",
            "--taker-fee-bps",
            "5",
            "--hourly",
        ]
    )
    out = capsys.readouterr().out
    assert "maker_fee=10bps taker_fee=5bps" in out
    assert "2026-09-20T14" in out


@pytest.mark.parametrize(
    ("side", "print_price", "at_price", "expected"),
    [
        ("buy", 9.9, False, False),
        ("buy", 9.9, True, True),
        ("sell", 10.3, False, False),
        ("sell", 10.3, True, True),
    ],
)
def test_fill_decision_at_price(side, print_price, at_price, expected):
    """A print at the quote's own price fills it only when asked to."""
    quote = 9.9 if side == "buy" else 10.3
    assert (
        fill_decision(side, quote, print_price, 10.0, 10.2, False, 5.0, 1.0, at_price)
        is expected
    )


def test_min_tick_takes_the_smallest_level_gap():
    """The tick estimate narrows to the smallest gap between adjacent levels."""
    tick = min_tick(None, [(1.0, 1.0), (0.98, 1.0)], [(1.01, 1.0), (1.03, 1.0)])
    assert tick == pytest.approx(0.02)
    tick = min_tick(tick, [(1.0, 1.0), (0.995, 1.0)], [])
    assert tick == pytest.approx(0.005)
    assert min_tick(None, [(1.0, 1.0)], []) is None


def test_summarize_earns_on_the_base_each_fill_took():
    """A fill a small print only partly took earns on that part, not the size."""
    full = Fill(0, 30.0, "sell", 1.0, 10.0, 0.99, 100.0)
    partial = Fill(1, 30.0, "sell", 1.0, 1.0, 0.99, 100.0)
    summary = summarize([full, partial], hours=1.0, size=10.0)
    assert summary is not None
    assert summary.quote_per_h == pytest.approx(0.11)
