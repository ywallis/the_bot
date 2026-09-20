"""Tests for the market screener's pure arithmetic."""

from apps.maker.src.tools.market_screener import (
    EDGES_BPS,
    Print,
    Sample,
    Snapshot,
    add_prints,
    common_symbols,
    fee_budget_bps,
    nearest_snapshot,
    rank,
    score_pair,
    select_symbols,
    snapshot_from_book,
)
from apps.shared.src.config import VenueConfig


def snapshots(prices: list[tuple[float, float]], step_ms: int = 1000) -> list[Snapshot]:
    """Build one snapshot per (bid, ask), one step apart."""
    return [
        Snapshot(ts_ms=i * step_ms, bid=bid, ask=ask, bid_depth=100.0, ask_depth=100.0)
        for i, (bid, ask) in enumerate(prices)
    ]


def test_common_symbols_keeps_active_spot_markets_of_the_quote():
    """Only symbols every venue lists, spot, active and in the quote, survive."""
    markets = {
        "venue_a": {
            "AAA/USDT": {"spot": True, "active": True, "quote": "USDT"},
            "BBB/USDT": {"spot": True, "active": True, "quote": "USDT"},
            "CCC/USDT": {"spot": True, "active": False, "quote": "USDT"},
            "AAA/USDC": {"spot": True, "active": True, "quote": "USDC"},
        },
        "venue_b": {
            "AAA/USDT": {"spot": True, "active": True, "quote": "USDT"},
            "BBB/USDT": {"spot": False, "active": True, "quote": "USDT"},
            "CCC/USDT": {"spot": True, "active": True, "quote": "USDT"},
        },
    }
    assert common_symbols(markets, "USDT") == ["AAA/USDT"]
    assert common_symbols({}, "USDT") == []


def test_add_prints_deduplicates_across_fetches():
    """A trade seen twice, by id or by its figures, is kept once."""
    sample = Sample()
    first = [
        {"id": "1", "timestamp": 1000, "price": 1.0, "amount": 2.0, "side": "buy"},
        {"id": None, "timestamp": 1001, "price": 1.1, "amount": 3.0, "side": "sell"},
    ]
    assert add_prints(sample, first) == 2
    again = first + [
        {"id": "2", "timestamp": 999, "price": 1.0, "amount": 1.0, "side": "buy"}
    ]
    assert add_prints(sample, again) == 1
    assert [p.ts_ms for p in sample.prints] == [999, 1000, 1001]


def test_snapshot_from_book_sums_near_depth_and_rejects_one_sided_books():
    """The depth figure is quote notional over the first levels."""
    book = {"bids": [[10.0, 1.0], [9.0, 2.0]], "asks": [[11.0, 1.0]]}
    snapshot = snapshot_from_book(book, 5)
    assert snapshot is not None
    assert snapshot.bid == 10.0 and snapshot.ask == 11.0
    assert snapshot.bid_depth == 10.0 + 18.0
    assert snapshot.ask_depth == 11.0
    assert snapshot_from_book({"bids": [], "asks": [[1.0, 1.0]]}, 5) is None


def test_nearest_snapshot_picks_the_closest_within_the_gap():
    """The closest snapshot either side wins; a far one is not used."""
    snaps = snapshots([(1.0, 1.1), (2.0, 2.1), (3.0, 3.1)], step_ms=10_000)
    assert nearest_snapshot(snaps, 4_000) is snaps[0]
    assert nearest_snapshot(snaps, 6_000) is snaps[1]
    assert nearest_snapshot(snaps, 200_000) is None
    assert nearest_snapshot([], 0) is None


def test_score_pair_measures_offset_and_harvestable_volume():
    """Prints past the taker touch by more than fee plus edge count as harvest."""
    # Maker asks sit 20 bps above the taker's asks throughout the hour.
    maker = Sample(
        snapshots=[
            Snapshot(ts_ms=t, bid=0.9990, ask=1.0020, bid_depth=10, ask_depth=10)
            for t in (0, 3_600_000)
        ]
    )
    taker = Sample(
        snapshots=[
            Snapshot(ts_ms=t, bid=1.0000, ask=1.0000, bid_depth=50, ask_depth=150)
            for t in (0, 3_600_000)
        ]
    )
    # One buy print 45 bps over the taker ask, one 8 bps over, one sell print
    # 45 bps under the taker bid; 1000 quote units each.
    maker.prints = [
        Print(ts_ms=1_000, side="buy", price=1.0045, amount=1000 / 1.0045),
        Print(ts_ms=2_000, side="buy", price=1.0008, amount=1000 / 1.0008),
        Print(ts_ms=3_000, side="sell", price=1 / 1.0045, amount=1000 * 1.0045),
    ]
    score = score_pair("AAA/USDT", "venue_a", "venue_b", maker, taker, fee_bps=12.0)
    assert score is not None
    assert round(score.sell_offset_bps, 6) == 20.0
    assert round(score.buy_offset_bps, 3) == round((1.0000 / 0.9990 - 1) * 1e4, 3)
    assert round(score.maker_spread_bps, 3) == round((1.0020 / 0.9990 - 1) * 1e4, 3)
    assert score.taker_depth == 100.0
    assert round(score.hours, 6) == 1.0
    assert round(score.maker_volume_per_h) == 3000
    # 45 bps clears fee + 5, 10, 20, 30; 8 bps clears none.
    assert {e: round(v) for e, v in score.harvest_per_h.items()} == {
        5: 2000,
        10: 2000,
        20: 2000,
        30: 2000,
    }
    assert round(score.net_per_h[30], 6) == round(2000 * 30 / 10_000, 6)
    assert score.best_net_per_h == score.net_per_h[30]
    assert score.crossed_share == 0.0
    assert set(score.harvest_per_h) == set(EDGES_BPS)


def test_score_pair_counts_crossed_books_and_needs_two_snapshots():
    """A maker bid over the taker ask by more than the fee is a cross."""
    maker = Sample(snapshots=snapshots([(1.0100, 1.0110), (1.0000, 1.0010)]))
    taker = Sample(snapshots=snapshots([(0.9990, 1.0000), (0.9990, 1.0000)]))
    score = score_pair("AAA/USDT", "venue_a", "venue_b", maker, taker, fee_bps=12.0)
    assert score is not None
    assert score.crossed_share == 0.5
    assert score_pair("AAA/USDT", "a", "b", Sample(), taker, 12.0) is None


def test_rank_orders_by_net_then_volume():
    """The best net figure ranks first; volume breaks ties."""
    maker = Sample(snapshots=snapshots([(1.0, 1.1), (1.0, 1.1)]))
    taker = Sample(snapshots=snapshots([(1.0, 1.1), (1.0, 1.1)]))
    quiet = score_pair("AAA/USDT", "a", "b", maker, taker, 12.0)
    busy_maker = Sample(
        snapshots=maker.snapshots,
        prints=[Print(ts_ms=500, side="buy", price=1.1, amount=1.0)],
    )
    busy = score_pair("BBB/USDT", "a", "b", busy_maker, taker, 12.0)
    paid_maker = Sample(
        snapshots=maker.snapshots,
        prints=[Print(ts_ms=500, side="buy", price=1.2, amount=1.0)],
    )
    paid = score_pair("CCC/USDT", "a", "b", paid_maker, taker, 12.0)
    assert quiet and busy and paid
    assert [s.symbol for s in rank([quiet, busy, paid])] == [
        "CCC/USDT",
        "BBB/USDT",
        "AAA/USDT",
    ]


def test_fee_budget_uses_static_rates_when_both_venues_have_them():
    """Configured maker and taker rates add up; anything missing falls back."""
    maker = VenueConfig(id="a", name="A", maker_fee=0.0, taker_fee=0.0005)
    taker = VenueConfig(id="b", name="B", maker_fee=0.001, taker_fee=0.001)
    assert fee_budget_bps(maker, taker, 12.0) == 10.0
    assert fee_budget_bps(VenueConfig(id="c", name="C"), taker, 12.0) == 12.0
    assert fee_budget_bps(None, taker, 12.0) == 12.0


def test_select_symbols_keeps_the_liquid_ones_busiest_first():
    """A symbol is judged by its quietest venue and the busiest are kept."""
    volumes = {
        "a": {"AAA/USDT": 100.0, "BBB/USDT": 50_000.0, "CCC/USDT": 20_000.0},
        "b": {"AAA/USDT": 90_000.0, "BBB/USDT": 30_000.0, "CCC/USDT": 25_000.0},
    }
    symbols = ["AAA/USDT", "BBB/USDT", "CCC/USDT"]
    assert select_symbols(symbols, volumes, 10_000.0, 5) == ["BBB/USDT", "CCC/USDT"]
    assert select_symbols(symbols, volumes, 10_000.0, 1) == ["BBB/USDT"]


def test_score_recording_reads_books_and_trades_from_the_recorder_files(tmp_path):
    """The recorded mode builds samples from the recorder's files and scores them."""
    import json as _json

    from apps.shared.src.events import (
        BookEvent,
        Side,
        TradeEvent,
        book_stream,
        to_stream_fields,
        trade_stream,
    )
    from apps.shared.src.streams import stream_path

    from apps.maker.src.tools.market_screener import score_recording

    t0 = 1_757_200_000_000_000_000

    def write(stream: str, events: list) -> None:
        directory = stream_path(tmp_path, stream)
        directory.mkdir(parents=True)
        with open(directory / "2025-09-07T00.jsonl", "w") as fh:
            for i, ev in enumerate(events):
                fields = to_stream_fields(ev)
                data = fields["data"]
                fh.write(
                    _json.dumps(
                        {
                            "id": f"{ev.ts_recv // 1_000_000}-{i}",
                            "type": fields["type"],
                            "data": _json.loads(
                                data if isinstance(data, str) else data.decode()
                            ),
                        }
                    )
                    + "\n"
                )

    hour = 3_600 * 10**9
    for venue, bid, ask in (("a", 0.9990, 1.0020), ("b", 1.0000, 1.0000)):
        write(
            book_stream(venue, "X/USDT"),
            [
                BookEvent(
                    ts_recv=t0 + k * hour,
                    venue=venue,
                    symbol="X/USDT",
                    seq=k,
                    ts_exch=None,
                    bids=[[bid, 10.0]],
                    asks=[[ask, 10.0]],
                )
                for k in (0, 1)
            ],
        )
    write(
        trade_stream("a", "X/USDT"),
        [
            TradeEvent(
                ts_recv=t0 + 1000,
                venue="a",
                symbol="X/USDT",
                seq=1,
                ts_exch=None,
                trade_id="1",
                side=Side.BUY,
                price=1.0045,
                amount=1000 / 1.0045,
            )
        ],
    )
    scores = score_recording(
        tmp_path, ["a", "b"], ["X/USDT"], {("a", "b"): 12.0, ("b", "a"): 12.0}, 0, 2**63
    )
    assert [(s.maker, s.taker) for s in scores] == [("a", "b"), ("b", "a")]
    top = scores[0]
    assert round(top.sell_offset_bps, 6) == 20.0
    assert round(top.harvest_per_h[30]) == 1000
    assert round(top.hours, 6) == 1.0
