"""Tests for the market screener's pure arithmetic."""

import pytest

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


def test_a_market_crossed_most_of_the_time_is_marked_suspect():
    """Two assets under one ticker look like a huge edge; the table flags them."""
    from apps.maker.src.tools.market_screener import format_table

    maker = Sample(snapshots=snapshots([(2.0, 2.1), (2.0, 2.1)]))
    taker = Sample(snapshots=snapshots([(1.0, 1.1), (1.0, 1.1)]))
    score = score_pair("XXX/USDT", "a", "b", maker, taker, 12.0)
    assert score is not None and score.suspect
    assert format_table([score], 5).splitlines()[2].startswith("!XXX/USDT")


def test_perpetual_markets_keeps_active_linear_swaps_settled_in_the_quote():
    """A perpetual is a linear swap settled in the quote; the rest is ignored."""
    from apps.maker.src.tools.market_screener import perpetual_markets

    markets = {
        "AAA/USDT": {"spot": True, "base": "AAA", "quote": "USDT"},
        "AAA/USDT:USDT": {
            "swap": True,
            "linear": True,
            "active": True,
            "base": "AAA",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": 10,
        },
        "BBB/USDT:USDT": {
            "swap": True,
            "linear": True,
            "active": False,
            "base": "BBB",
            "quote": "USDT",
            "settle": "USDT",
        },
        "CCC/USD:CCC": {
            "swap": True,
            "linear": False,
            "inverse": True,
            "base": "CCC",
            "quote": "USD",
            "settle": "CCC",
        },
        "DDD/USDT:USDT": {
            "swap": True,
            "linear": True,
            "base": "DDD",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": None,
        },
    }
    assert perpetual_markets(markets, "USDT") == {
        "AAA": ("AAA/USDT:USDT", 10.0),
        "DDD": ("DDD/USDT:USDT", None),
    }
    assert perpetual_markets(markets, "USDC") == {}


def hedge_sample(venue: str, unified: bool, depth: float = 100.0):
    """Build a perpetual with one snapshot at parity and the given depth."""
    from apps.maker.src.tools.market_screener import HedgeSample

    return HedgeSample(
        venue=venue,
        symbol="AAA/USDT:USDT",
        unified=unified,
        taker_fee_bps=5.0,
        contract_size=1.0,
        snapshots=[
            Snapshot(ts_ms=0, bid=1.0, ask=1.0, bid_depth=depth, ask_depth=depth)
        ],
    )


def test_choose_hedge_prefers_a_unified_account_then_depth():
    """A unified account needs no transfer, so it wins over a deeper split wallet."""
    from apps.maker.src.tools.market_screener import choose_hedge

    split_deep = hedge_sample("split", unified=False, depth=1_000.0)
    unified_thin = hedge_sample("unified", unified=True, depth=10.0)
    unified_deep = hedge_sample("unified2", unified=True, depth=50.0)
    assert choose_hedge([split_deep, unified_thin]) is unified_thin
    assert choose_hedge([unified_thin, unified_deep]) is unified_deep
    assert choose_hedge([split_deep]) is split_deep
    assert choose_hedge([]) is None


def test_score_hedge_prices_basis_funding_and_the_round_trip():
    """Basis is the perp bid over the spot bid; funding sums the trailing week."""
    from apps.maker.src.tools.market_screener import HedgeSample, score_hedge
    from apps.shared.src.funding import HOUR_MS, WEEK_MS, FundingRate

    now = 10 * WEEK_MS
    # Spot bid 1.0000 throughout; the perp bids 1.0020: a 20 bps premium.
    spot = Sample(
        snapshots=[
            Snapshot(ts_ms=now - 2000 + k, bid=1.0, ask=1.001, bid_depth=1, ask_depth=1)
            for k in (0, 1000)
        ]
    )
    hedge = HedgeSample(
        venue="perp",
        symbol="AAA/USDT:USDT",
        unified=True,
        taker_fee_bps=5.0,
        contract_size=1.0,
        snapshots=[
            Snapshot(
                ts_ms=now - 2000 + k, bid=1.002, ask=1.003, bid_depth=200, ask_depth=400
            )
            for k in (0, 1000)
        ],
        funding=FundingRate(
            rate=0.0001,
            interval_s=28_800,
            mark_price=1.0,
            index_price=1.0,
            next_funding_ms=now,
        ),
        # Three payments inside the week and one just before it.
        history=[
            (now - WEEK_MS - HOUR_MS, 0.5),
            (now - 3 * 8 * HOUR_MS, 0.0002),
            (now - 2 * 8 * HOUR_MS, 0.0003),
            (now - 8 * HOUR_MS, -0.0001),
        ],
    )
    score = score_hedge(spot, hedge, spot_maker_fee_bps=2.0, now_ms=now)
    assert score is not None
    assert score.venue == "perp" and score.unified
    assert round(score.basis_bps, 6) == 20.0
    assert round(score.perp_spread_bps, 3) == round((1.003 / 1.002 - 1) * 1e4, 3)
    assert score.perp_depth == 300.0
    assert score.funding_rate_bps == 1.0
    assert score.interval_s == 28_800
    assert score.funding_day_bps == pytest.approx(3.0)
    assert round(score.funding_week_bps, 6) == 4.0
    assert score.funding_hours == 7 * 24 - 7.0  # the pre-week entry is in the span
    assert round(score.entry_bps, 6) == 20.0 - 7.0
    assert round(score.round_trip_bps, 6) == 4.0 - 14.0


def test_score_hedge_infers_the_interval_and_needs_overlapping_books():
    """Without a reported interval the history's spacing is used; no overlap is None."""
    from apps.maker.src.tools.market_screener import HedgeSample, score_hedge
    from apps.shared.src.funding import HOUR_MS, FundingRate

    now = 100 * HOUR_MS
    spot = Sample(snapshots=snapshots([(1.0, 1.001), (1.0, 1.001)]))
    hedge = HedgeSample(
        venue="perp",
        symbol="AAA/USDT:USDT",
        unified=False,
        taker_fee_bps=5.0,
        contract_size=None,
        snapshots=snapshots([(1.0, 1.001)]),
        funding=FundingRate(
            rate=0.0001,
            interval_s=None,
            mark_price=None,
            index_price=None,
            next_funding_ms=None,
        ),
        history=[(now - 8 * HOUR_MS, 0.0), (now - 4 * HOUR_MS, 0.0), (now, 0.0)],
    )
    score = score_hedge(spot, hedge, 0.0, now)
    assert score is not None
    assert score.interval_s == 4 * 3600
    assert score.funding_day_bps == pytest.approx(6.0)
    far = HedgeSample(
        venue="perp",
        symbol="AAA/USDT:USDT",
        unified=False,
        taker_fee_bps=5.0,
        contract_size=None,
        snapshots=[Snapshot(ts_ms=10**9, bid=1.0, ask=1.0, bid_depth=1, ask_depth=1)],
    )
    assert score_hedge(spot, far, 0.0, now) is None
    assert score_hedge(spot, hedge_sample("x", False), 0.0, now) is not None
    empty = hedge_sample("x", False)
    empty.snapshots = []
    assert score_hedge(spot, empty, 0.0, now) is None


def test_format_table_renders_the_perpetual_columns():
    """A hedged row shows the perp venue, starred on a unified account; others a dash."""
    from dataclasses import replace

    from apps.maker.src.tools.market_screener import HedgeScore, format_table

    maker = Sample(snapshots=snapshots([(1.0, 1.1), (1.0, 1.1)]))
    taker = Sample(snapshots=snapshots([(1.0, 1.1), (1.0, 1.1)]))
    bare = score_pair("AAA/USDT", "a", "b", maker, taker, 12.0)
    assert bare is not None
    hedged = replace(
        bare,
        symbol="BBB/USDT",
        hedge=HedgeScore(
            venue="unified",
            symbol="BBB/USDT:USDT",
            unified=True,
            contract_size=1.0,
            basis_bps=12.5,
            perp_spread_bps=3.0,
            perp_depth=1000.0,
            funding_rate_bps=1.0,
            interval_s=28_800,
            funding_day_bps=3.0,
            funding_week_bps=21.0,
            funding_hours=168.0,
            entry_bps=5.5,
            round_trip_bps=7.0,
        ),
    )
    lines = format_table([hedged, bare], 5).splitlines()
    assert lines[0].rstrip().endswith("perp  basis    f/d   f/wk  rt/wk")
    assert lines[2].rstrip().endswith("*unified   12.5    3.0   21.0    7.0")
    assert lines[3].rstrip().endswith("-")


class FakeClient:
    """A CCXT stand-in serving fixed books, funding and history."""

    def __init__(self, has: dict[str, bool], funding=None, history=None):
        """Remember what to serve."""
        self.has = has
        self.funding = funding or {}
        self.history = history or []
        self.calls: list[tuple] = []

    async def fetch_order_book(self, symbol, limit=None):
        """Serve a one-level book at parity."""
        self.calls.append(("book", symbol))
        return {"bids": [[1.0, 10.0]], "asks": [[1.001, 10.0]]}

    async def fetch_trades(self, symbol, limit=None):
        """Serve no trades."""
        self.calls.append(("trades", symbol))
        return []

    async def fetch_funding_rate(self, symbol):
        """Serve the funding rate."""
        self.calls.append(("funding", symbol))
        return self.funding

    async def fetch_funding_rate_history(self, symbol, since=None, limit=None):
        """Serve the history."""
        self.calls.append(("history", symbol, since, limit))
        return self.history


@pytest.mark.asyncio
async def test_screener_samples_hedges_reads_funding_and_attaches_the_score():
    """The perp's book is sampled each round, funding read once, score attached."""
    from apps.maker.src.tools.market_screener import HedgeSample, Screener
    from apps.shared.src.funding import HOUR_MS

    spot_a, spot_b = FakeClient({}), FakeClient({})
    perp = FakeClient(
        {"fetchFundingRate": True, "fetchFundingRateHistory": True},
        funding={"fundingRate": 0.0001, "interval": "8h"},
        history=[{"timestamp": 8 * HOUR_MS, "fundingRate": 0.0002}],
    )
    screener = Screener({"a": spot_a, "b": spot_b, "p": perp}, spot_venues=["a", "b"])
    screener.add_hedge(
        "AAA",
        HedgeSample(
            "p", "AAA/USDT:USDT", unified=True, taker_fee_bps=5.0, contract_size=1.0
        ),
    )
    screener.add_hedge(
        "ZZZ",
        HedgeSample(
            "p", "ZZZ/USDT:USDT", unified=False, taker_fee_bps=5.0, contract_size=1.0
        ),
    )
    assert screener.hedged("AAA/USDT") and screener.hedged(
        "AAA/USDT", unified_only=True
    )
    assert screener.hedged("ZZZ/USDT") and not screener.hedged(
        "ZZZ/USDT", unified_only=True
    )
    assert not screener.hedged("BBB/USDT")
    screener.keep_hedges_for(["AAA/USDT"])
    assert list(screener.hedges) == ["AAA"]

    now = 24 * HOUR_MS
    await screener.prime_hedges(now)
    await screener.collect(["AAA/USDT"], seconds=0.0, interval=0.0)
    await screener.collect(["AAA/USDT"], seconds=0.0, interval=0.0)
    hedge = screener.hedges["AAA"][0]
    assert hedge.funding is not None and hedge.funding.interval_s == 28_800
    assert hedge.history == [(8 * HOUR_MS, 0.0002)]
    assert len(hedge.snapshots) == 2
    assert [c[0] for c in perp.calls] == ["funding", "history", "book", "book"]
    assert ("trades", "AAA/USDT:USDT") not in perp.calls
    assert perp.calls[1][2] == now - 7 * 24 * HOUR_MS

    scores = screener.scores(
        ["AAA/USDT"], {("a", "b"): 12.0, ("b", "a"): 12.0}, {"a": 2.0, "b": 2.0}, now
    )
    assert len(scores) == 2
    for score in scores:
        assert score.hedge is not None
        assert score.hedge.venue == "p" and score.hedge.unified
        assert round(score.hedge.basis_bps, 6) == 0.0
        assert score.hedge.funding_week_bps == 2.0
        assert round(score.hedge.round_trip_bps, 6) == 2.0 - 14.0
    # Without maker fees no hedge is scored; without candidates none either.
    assert (
        screener.scores(["AAA/USDT"], {("a", "b"): 12.0, ("b", "a"): 12.0})[0].hedge
        is None
    )


@pytest.mark.asyncio
async def test_prime_hedge_respects_the_has_map_and_survives_a_failure():
    """A venue without the endpoint is not asked; one that fails leaves the field empty."""
    from apps.maker.src.tools.market_screener import HedgeSample, Screener

    class Failing(FakeClient):
        async def fetch_funding_rate(self, symbol):
            raise RuntimeError("down")

    quiet = FakeClient({"fetchFundingRate": False, "fetchFundingRateHistory": False})
    failing = Failing(
        {"fetchFundingRate": True, "fetchFundingRateHistory": True},
        history=[{"timestamp": 1, "fundingRate": 0.1}],
    )
    screener = Screener({"q": quiet, "f": failing}, spot_venues=[])
    q = HedgeSample("q", "AAA/USDT:USDT", False, 5.0, None)
    f = HedgeSample("f", "AAA/USDT:USDT", False, 5.0, None)
    screener.add_hedge("AAA", q)
    screener.add_hedge("AAA", f)
    await screener.prime_hedges(0)
    assert quiet.calls == []
    assert q.funding is None and q.history == []
    assert f.funding is None and f.history == [(1, 0.1)]


def test_public_clients_build_from_the_exchange_class_with_client_options(
    monkeypatch,
):
    """A futures wallet entry builds its exchange class addressed at the swap endpoints."""
    from types import SimpleNamespace

    from apps.maker.src.tools.market_screener import public_clients

    class Recorder:
        def __init__(self, params):
            self.params = params

    import apps.maker.src.tools.market_screener as module

    class FakeCcxt:
        gate = Recorder

    monkeypatch.setattr(module, "ccxt", FakeCcxt)
    config = SimpleNamespace(
        venues=(
            VenueConfig(id="gate", name="spot", options={"x": 1}),
            VenueConfig(id="gateperp", name="perp", ccxt_id="gate", market_type="swap"),
        )
    )
    clients = public_clients(config, ["gate", "gateperp"], rate_limit_ms=50)
    assert clients["gate"].params == {
        "enableRateLimit": True,
        "rateLimit": 50,
        "options": {"x": 1},
    }
    assert clients["gateperp"].params == {
        "enableRateLimit": True,
        "rateLimit": 50,
        "options": {"defaultType": "swap"},
    }
