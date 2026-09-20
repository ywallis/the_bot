"""Tests for the realized PnL report."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from apps.maker.src.tools import pnl
from apps.maker.src.tools.pnl import (
    FeeRate,
    Leg,
    Pair,
    fold_legs,
    pair_legs,
    value_pair,
)
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    OmsConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
    VenueConfig,
)
from apps.shared.src.events import (
    ORDER_EVENTS_STREAM,
    Fill,
    OrderEvent,
    OrderState,
    Side,
    to_stream_fields,
)
from apps.shared.src.streams import stream_path

TS = 1_757_160_000_000_000_000


def event(
    venue: str,
    intent_id: str,
    side: Side,
    filled: str,
    avg_price: str,
    ts: int = TS,
    state: OrderState = OrderState.FILLED,
    fee: str | None = None,
    fee_currency: str | None = None,
    strategy_id: str = "fmb",
) -> OrderEvent:
    """Return an order event that reports a fill."""
    return OrderEvent(
        ts_recv=ts,
        intent_id=intent_id,
        strategy=f"{strategy_id}_es",
        venue=venue,
        symbol="BASE/QUOTE",
        state=state,
        side=side,
        filled=Decimal(filled),
        remaining=Decimal(0),
        avg_price=Decimal(avg_price),
        last_fill=Fill(
            price=Decimal(avg_price),
            amount=Decimal(filled),
            fee=Decimal(fee) if fee is not None else None,
            fee_currency=fee_currency,
        ),
        tags={"strategy_id": strategy_id, "order_id": "es"},
    )


def leg(venue: str, side: Side, filled: str, price: str, ts: int = TS) -> Leg:
    """Return a folded leg."""
    return Leg(
        venue=venue,
        intent_id="t-1_fmb_es",
        symbol="BASE/QUOTE",
        side=side,
        strategy_id="fmb",
        filled=Decimal(filled),
        avg_price=Decimal(price),
        first_fill_ns=ts,
    )


def test_fold_legs_keeps_the_largest_fill_and_counts_each_fee_once():
    """The order manager and the watcher report the same fill; it is one fill."""
    events = [
        event("venue_a", "t-1_fmb_es", Side.SELL, "0", "0", state=OrderState.OPEN),
        event(
            "venue_a",
            "t-1_fmb_es",
            Side.SELL,
            "100",
            "2.0",
            fee="0.1",
            fee_currency="QUOTE",
        ),
        event(
            "venue_a",
            "t-1_fmb_es",
            Side.SELL,
            "100",
            "2.0",
            fee="0.1",
            fee_currency="QUOTE",
        ),
        event(
            "venue_a",
            "t-1_fmb_es",
            Side.SELL,
            "150",
            "2.0",
            fee="0.05",
            fee_currency="QUOTE",
        ),
    ]
    legs = fold_legs(events)
    assert set(legs) == {("venue_a", "t-1_fmb_es")}
    folded = legs[("venue_a", "t-1_fmb_es")]
    assert folded.filled == Decimal(150)
    assert folded.fee == Decimal("0.15")
    assert folded.fee_currency == "QUOTE"
    assert folded.first_fill_ns == TS


def test_value_pair_sell_hedged_with_a_buy_nets_the_gap_minus_fees():
    """Sell 100 at 2.006, buy back 100 at 2.0, pay 10 bps on the hedge in base."""
    maker = leg("venue_a", Side.SELL, "100", "2.006")
    hedge = leg("venue_b", Side.BUY, "100", "2.0")
    pair = value_pair(
        maker,
        hedge,
        FeeRate(Decimal(0), False, "config"),
        FeeRate(Decimal("0.001"), True, "config"),
    )
    assert round(pair.gross_bps or 0, 6) == 30.0
    # Cash: +200.6 - 200 = 0.6; base: -100 + 100 - 0.1 = -0.1, marked at 2.0.
    assert round(pair.base_residual, 9) == -0.1
    assert round(pair.fees_quote, 9) == 0.2
    assert round(pair.pnl_quote, 9) == round(0.6 - 0.2, 9)


def test_value_pair_uses_reported_fees_over_the_schedule():
    """A fill that says what it paid is charged that, whatever the rate says."""
    maker = leg("venue_a", Side.BUY, "100", "1.0")
    maker.fee, maker.fee_currency = Decimal("0.5"), "QUOTE"
    hedge = leg("venue_b", Side.SELL, "100", "1.01")
    hedge.fee, hedge.fee_currency = Decimal("0.2"), "QUOTE"
    pair = value_pair(
        maker,
        hedge,
        FeeRate(Decimal("0.5"), False, "config"),
        FeeRate(Decimal("0.5"), False, "config"),
    )
    assert round(pair.pnl_quote, 9) == round(101 - 100 - 0.5 - 0.2, 9)
    assert round(pair.gross_bps or 0, 6) == round((1.01 / 1.0 - 1) * 1e4, 6)


def test_value_pair_without_a_hedge_marks_the_position_at_the_fill():
    """An unhedged fill is worth its residual at its own price, minus fees."""
    maker = leg("venue_a", Side.SELL, "100", "2.0")
    pair = value_pair(
        maker, None, FeeRate(None, False, "none"), FeeRate(None, False, "none")
    )
    assert pair.hedge_venue is None
    assert pair.gross_bps is None
    assert round(pair.base_residual, 9) == -100.0
    assert round(pair.pnl_quote, 9) == 0.0


def test_pair_legs_matches_by_intent_id_across_venues():
    """The hedge is the leg with the same client id on the strategy's taker venue."""
    maker = leg("venue_a", Side.SELL, "100", "2.0")
    hedge = leg("venue_b", Side.BUY, "100", "1.99", ts=TS + 1)
    stranger = leg("venue_b", Side.BUY, "5", "1.0")
    stranger.intent_id = "t-2_fmb_es"
    legs = {
        ("venue_a", maker.intent_id): maker,
        ("venue_b", hedge.intent_id): hedge,
        ("venue_b", stranger.intent_id): stranger,
    }
    pairs = pair_legs(legs, {"fmb": ("venue_a", "venue_b")})
    assert [(m.venue, h.venue if h else None) for m, h in pairs] == [
        ("venue_a", "venue_b")
    ]
    # Without config the earlier fill is the maker, the later one its hedge.
    pairs = pair_legs(legs, {})
    assert [(m.venue, h.venue if h else None) for m, h in pairs] == [
        ("venue_a", "venue_b"),
        ("venue_b", None),
    ]


def config_for(tmp_path: Path) -> AppConfig:
    """Return a config with one hedging strategy and static fees."""
    return AppConfig(
        refresh_speed=0.01,
        redis=RedisConfig(host="localhost", port=6379),
        market_data=MarketDataConfig(book_depth=20, stream_maxlen=1000),
        oms=OmsConfig(),
        venues=(
            VenueConfig(id="venue_a", name="A", maker_fee=0.0, taker_fee=0.0005),
            VenueConfig(
                id="venue_b",
                name="B",
                maker_fee=0.001,
                taker_fee=0.001,
                fee_currency="received",
            ),
        ),
        strategies=(
            StrategyConfig(
                identifier="fmb",
                type="fake_maker",
                production=False,
                subscriptions=(Subscription(venue="venue_a", symbol="BASE/QUOTE"),),
                params={
                    "should_match": True,
                    "maker_exchange": "venue_a",
                    "taker_exchange": "venue_b",
                },
            ),
        ),
    )


def write_recording(root: Path, events: list[OrderEvent]) -> None:
    """Write events the way the recorder does, one hourly bucket."""
    directory = stream_path(root, ORDER_EVENTS_STREAM)
    directory.mkdir(parents=True)
    with open(directory / "2026-09-08T14.jsonl", "w") as fh:
        for i, ev in enumerate(events):
            fields = to_stream_fields(ev)
            data = fields["data"]
            fh.write(
                json.dumps(
                    {
                        "id": f"{ev.ts_recv // 1_000_000}-{i}",
                        "type": fields["type"],
                        "data": json.loads(
                            data if isinstance(data, str) else data.decode()
                        ),
                    }
                )
                + "\n"
            )


def test_realized_pnl_end_to_end(tmp_path: Path):
    """From recorder files to a valued pair, with config fees for the taker leg."""
    write_recording(
        tmp_path,
        [
            event("venue_a", "t-1_fmb_es", Side.SELL, "100", "2.006", ts=TS),
            event("venue_b", "t-1_fmb_es", Side.BUY, "100", "2.0", ts=TS + 10**9),
            # A fill outside the range must not count.
            event("venue_a", "t-9_fmb_es", Side.SELL, "100", "3.0", ts=TS + 10**12),
        ],
    )
    pairs, sources = pnl.realized_pnl(
        config_for(tmp_path), tmp_path, TS, TS + 10**11, None, None
    )
    assert len(pairs) == 1
    pair: Pair = pairs[0]
    assert pair.hedge_venue == "venue_b"
    # 30 bps gross on 200 quote units is 0.6; the hedge buys 100 base and
    # pays 10 bps of it in base under the "received" policy: 0.1 base at 2.0.
    assert round(pair.pnl_quote, 9) == round(0.6 - 0.2, 9)
    assert sources == {"venue_a": "config", "venue_b": "config"}
    summary = pnl.summarize(pairs)
    assert summary["pairs"] == 1 and summary["unhedged"] == 0
    hour = datetime.fromtimestamp(TS / 1e9, UTC).strftime("%Y-%m-%dT%H")
    assert list(summary["hourly"]) == [hour]
    report = pnl.format_report(pairs, summary, sources)
    assert "realized pnl +0.4000" in report

    pairs, _ = pnl.realized_pnl(
        config_for(tmp_path), tmp_path, TS, TS + 10**11, 5.0, 20.0
    )
    assert round(pairs[0].fees_quote, 6) == round(200.6 * 0.0005 + 100 * 0.002 * 2.0, 6)
