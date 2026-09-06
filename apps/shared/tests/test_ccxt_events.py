"""Tests for CCXT to event converters."""

from decimal import Decimal

import pytest

from apps.shared.src.ccxt_events import (
    balance_event_from_ccxt,
    book_event_from_ccxt,
    fill_from_ccxt,
    order_event_from_ccxt,
    order_state_from_ccxt,
    trade_event_from_ccxt,
)
from apps.shared.src.events import AssetBalance, Liquidity, OrderState, Side

TS = 1_757_160_000_000_000_000


def test_book_event_truncates_and_casts():
    """Levels are cut to depth, cast to float pairs and extra columns dropped."""
    order_book = {
        "symbol": "ALPH/USDT",
        "timestamp": 1_757_160_000_123,
        "bids": [["1.10", "5", "x"], [1.09, 6.0], [1.08, 7.0]],
        "asks": [[1.11, 1.0], [1.12, 2.0], [1.13, 3.0]],
    }
    event = book_event_from_ccxt(
        "mexc", "ALPH/USDT", order_book, seq=4, ts_recv=TS, depth=2
    )
    assert event.venue == "mexc"
    assert event.symbol == "ALPH/USDT"
    assert event.seq == 4
    assert event.ts_recv == TS
    assert event.ts_exch == 1_757_160_000_123
    assert event.bids == [(1.10, 5.0), (1.09, 6.0)]
    assert event.asks == [(1.11, 1.0), (1.12, 2.0)]


def test_book_event_without_exchange_timestamp():
    """A venue that sends no timestamp yields ts_exch None and empty sides."""
    event = book_event_from_ccxt(
        "gate", "BTC/USDT", {"timestamp": None}, seq=1, ts_recv=TS, depth=20
    )
    assert event.ts_exch is None
    assert event.bids == []
    assert event.asks == []


def test_trade_event_fields():
    """Trade fields are mapped and side parsed into the enum."""
    trade = {
        "id": 12345,
        "timestamp": 1_757_160_000_500,
        "side": "sell",
        "price": "150.25",
        "amount": "3",
    }
    event = trade_event_from_ccxt("mexc", "SOL/USDT", trade, seq=9, ts_recv=TS)
    assert event.trade_id == "12345"
    assert event.side is Side.SELL
    assert event.price == 150.25
    assert event.amount == 3.0
    assert event.ts_exch == 1_757_160_000_500
    assert event.seq == 9


def test_trade_event_missing_optionals():
    """Missing id, side and timestamp become None rather than errors."""
    event = trade_event_from_ccxt(
        "mexc", "SOL/USDT", {"price": 1.0, "amount": 2.0}, seq=1, ts_recv=TS
    )
    assert event.trade_id is None
    assert event.side is None
    assert event.ts_exch is None


def test_balance_event_keeps_only_assets():
    """CCXT meta keys are dropped and missing figures read as zero."""
    balance = {
        "info": {"raw": True},
        "timestamp": 1_757_160_000_000,
        "datetime": "2026-09-06T00:00:00Z",
        "free": {"USDT": 10.0},
        "used": {"USDT": 5.0},
        "total": {"USDT": 15.0},
        "USDT": {"free": 10.0, "used": 5.0, "total": 15.0},
        "ALPH": {"free": None, "used": None, "total": 3.5},
    }
    event = balance_event_from_ccxt("mexc", balance, seq=2, ts_recv=TS)
    assert event.venue == "mexc"
    assert event.ts_exch == 1_757_160_000_000
    assert event.balances == {
        "USDT": AssetBalance(free=10.0, used=5.0, total=15.0),
        "ALPH": AssetBalance(free=0.0, used=0.0, total=3.5),
    }


def ccxt_order(**overrides: object) -> dict:
    """Return a CCXT unified order, overridable field by field."""
    order = {
        "id": "venue-99",
        "clientOrderId": "t-250906120000_lmb_eb",
        "symbol": "ALPH/USDT",
        "status": "open",
        "side": "sell",
        "price": 0.35,
        "average": None,
        "amount": 40,
        "filled": 0,
        "remaining": 40,
        "timestamp": 1_757_160_000_000,
        "fee": None,
        "takerOrMaker": None,
    }
    order.update(overrides)
    return order


@pytest.mark.parametrize(
    ("status", "filled", "expected"),
    [
        ("open", Decimal(0), OrderState.OPEN),
        ("open", Decimal(1), OrderState.PARTIALLY_FILLED),
        ("closed", Decimal(40), OrderState.FILLED),
        ("canceled", Decimal(0), OrderState.CANCELLED),
        ("cancelled", Decimal(0), OrderState.CANCELLED),
        ("expired", Decimal(0), OrderState.EXPIRED),
        ("rejected", Decimal(0), OrderState.REJECTED),
    ],
)
def test_order_state_from_ccxt(status: str, filled: Decimal, expected: OrderState):
    """Every CCXT status maps to the state consumers reason about."""
    assert order_state_from_ccxt(status, filled) is expected


def test_an_unknown_status_is_reported_as_open():
    """A status we do not recognise is reported, never dropped."""
    assert order_state_from_ccxt("weird", Decimal(0)) is OrderState.OPEN


def test_fill_from_ccxt_is_the_delta_since_the_last_update():
    """CCXT reports cumulative fills, so a fill is a difference."""
    fill = fill_from_ccxt(ccxt_order(filled=25, average=0.351), Decimal("10"))
    assert fill is not None
    assert fill.amount == Decimal("15")
    assert fill.price == Decimal("0.351")


def test_fill_from_ccxt_is_none_when_nothing_traded():
    """An update that moves no quantity carries no fill."""
    assert fill_from_ccxt(ccxt_order(filled=10), Decimal("10")) is None
    assert fill_from_ccxt(ccxt_order(), Decimal(0)) is None


def test_fill_from_ccxt_falls_back_to_the_order_price():
    """Without an average, the order's own price prices the fill."""
    fill = fill_from_ccxt(ccxt_order(filled=10), Decimal(0))
    assert fill is not None
    assert fill.price == Decimal("0.35")


def test_fill_from_ccxt_carries_fee_and_liquidity():
    """Fee and maker or taker are kept when the venue reports them."""
    order = ccxt_order(
        filled=10,
        average=0.351,
        fee={"cost": 0.0035, "currency": "USDT"},
        takerOrMaker="maker",
    )
    fill = fill_from_ccxt(order, Decimal(0))
    assert fill is not None
    assert fill.fee == Decimal("0.0035")
    assert fill.fee_currency == "USDT"
    assert fill.liquidity is Liquidity.MAKER


def test_order_event_from_ccxt_carries_the_whole_order():
    """The event is a faithful rendering of the CCXT order."""
    event = order_event_from_ccxt(
        "mexc",
        ccxt_order(status="closed", filled=40, remaining=0, average=0.351),
        strategy="lmb_eb",
        ts_recv=TS,
        tags={"strategy_id": "lmb"},
    )
    assert event.venue == "mexc"
    assert event.symbol == "ALPH/USDT"
    assert event.state is OrderState.FILLED
    assert event.side is Side.SELL
    assert event.intent_id == "t-250906120000_lmb_eb"
    assert event.venue_order_id == "venue-99"
    assert event.filled == Decimal("40")
    assert event.remaining == Decimal(0)
    assert event.avg_price == Decimal("0.351")
    assert event.ts_exch == 1_757_160_000_000
    assert event.tags == {"strategy_id": "lmb"}


def test_order_event_falls_back_to_the_venue_id():
    """An order the venue gave no client id for is still identifiable."""
    event = order_event_from_ccxt(
        "mexc", ccxt_order(clientOrderId=None), strategy="", ts_recv=TS
    )
    assert event.intent_id == "venue-99"


def test_order_event_amounts_are_exact():
    """Money-touching figures never pass through a binary float."""
    event = order_event_from_ccxt(
        "mexc", ccxt_order(filled=0.1, remaining=0.2), strategy="", ts_recv=TS
    )
    assert event.filled == Decimal("0.1")
    assert event.remaining == Decimal("0.2")
