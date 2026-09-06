"""Tests for CCXT to event converters."""

from apps.shared.src.ccxt_events import (
    balance_event_from_ccxt,
    book_event_from_ccxt,
    trade_event_from_ccxt,
)
from apps.shared.src.events import AssetBalance, Side

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
