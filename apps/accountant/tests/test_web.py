"""Tests for the accountant's status page."""

from datetime import UTC, datetime
from decimal import Decimal

from pandas import DataFrame

from apps.accountant.src.web import (
    Snapshot,
    balance_rows,
    imbalance_rows,
    order_rows,
    performance_rows,
    render,
)

SYMBOL = "BASE/QUOTE"


def test_balance_rows_leave_out_empty_assets() -> None:
    """Only assets with a non-zero total are listed."""
    balance = {
        "free": {"BASE": 1.5, "QUOTE": 0.0},
        "used": {"BASE": 0.5, "QUOTE": 0.0},
        "total": {"BASE": 2.0, "QUOTE": 0.0},
    }
    assert balance_rows("venue_a", balance) == [
        {
            "venue": "venue_a",
            "asset": "BASE",
            "free": "1.500000",
            "used": "0.500000",
            "total": "2.000000",
        }
    ]


def test_order_rows_keep_the_client_order_id() -> None:
    """The client order id is what ties an open order to an imbalance."""
    order = {
        "symbol": SYMBOL,
        "side": "sell",
        "price": 0.1,
        "remaining": 4,
        "amount": 5,
        "clientOrderId": "t-1_strat_slot",
        "datetime": "2026-01-01T00:00:00Z",
    }
    [row] = order_rows("venue_a", [order])
    assert row["client order id"] == "t-1_strat_slot"
    assert row["remaining"] == "4.0000"


def test_imbalance_rows_mark_pending_and_count_manual_orders() -> None:
    """An open order on the id makes it pending; ids not from the bot are counted."""
    imbalances = DataFrame(
        [
            {
                "symbol": SYMBOL,
                "clientorderid": "t-1_strat_slot",
                "delta": Decimal("10"),
                "usdt_delta": Decimal("1.5"),
            },
            {
                "symbol": SYMBOL,
                "clientorderid": "t-2_strat_slot",
                "delta": Decimal("-3"),
                "usdt_delta": Decimal("-0.45"),
            },
            {
                "symbol": SYMBOL,
                "clientorderid": None,
                "delta": Decimal("200"),
                "usdt_delta": Decimal("30"),
            },
            {
                "symbol": SYMBOL,
                "clientorderid": "123456",
                "delta": Decimal("200"),
                "usdt_delta": Decimal("30"),
            },
        ]
    )
    rows, manual = imbalance_rows(imbalances, {"t-1_strat_slot"})
    assert manual == 2
    assert [(r["client order id"], r["to close"], r["state"]) for r in rows] == [
        ("t-1_strat_slot", "sell", "pending"),
        ("t-2_strat_slot", "buy", "unaddressed"),
    ]


def test_imbalance_rows_accept_an_empty_result() -> None:
    """``send_sql_query`` returns None when no row matches."""
    assert imbalance_rows(None, set()) == ([], 0)


def test_performance_rows_mark_the_inventory_change() -> None:
    """Inventory bought is valued at the last price, not counted as a loss."""
    daily = DataFrame(
        [
            {
                "symbol": SYMBOL,
                "date": "2026-01-01",
                "item_count": 3,
                "usdt_gain": Decimal("-30"),
                "inventory_delta": Decimal("200"),
            }
        ]
    )
    [row] = performance_rows(daily, "date", Decimal("0.151"))
    assert row["marked at last price"] == "0.2000"
    assert row["trades"] == "3"


def test_performance_rows_read_the_monthly_columns() -> None:
    """The monthly query names its columns with a ``total_`` prefix."""
    monthly = DataFrame(
        [
            {
                "symbol": SYMBOL,
                "month": "2026-01",
                "total_usdt_gain": Decimal("1"),
                "total_inventory_delta": Decimal("0"),
            }
        ]
    )
    [row] = performance_rows(monthly, "month", None)
    assert row["quote gain"] == "1.0000"
    assert row["marked at last price"] == ""


def test_render_escapes_values_and_lists_errors() -> None:
    """Text from venues is escaped, and failures show on the page."""
    snapshot = Snapshot(
        taken_at=datetime(2026, 1, 1, tzinfo=UTC),
        open_orders=[{"client order id": "<script>"}],
        errors=["venue_a balance: timeout"],
    )
    page = render(snapshot)
    assert "&lt;script&gt;" in page
    assert "<script>" not in page
    assert "venue_a balance: timeout" in page
    assert "No balance." in page


def test_render_before_the_first_snapshot() -> None:
    """The page renders while the first snapshot is still being taken."""
    assert "not yet" in render(Snapshot())
