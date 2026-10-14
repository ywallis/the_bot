"""Tests for the accountant's status page."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from pandas import DataFrame

import apps.accountant.src.web as web
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


class FakeVenue:
    """A venue client with one resting order and one balance."""

    def __init__(self, fail_orders: bool = False) -> None:
        self.fail_orders = fail_orders

    async def fetch_open_orders(self, symbol: str) -> list[dict]:
        """Return one resting order, or fail as a timed-out request does."""
        if self.fail_orders:
            raise TimeoutError("open orders timed out")
        return [{"symbol": symbol, "side": "sell", "clientOrderId": "t-1_strat_slot"}]

    async def fetch_balance(self) -> dict:
        """Return a balance of one base unit."""
        return {"free": {"BASE": 1}, "used": {"BASE": 0}, "total": {"BASE": 1}}


IMBALANCES = DataFrame(
    [
        {
            "symbol": SYMBOL,
            "clientorderid": "t-2_strat_slot",
            "delta": Decimal("5"),
            "usdt_delta": Decimal("1"),
        }
    ]
)


def fake_database(*args: Any) -> dict:
    """Return one imbalance and no performance row."""
    return {SYMBOL: (IMBALANCES, None, None, Decimal("0.15"))}


def failing_database(*args: Any) -> dict:
    """Fail as an unreachable database does."""
    raise psycopg.OperationalError("connection refused")


async def snapshot_with(monkeypatch: pytest.MonkeyPatch, database: Any, **venues: Any):
    """Take a snapshot over fake venues and a fake database."""
    monkeypatch.setattr(web, "query_database", database)
    pairs = {(venue, SYMBOL) for venue in venues}
    return await web.take_snapshot(venues, pairs, {}, None)


@pytest.mark.asyncio
async def test_take_snapshot_marks_an_imbalance_without_an_order_unaddressed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With every venue listed, an imbalance without an order is unaddressed."""
    snapshot = await snapshot_with(
        monkeypatch, fake_database, venue_a=FakeVenue(), venue_b=FakeVenue()
    )
    assert snapshot.errors == []
    assert len(snapshot.open_orders) == 2
    assert [row["state"] for row in snapshot.imbalances] == ["unaddressed"]


@pytest.mark.asyncio
async def test_take_snapshot_marks_imbalances_unknown_when_a_venue_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A venue whose open orders failed may hold the order closing it."""
    snapshot = await snapshot_with(
        monkeypatch,
        fake_database,
        venue_a=FakeVenue(),
        venue_b=FakeVenue(fail_orders=True),
    )
    assert not snapshot.open_orders_complete
    assert [row["state"] for row in snapshot.imbalances] == ["unknown"]
    assert any("venue_b" in error for error in snapshot.errors)
    assert "Open orders are incomplete" in render(snapshot)


@pytest.mark.asyncio
async def test_take_snapshot_reports_an_unreachable_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreachable database is an error, never an empty table."""
    snapshot = await snapshot_with(monkeypatch, failing_database, venue_a=FakeVenue())
    assert not snapshot.database_ok
    assert any("connection refused" in error for error in snapshot.errors)
    assert len(snapshot.balances) == 1
    page = render(snapshot)
    assert "Database unavailable." in page
    assert "No trade." not in page


@pytest.mark.asyncio
async def test_refresh_keeps_the_last_snapshot_and_says_it_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed refreshes replace the error, they never pile errors up."""
    good = Snapshot(taken_at=datetime(2026, 1, 1, tzinfo=UTC), errors=["old"])
    app = web.make_app()
    app[web.LATEST].snapshot = good
    rounds = 0

    async def failing_snapshot(*args: Any) -> Snapshot:
        raise RuntimeError("venue down")

    async def stop_after_three(seconds: float) -> None:
        nonlocal rounds
        rounds += 1
        if rounds == 3:
            raise asyncio.CancelledError

    monkeypatch.setattr(web, "take_snapshot", failing_snapshot)
    monkeypatch.setattr(web.asyncio, "sleep", stop_after_three)
    with pytest.raises(asyncio.CancelledError):
        await web.refresh(app, {}, None)
    shown = app[web.LATEST].snapshot
    assert shown.taken_at == good.taken_at
    assert len(shown.errors) == 1
    assert "stale" in shown.errors[0] and "venue down" in shown.errors[0]
