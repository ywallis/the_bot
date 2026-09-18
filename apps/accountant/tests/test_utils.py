"""Tests for the accountant's export preparation and endpoint selection."""

from typing import Any

import pytest

from apps.accountant.src.utils import (
    fetch_finished_orders,
    prepare_items_for_pg,
    retrieve_and_prepare_orders,
)

SYMBOL = "BTC/USDT"


def fake_client(has: dict[str, bool] | None = None) -> Any:
    """Return a CCXT client stand-in recording which endpoint was called."""

    class FakeClient:
        name = "venue_a"
        id = "venue_a"

        def __init__(self) -> None:
            self.has = has or {}
            self.calls: list[str] = []

        async def fetch_canceled_and_closed_orders(self, **kwargs: Any) -> list[Any]:
            self.calls.append("canceled_and_closed")
            return [dict(self.order)]

        async def fetch_closed_orders(self, **kwargs: Any) -> list[Any]:
            self.calls.append("closed")
            return [dict(self.order)]

    client = FakeClient()
    client.order = {"symbol": SYMBOL, "side": "buy", "amount": 1.0, "price": 1.0}
    return client


def trade(**overrides: Any) -> dict[str, Any]:
    """Return a CCXT trade in the shape the preparer reads."""
    raw: dict[str, Any] = {
        "symbol": SYMBOL,
        "side": "buy",
        "amount": 10.0,
        "price": 2.0,
        "cost": 20.0,
    }
    raw.update(overrides)
    return raw


# Fee columns ----------------------------------------------------------------


def test_a_quote_fee_on_a_buy_adds_to_the_usdt_spent():
    """A fee in the quote asset comes out of the USDT leg."""
    item = trade(fee={"cost": 0.02, "currency": "USDT"})
    prepared = prepare_items_for_pg(fake_client(), [item])[0]
    assert prepared["usdt_value"] == "20.02"
    assert prepared["asset_net_q"] == "10.0"
    assert prepared["fee_cost"] == "0.02"
    assert prepared["fee_currency"] == "USDT"


def test_a_quote_fee_on_a_sell_reduces_the_usdt_received():
    """The venue takes its cut from the proceeds."""
    item = trade(side="sell", fee={"cost": 0.02, "currency": "USDT"})
    prepared = prepare_items_for_pg(fake_client(), [item])[0]
    assert prepared["usdt_value"] == "19.98"
    assert prepared["asset_net_q"] == "10.0"


def test_a_base_fee_on_a_buy_reduces_the_asset_received():
    """A fee charged in the base asset erodes what the buy delivered."""
    item = trade(fee={"cost": 0.01, "currency": "BTC"})
    prepared = prepare_items_for_pg(fake_client(), [item])[0]
    assert prepared["usdt_value"] == "20.0"
    assert prepared["asset_net_q"] == "9.99"


def test_a_base_fee_on_a_sell_takes_more_than_the_amount():
    """A fee charged in the base asset leaves the balance beyond the amount."""
    item = trade(side="sell", fee={"cost": 0.01, "currency": "BTC"})
    prepared = prepare_items_for_pg(fake_client(), [item])[0]
    assert prepared["usdt_value"] == "20.0"
    assert prepared["asset_net_q"] == "10.01"


def test_a_fee_in_a_third_asset_leaves_both_legs_alone():
    """A discount token fee is paid in kind; base and USDT flows are untouched."""
    item = trade(fee={"cost": 0.5, "currency": "TKN"})
    prepared = prepare_items_for_pg(fake_client(), [item])[0]
    assert prepared["usdt_value"] == "20.0"
    assert prepared["asset_net_q"] == "10.0"


def test_a_trade_without_a_fee_reports_empty_fee_columns():
    """Nothing charged means nothing to report."""
    prepared = prepare_items_for_pg(fake_client(), [trade()])[0]
    assert prepared["fee_cost"] == ""
    assert prepared["fee_currency"] == ""
    assert prepared["usdt_value"] == "20.0"
    assert prepared["asset_net_q"] == "10.0"


def test_the_last_non_zero_fee_entry_wins():
    """A multi-entry fee list resolves to the fee actually charged."""
    item = trade(
        fees=[
            {"cost": 0.0, "currency": "USDT"},
            {"cost": 0.01, "currency": "BTC"},
        ]
    )
    prepared = prepare_items_for_pg(fake_client(), [item])[0]
    assert prepared["fee_cost"] == "0.01"
    assert prepared["fee_currency"] == "BTC"
    assert prepared["asset_net_q"] == "9.99"


def test_a_none_fee_cost_counts_as_zero():
    """Venues that report a currency but no cost do not crash the export."""
    item = trade(fee={"cost": None, "currency": "USDT"})
    prepared = prepare_items_for_pg(fake_client(), [item])[0]
    assert prepared["fee_cost"] == "0.0"
    assert prepared["usdt_value"] == "20.0"


def test_a_malformed_symbol_is_rejected():
    """A symbol without both halves cannot be valued; fail loudly."""
    with pytest.raises(ValueError, match="BASE/QUOTE"):
        prepare_items_for_pg(fake_client(), [trade(symbol="BTCUSDT")])


def test_a_single_item_is_wrapped_and_stamped():
    """One item outside a list still exports, tagged with the venue."""
    prepared = prepare_items_for_pg(fake_client(), trade())[0]
    assert prepared["exchange"] == "venue_a"


def test_the_order_key_is_renamed_for_sql():
    """CCXT calls it ``order``; the database column is ``order_id``."""
    item = trade(order="123")
    prepared = prepare_items_for_pg(fake_client(), [item])[0]
    assert "order" not in prepared
    assert prepared["order_id"] == "123"


# Endpoint selection ---------------------------------------------------------


@pytest.mark.asyncio
async def test_finished_orders_use_the_combined_endpoint_where_it_exists():
    """A venue that lists cancelled and closed orders together is used."""
    client = fake_client(has={"fetchCanceledAndClosedOrders": True})
    orders = await fetch_finished_orders(client, SYMBOL, 1, 2)
    assert client.calls == ["canceled_and_closed"]
    assert orders[0]["symbol"] == SYMBOL


@pytest.mark.asyncio
async def test_finished_orders_fall_back_to_closed():
    """A venue without the combined endpoint still yields finished orders."""
    client = fake_client(has={})
    orders = await fetch_finished_orders(client, SYMBOL, 1, 2)
    assert client.calls == ["closed"]
    assert orders[0]["symbol"] == SYMBOL


@pytest.mark.asyncio
async def test_retrieve_and_prepare_orders_prepares_what_it_fetches():
    """The fetched orders go through the fee preparation end to end."""
    client = fake_client(has={})
    client.order = trade(fee={"cost": 0.02, "currency": "USDT"})
    prepared = await retrieve_and_prepare_orders(client, SYMBOL, 1, 2)
    assert prepared[0]["usdt_value"] == "20.02"
    assert prepared[0]["exchange"] == "venue_a"
