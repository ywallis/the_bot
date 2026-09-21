"""Tests for the derivatives account setup."""

from typing import Any

import pytest
from ccxt.async_support import ExchangeError, MarginModeAlreadySet, NotSupported

from apps.shared.src.config import VenueConfig
from apps.shared.src.derivatives import DerivativesSetupError, configure_derivatives

PERP = "BASE/QUOTE:QUOTE"
SPOT = "BASE/QUOTE"


class FakeClient:
    """A CCXT stand-in that records the setters called on it."""

    def __init__(
        self,
        has: dict[str, bool] | None = None,
        fail: dict[str, Exception] | None = None,
    ) -> None:
        self.has = (
            has
            if has is not None
            else {
                "setMarginMode": True,
                "setLeverage": True,
                "setPositionMode": True,
            }
        )
        self.fail = fail or {}
        self.calls: list[tuple[Any, ...]] = []

    async def set_margin_mode(
        self, mode: str, symbol: str, params: dict[str, Any] | None = None
    ) -> None:
        """Record the call and fail if told to."""
        self.calls.append(("margin", mode, symbol, params))
        if "margin" in self.fail:
            raise self.fail["margin"]

    async def set_leverage(self, leverage: int, symbol: str) -> None:
        """Record the call and fail if told to."""
        self.calls.append(("leverage", leverage, symbol))
        if "leverage" in self.fail:
            raise self.fail["leverage"]

    async def set_position_mode(self, hedged: bool, symbol: str) -> None:
        """Record the call and fail if told to."""
        self.calls.append(("position_mode", hedged, symbol))
        if "position_mode" in self.fail:
            raise self.fail["position_mode"]


def swap_venue(**overrides: Any) -> VenueConfig:
    """Return a swap venue with cross margin at 2x unless overridden."""
    fields: dict[str, Any] = dict(
        id="venue_a_perp",
        ccxt_id="venue_a",
        name="Venue A perpetuals",
        market_type="swap",
        margin_mode="cross",
        leverage=2,
    )
    fields.update(overrides)
    return VenueConfig(**fields)


@pytest.mark.asyncio
async def test_every_setter_runs_on_every_contract_symbol_in_order():
    """Margin mode, then leverage, then one-way mode, per contract symbol."""
    client = FakeClient()
    applied = await configure_derivatives(client, swap_venue(), {PERP, SPOT})
    assert client.calls == [
        ("margin", "cross", PERP, {"leverage": 2}),
        ("leverage", 2, PERP),
        ("position_mode", False, PERP),
    ]
    assert applied == [
        f"{PERP} margin mode cross",
        f"{PERP} leverage 2",
        f"{PERP} one-way position mode",
    ]


@pytest.mark.asyncio
async def test_a_spot_venue_is_left_alone():
    """Nothing is set on a wallet that cannot hold a position."""
    client = FakeClient()
    venue = VenueConfig(id="venue_a", name="Venue A")
    assert await configure_derivatives(client, venue, {SPOT}) == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_a_swap_venue_with_nothing_configured_is_left_alone():
    """The settings are opt-in: an account configured by hand stays as is."""
    client = FakeClient()
    venue = swap_venue(margin_mode=None, leverage=None)
    assert await configure_derivatives(client, venue, {PERP}) == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_already_set_is_not_an_error():
    """A venue reporting the mode is already in place has done what we asked."""
    client = FakeClient(fail={"margin": MarginModeAlreadySet("no change")})
    applied = await configure_derivatives(client, swap_venue(), {PERP})
    assert f"{PERP} margin mode cross" not in applied
    assert f"{PERP} leverage 2" in applied


@pytest.mark.asyncio
async def test_a_refused_setting_fails_the_venue():
    """A leverage the venue will not grant must stop the venue trading."""
    client = FakeClient(fail={"leverage": ExchangeError("max leverage exceeded")})
    with pytest.raises(DerivativesSetupError, match="leverage 2"):
        await configure_derivatives(client, swap_venue(), {PERP})


@pytest.mark.asyncio
async def test_missing_setters_are_skipped_with_the_rest_applied():
    """A venue without a setter in its has map is configured by hand."""
    client = FakeClient(has={"setLeverage": True})
    applied = await configure_derivatives(client, swap_venue(), {PERP})
    assert client.calls == [("leverage", 2, PERP)]
    assert applied == [f"{PERP} leverage 2"]


@pytest.mark.asyncio
async def test_unsupported_position_mode_is_tolerated():
    """A venue that is one-way by construction raises NotSupported; fine."""
    client = FakeClient(fail={"position_mode": NotSupported("one-way only")})
    applied = await configure_derivatives(client, swap_venue(), {PERP})
    assert applied == [f"{PERP} margin mode cross", f"{PERP} leverage 2"]


@pytest.mark.asyncio
async def test_a_unified_account_configures_its_contracts_only():
    """On one margin pool the spot symbol is skipped and the contract set."""
    client = FakeClient()
    venue = VenueConfig(id="venue_a", name="Venue A", account="unified", leverage=3)
    applied = await configure_derivatives(client, venue, {SPOT, PERP})
    assert ("leverage", 3, PERP) in client.calls
    assert all(call[2] == PERP for call in client.calls)
    assert applied[0] == f"{PERP} leverage 3"
