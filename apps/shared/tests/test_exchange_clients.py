"""Tests for CCXT client construction."""

from apps.shared.src.config import VenueConfig
from apps.shared.src.exchange_clients import ccxt_params


def test_ccxt_params_with_password_and_options():
    """Password is included only when required; venue options are forwarded."""
    venue = VenueConfig(
        id="bitget", name="Bitget", options={"watchOrderBook": {"checksum": False}}
    )
    params = ccxt_params(venue, "k", "s", "p", requires_password=True)
    assert params == {
        "apiKey": "k",
        "secret": "s",
        "password": "p",
        "options": {"watchOrderBook": {"checksum": False}},
    }


def test_ccxt_params_minimal():
    """No password and no options yields just the key pair."""
    venue = VenueConfig(id="mexc", name="Mexc")
    assert ccxt_params(venue, "k", "s", None, requires_password=False) == {
        "apiKey": "k",
        "secret": "s",
    }


def test_a_swap_venue_addresses_the_futures_account():
    """``market_type = "swap"`` becomes CCXT's defaultType unless options say."""
    venue = VenueConfig(
        id="venue_a_perp", ccxt_id="venue_a", name="Venue A perp", market_type="swap"
    )
    params = ccxt_params(venue, "k", "s", None, requires_password=False)
    assert params["options"] == {"defaultType": "swap"}

    explicit = VenueConfig(
        id="venue_a_perp",
        ccxt_id="venue_a",
        name="Venue A perp",
        market_type="swap",
        options={"defaultType": "future", "watchOrderBook": {"checksum": False}},
    )
    params = ccxt_params(explicit, "k", "s", None, requires_password=False)
    assert params["options"] == {
        "defaultType": "future",
        "watchOrderBook": {"checksum": False},
    }
