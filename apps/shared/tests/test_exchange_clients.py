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
