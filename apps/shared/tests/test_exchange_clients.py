"""Tests for CCXT client construction."""

import asyncio

import pytest
from ccxt.async_support import RateLimitExceeded
from ccxt.async_support.base.exchange import Exchange

from apps.shared.src import exchange_clients
from apps.shared.src.config import VenueConfig
from apps.shared.src.exchange_clients import ccxt_params


def test_ccxt_params_with_password_and_options():
    """Password is included only when required; venue options are forwarded."""
    venue = VenueConfig(
        id="venue_y", name="Venue Y", options={"watchOrderBook": {"checksum": False}}
    )
    params = ccxt_params(venue, "k", "s", "p", requires_password=True)
    assert params == {
        "apiKey": "k",
        "secret": "s",
        "password": "p",
        "rateLimiterAlgorithm": "rollingWindow",
        "rollingWindowSize": exchange_clients.RATE_WINDOW_MS,
        "options": {"watchOrderBook": {"checksum": False}},
    }


def test_ccxt_params_minimal():
    """No password and no options yields the key pair and the rate limiter."""
    venue = VenueConfig(id="venue_x", name="Venue X")
    assert ccxt_params(venue, "k", "s", None, requires_password=False) == {
        "apiKey": "k",
        "secret": "s",
        "rateLimiterAlgorithm": "rollingWindow",
        "rollingWindowSize": exchange_clients.RATE_WINDOW_MS,
    }


async def throttle_wait(client: Exchange) -> float:
    """Return the seconds the client's rate limiter holds one request."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    await client.throttle()
    return loop.time() - start


@pytest.mark.asyncio
async def test_an_idle_client_is_not_throttled(monkeypatch):
    """After an idle spell the next request leaves at once, a burst still waits."""
    # 50 ms per request in a 100 ms window: a budget of two requests.
    monkeypatch.setattr(exchange_clients, "RATE_WINDOW_MS", 100)
    venue = VenueConfig(id="venue_x", name="Venue X")
    params = ccxt_params(venue, "k", "s", None, requires_password=False)
    client = Exchange({**params, "rateLimit": 50})
    try:
        await client.throttle()
        await asyncio.sleep(0.15)
        # CCXT's leaky bucket holds this one for a full rateLimit.
        assert await throttle_wait(client) < 0.01
        assert await throttle_wait(client) < 0.01
        # Over budget: held until the first request leaves the window.
        assert await throttle_wait(client) > 0.05
    finally:
        await client.close()


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


class Flaky:
    """A client refused by the rate limit a given number of times."""

    name = "venue"

    def __init__(self, refusals: int):
        """Store how many loads fail."""
        self.refusals = refusals
        self.calls = 0

    async def load_markets(self):
        """Fail, then succeed."""
        self.calls += 1
        if self.calls <= self.refusals:
            raise RateLimitExceeded("429 Too Many Requests")


@pytest.mark.asyncio
async def test_a_rate_limited_load_is_retried(monkeypatch):
    """A 429 at start is waited out rather than ending the start-up."""
    client = Flaky(refusals=2)
    monkeypatch.setattr(exchange_clients, "authenticated_clients", {"v": client})
    monkeypatch.setattr(exchange_clients, "LOAD_BACKOFF_S", 0)
    await exchange_clients.load_clients()
    assert client.calls == 3


@pytest.mark.asyncio
async def test_a_venue_that_never_loads_raises(monkeypatch):
    """After the last attempt the error reaches the caller."""
    client = Flaky(refusals=99)
    monkeypatch.setattr(exchange_clients, "authenticated_clients", {"v": client})
    monkeypatch.setattr(exchange_clients, "LOAD_BACKOFF_S", 0)
    with pytest.raises(RateLimitExceeded):
        await exchange_clients.load_clients()
    assert client.calls == exchange_clients.LOAD_ATTEMPTS
