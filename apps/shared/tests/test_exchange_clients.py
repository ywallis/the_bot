"""Tests for loading the exchange clients' markets."""

import pytest
from ccxt.async_support import RateLimitExceeded

from apps.shared.src import exchange_clients


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
