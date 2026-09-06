"""Fixtures for the maker app tests."""

import asyncio
import json
from collections.abc import Awaitable
from copy import deepcopy
from decimal import Decimal
from typing import Callable

import pytest
import pytest_asyncio

from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.structs import (
    CancellationMessage,
    OrderBatchMessage,
    OrderMessage,
    Response,
)


class StopWatching(Exception):
    """Raised by fake clients to break out of a watch loop."""


class FakeClient:
    """
    A CCXT-like client that replays scripted results then stops.

    Every ``watch_*`` method returns the next scripted result, raising it if
    it is an exception and raising ``StopWatching`` once the script runs out.
    A watch loop swallows the errors it knows how to recover from, so the
    only reliable way to end one in a test is an error it does not know.
    """

    def __init__(self, venue: str, results: list):
        """Store the scripted results, which may be dicts, lists or exceptions."""
        self.id = venue
        self.name = venue.title()
        self._results = list(results)
        self.closed = 0

    async def _next(self):
        if not self._results:
            raise StopWatching()
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def watch_order_book(self, symbol: str):
        """Return the next scripted order book."""
        return await self._next()

    async def watch_trades(self, symbol: str):
        """Return the next scripted trade batch."""
        return await self._next()

    async def watch_orders(self, symbol: str, since: int | None = None):
        """Return the next scripted batch of order updates."""
        self.since = since
        return await self._next()

    async def close(self):
        """Count close calls."""
        self.closed += 1


@pytest.fixture
def order_raw_item():
    """Return a raw dictionary representation of an order."""
    order_raw_item: dict[str, str] = {
        "kind": "order",
        "strategy": "ALPH_gate",
        "exchange": "gate",
        "id": "gate_test_1",
        "exchange_id": "_",
        "pair": "ALPH/USDT",
        "side": "sell",
        "order_type": "replace",
        "price": "100",
        "amount": "10",
    }
    return deepcopy(order_raw_item)


@pytest.fixture
def order_raw(order_raw_item: dict[str, str]):
    """Return a copy of the raw order item."""
    return deepcopy(order_raw_item)


@pytest.fixture
def order_raw_string(order_raw_item: dict[str, str]):
    """Return a JSON string representation of the raw order item."""
    order_raw_string = json.dumps(order_raw_item)
    return deepcopy(order_raw_string)


@pytest.fixture
def order_1():
    """Return a deepcopy of an OrderMessage."""
    order_1: OrderMessage = OrderMessage(
        kind=MessageType.ORDER,
        strategy="lab_eb",
        exchange="gate",
        id="t-2025_lab_eb",
        exchange_id="_",
        pair="ALPH/USDT",
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal(100),
        amount=Decimal(10),
    )
    return deepcopy(order_1)


@pytest.fixture
def order_2():
    """Return a deepcopy of a second OrderMessage."""
    order_2: OrderMessage = OrderMessage(
        kind=MessageType.ORDER,
        strategy="lab_eb",
        exchange="gate",
        id="t-2026_lab_eb",
        exchange_id="_",
        pair="ALPH/USDT",
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal(100),
        amount=Decimal(10),
    )
    return deepcopy(order_2)


@pytest.fixture
def order_unique_1():
    """Return a deepcopy of a unique OrderMessage."""
    order_unique_1: OrderMessage = OrderMessage(
        kind=MessageType.ORDER,
        strategy="ALPH_gate",
        exchange="gate",
        id="gate_test_3-ALPH_gate",
        exchange_id="_",
        pair="ALPH/USDT",
        side=OrderSide.SELL,
        order_type=OrderType.UNIQUE,
        price=Decimal(100),
        amount=Decimal(10),
    )
    return deepcopy(order_unique_1)


@pytest.fixture
def order_batch_1(order_1: OrderMessage, order_2: OrderMessage):
    """Return a deepcopy of an OrderBatchMessage."""
    order_batch_1: OrderBatchMessage = OrderBatchMessage(
        kind=MessageType.ORDERBATCH,
        strategy="ALPH_gate",
        id="gate_test_batch-ALPH_gate",
        orders=[order_1, order_2],
    )
    return deepcopy(order_batch_1)


@pytest.fixture
def empty_open_orders():
    """Return a deepcopy of an empty OrderBatchMessage."""
    order_batch_empty: OrderBatchMessage = OrderBatchMessage(
        kind=MessageType.ORDERBATCH,
        strategy="INIT",
        id="INIT",
        orders=[],
    )
    return deepcopy(order_batch_empty)


@pytest.fixture
def order_batch_raw(order_raw_string: str):
    """Return a raw dictionary representation of an order batch."""
    order_batch_1 = {
        "kind": "orderbatch",
        "strategy": "ALPH_gate",
        "id": "gate_test_batch-ALPH_gate",
        "orders": [order_raw_string, order_raw_string],
    }
    return deepcopy(order_batch_1)


@pytest.fixture
def cancellation_1():
    """Return a deepcopy of a CancellationMessage."""
    cancellation_1: CancellationMessage = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy="lab_eb",
        exchange="gate",
        id="gate_test_2-ALPH_gate",
        pair="ALPH/USDT",
    )
    return deepcopy(cancellation_1)


@pytest.fixture
def order_response_positive() -> Callable[
    [OrderMessage | CancellationMessage], Response
]:
    """Return a factory for positive responses."""

    def _factory(msg: OrderMessage | CancellationMessage):
        order_response_positive: Response = Response(
            kind=MessageType.ORDER, text=f'{{"id": "{msg["id"]}mock_response"}}'
        )
        return order_response_positive

    return _factory


@pytest_asyncio.fixture
async def fake_send_to_broker_positive(
    order_response_positive: Callable[
        [OrderMessage | CancellationMessage], Awaitable[Response]
    ],
) -> Callable[[OrderMessage | CancellationMessage], Awaitable[Awaitable[Response]]]:
    """Return an async callable simulating a positive broker response."""

    async def _fake_send_to_broker_positive(msg: OrderMessage | CancellationMessage):
        return order_response_positive(msg)

    return _fake_send_to_broker_positive


@pytest_asyncio.fixture
async def fake_send_to_broker_negative() -> Callable[[str], Awaitable[Response]]:
    """Return an async callable simulating a negative broker response."""

    async def _fake_send_to_broker_positive(_msg):
        return Response(kind=MessageType.ERROR, text="error")

    return _fake_send_to_broker_positive


# A dummy asynchronous task that completes quickly.
@pytest_asyncio.fixture
async def dummy_task() -> Callable[[], Awaitable[str]]:
    """Return a dummy async task."""

    async def _dummy_task():
        await asyncio.sleep(0.1)
        return "dummy result"

    return _dummy_task
