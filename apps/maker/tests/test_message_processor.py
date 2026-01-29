"""Tests for the message processor."""

import asyncio
import json
from typing import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from apps.maker.src.errors import BrokerError
from apps.maker.src.message_processor import (
    MessageProcessor,
)
from apps.maker.src.structs import (
    CancellationMessage,
    OrderBatchMessage,
    OrderMessage,
    Response,
)


@pytest.fixture
def processor():
    """Return a MessageProcessor instance."""
    return MessageProcessor()


def test_get_lock_creates_new_lock(processor: MessageProcessor):
    """Test that a new lock is created if one does not exist."""
    strategy = "test_strategy"
    lock = processor.get_lock(strategy)
    assert isinstance(lock, asyncio.Lock)
    # Calling again should return the same lock.
    assert processor.get_lock(strategy) is lock


def test_replace_queued_value(
    order_1: OrderMessage, order_2: OrderMessage, processor: MessageProcessor
):
    """Test replacing a queued message."""
    strategy = order_1["strategy"]
    # Initial queued order.
    processor.message_queue[strategy] = order_1
    # New message replaces the queued one.
    previous_id = processor.replace_queued_value(order_2)
    assert previous_id == order_1["id"]
    assert processor.message_queue[strategy] == order_2


@pytest.mark.asyncio
async def test_process_order_message_without_lock(
    order_1: OrderMessage,
    fake_send_to_broker_positive: Callable[[str], Response],
    monkeypatch: pytest.MonkeyPatch,
    processor: MessageProcessor,
):
    """Test processing an order message when no lock is held."""
    strategy = order_1["strategy"]

    # Monkeypatch send_to_broker to simulate a valid broker response.
    monkeypatch.setattr(processor, "send_to_broker", fake_send_to_broker_positive)

    result = await processor.process_message(order_1)
    assert result is not None
    assert f"{order_1['id']} was processed" in result
    assert strategy in processor.open_orders
    assert (
        processor.open_orders[strategy]["exchange_id"]
        == f"{order_1['id']}mock_response"
    )


@pytest.mark.asyncio
async def test_process_cancellation_message_with_open_order(
    order_1: OrderMessage,
    cancellation_1: CancellationMessage,
    monkeypatch: pytest.MonkeyPatch,
    processor: MessageProcessor,
):
    """Test processing a cancellation message when an open order exists."""
    strategy = order_1["strategy"]
    # Pre-populate open_orders to simulate an existing order.
    processor.open_orders[strategy] = order_1
    cancellation_msg = cancellation_1

    async def fake_place_cancellation(_msg):
        return True

    monkeypatch.setattr(processor, "place_cancellation", fake_place_cancellation)

    result = await processor.process_message(cancellation_msg)
    assert result is not None
    assert f"{cancellation_1['id']} was processed" in result
    assert strategy not in processor.open_orders


@pytest.mark.asyncio
async def test_unique_order(
    order_unique_1: OrderMessage,
    fake_send_to_broker_positive: Callable[[str], Response],
    monkeypatch: pytest.MonkeyPatch,
    processor: MessageProcessor,
):
    """Test processing a unique order."""
    strategy = order_unique_1["strategy"]

    # Monkeypatch send_to_broker to simulate a valid broker response.
    monkeypatch.setattr(processor, "send_to_broker", fake_send_to_broker_positive)

    result = await processor.process_message(order_unique_1)
    assert result is not None
    assert f"{order_unique_1['id']} was processed" in result
    assert strategy not in processor.open_orders


@pytest.mark.asyncio
async def test_order_batch(
    order_batch_1: OrderMessage,
    fake_send_to_broker_positive: Callable[[str], Response],
    monkeypatch: pytest.MonkeyPatch,
    processor: MessageProcessor,
):
    """Test processing an order batch."""
    strategy = order_batch_1["strategy"]

    # Monkeypatch send_to_broker to simulate a valid broker response.
    monkeypatch.setattr(processor, "send_to_broker", fake_send_to_broker_positive)

    result = await processor.process_message(order_batch_1)
    assert result is not None
    assert f"{order_batch_1['id']} was processed" in result
    assert strategy not in processor.open_orders


@pytest.mark.asyncio
async def test_process_message_queue(
    order_1: OrderMessage, order_2: OrderMessage, processor: MessageProcessor
):
    """Test that messages are queued if a lock is held."""
    strategy = order_1["strategy"]
    # Acquire the lock to simulate it being busy.
    lock = processor.get_lock(strategy)
    await lock.acquire()

    processor.message_queue[strategy] = order_1
    # Process a new order message; it should replace the queued one.
    result = await processor.process_message(order_2)
    assert result is not None
    assert f"{order_2['id']} replaced {order_1['id']} in queue" in result

    lock.release()


@pytest.mark.asyncio
async def test_place_order_raises_broker_error(
    order_1: OrderMessage,
    fake_send_to_broker_negative: Callable[[str], Response],
    monkeypatch: pytest.MonkeyPatch,
    processor: MessageProcessor,
):
    """Test that place_order raises BrokerError on failure."""
    # Simulate an invalid broker response.
    monkeypatch.setattr(processor, "send_to_broker", fake_send_to_broker_negative)

    with pytest.raises(BrokerError):
        await processor.place_order(order_1)


@pytest.mark.asyncio
async def test_collect_results_periodically(
    processor: MessageProcessor, dummy_task: Callable
):
    """Test periodic collection of results."""
    # Add a dummy task that should complete quickly.
    processor.tasks.append(asyncio.create_task(dummy_task()))

    # Start the periodic collector in the background.
    collector = asyncio.create_task(processor.collect_results_periodically())

    # Wait long enough for the dummy task to complete and be collected.
    await asyncio.sleep(1)

    # After collection, the tasks list should be empty.
    assert len(processor.tasks) == 0

    # Cancel the infinite loop task to clean up.
    collector.cancel()
    try:
        await collector
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_get_open_orders(
    processor: MessageProcessor, order_batch_raw: str, mocker: MockerFixture
):
    """Test fetching open orders."""
    # Create a fake Redis pool
    mock_redis = MagicMock()
    mock_pubsub = MagicMock()

    # Mock Redis connection behavior
    mock_redis.__aenter__.return_value = mock_redis
    mock_pubsub.__aenter__.return_value = mock_pubsub

    # Mock publish method (so it doesn't do anything)
    mock_redis.publish = AsyncMock()

    # Mock pubsub.subscribe() and unsubscribe()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()

    # Define synthetic response
    synthetic_message = {
        "type": "message",
        "data": json.dumps(order_batch_raw).encode("utf-8"),
    }

    # Mock listen() method to yield a synthetic response
    async def mock_listen():
        yield synthetic_message

    mock_pubsub.listen = mock_listen

    mock_redis.pubsub.return_value = mock_pubsub
    # Mock `Redis` instance inside your function
    mocker.patch("apps.maker.src.message_processor.Redis", return_value=mock_redis)

    # Create instance of your class

    # Call the function
    processor.open_orders = await processor.get_open_orders()

    # Validate response
    assert "ALPH_gate" in processor.open_orders
    # Ensure correct Redis calls
    mock_redis.publish.assert_called_once()
    mock_pubsub.subscribe.assert_called_once_with("INIT")
    mock_pubsub.unsubscribe.assert_called_once_with("INIT")


@pytest.mark.asyncio
async def test_get_open_orders_empty(
    processor: MessageProcessor, empty_open_orders: OrderBatchMessage, mocker
):
    """Test fetching open orders when there are none."""
    # Create a fake Redis pool
    mock_redis = MagicMock()
    mock_pubsub = MagicMock()

    # Mock Redis connection behavior
    mock_redis.__aenter__.return_value = mock_redis
    mock_pubsub.__aenter__.return_value = mock_pubsub

    # Mock publish method (so it doesn't do anything)
    mock_redis.publish = AsyncMock()

    # Mock pubsub.subscribe() and unsubscribe()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()

    # Define synthetic response
    synthetic_message = {
        "type": "message",
        "data": json.dumps(dict(empty_open_orders), default=str).encode("utf-8"),
    }

    # Mock listen() method to yield a synthetic response
    async def mock_listen():
        yield synthetic_message

    mock_pubsub.listen = mock_listen

    mock_redis.pubsub.return_value = mock_pubsub
    # Mock `Redis` instance inside your function
    mocker.patch("apps.maker.src.message_processor.Redis", return_value=mock_redis)

    # Create instance of your class

    # Call the function
    processor.open_orders = await processor.get_open_orders()

    # Validate response
    assert "ALPH_gate" not in processor.open_orders
    assert processor.open_orders == {}
    # Ensure correct Redis calls
    mock_redis.publish.assert_called_once()
    mock_pubsub.subscribe.assert_called_once_with("INIT")
    mock_pubsub.unsubscribe.assert_called_once_with("INIT")
