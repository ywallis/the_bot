import asyncio
from typing import Callable

import pytest

from apps.maker.src.errors import BrokerError
from apps.maker.src.message_processor import (
    MessageProcessor,
)
from apps.maker.src.structs import CancellationMessage, OrderMessage, Response


@pytest.fixture
def processor():
    return MessageProcessor()


def test_get_lock_creates_new_lock(processor: MessageProcessor):
    strategy = "test_strategy"
    lock = processor.get_lock(strategy)
    assert isinstance(lock, asyncio.Lock)
    # Calling again should return the same lock.
    assert processor.get_lock(strategy) is lock


def test_replace_queued_value(
    order_1: OrderMessage, order_2: OrderMessage, processor: MessageProcessor
):
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
    # Simulate an invalid broker response.
    monkeypatch.setattr(processor, "send_to_broker", fake_send_to_broker_negative)

    with pytest.raises(BrokerError):
        await processor.place_order(order_1)


# # A dummy asynchronous task that completes quickly.
# async def dummy_task():
#     await asyncio.sleep(0.1)
#     return "dummy result"


@pytest.mark.asyncio
async def test_collect_results_periodically(processor, dummy_task):
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
