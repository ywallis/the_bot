import asyncio

import pytest

from apps.maker.src.enums import MessageType
from apps.maker.src.errors import BrokerError
from apps.maker.src.message_processor import (
    MessageProcessor,
)  # Adjust your import accordingly
from apps.maker.tests.test_data import order_1, order_2


@pytest.fixture
def processor():
    return MessageProcessor()


def test_get_lock_creates_new_lock(processor: MessageProcessor):
    strategy = "test_strategy"
    lock = processor.get_lock(strategy)
    assert isinstance(lock, asyncio.Lock)
    # Calling again should return the same lock.
    assert processor.get_lock(strategy) is lock


def test_replace_queued_value(processor: MessageProcessor):
    strategy = order_1["strategy"]
    # Initial queued order.
    processor.message_queue[strategy] = order_1
    # New message replaces the queued one.
    previous_id = processor.replace_queued_value(order_2)
    assert previous_id == order_1["id"]
    assert processor.message_queue[strategy] == order_2
