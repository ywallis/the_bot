"""Tests for utility functions."""

import os
import sys

from apps.maker.src.structs import CancellationMessage, OrderBatchMessage, OrderMessage

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import json

from src.utils import (
    cancellation_from_order,
    is_cancellation_message,
    is_order_message,
    is_orderbatch_message,
    order_from_ccxt,
    parse_message,
)


def test_parse_message(order_raw: dict[str, str]):
    """Test parsing a single order message."""
    parsed_order = parse_message(json.dumps(order_raw))

    assert type(parsed_order) is dict


def test_parse_message_batch(order_batch_raw: dict[str, str | list]):
    """Test parsing an order batch message."""
    parsed_order = parse_message(json.dumps(order_batch_raw))

    assert type(parsed_order) is dict


def test_cancellation_from_order(order_1: OrderMessage):
    """Test creating a cancellation from an order."""
    cancellation = cancellation_from_order(order_1)

    assert cancellation["kind"].value == "cancellation"


def test_is_cancellation_message(cancellation_1: CancellationMessage):
    """Test identification of cancellation messages."""
    assert is_cancellation_message(cancellation_1) is True


def test_is_order_message(order_1: OrderMessage):
    """Test identification of order messages."""
    assert is_order_message(order_1) is True


def test_is_orderbatch_message(order_batch_1: OrderBatchMessage):
    """Test identification of order batch messages."""
    assert is_orderbatch_message(order_batch_1) is True


def test_order_from_ccxt(order_1: OrderMessage):
    """Test conversion from CCXT order to OrderMessage."""
    exchange_name = "gate"
    order_ccxt = {
        "id": "_",
        "clientOrderId": "t-2025_lab_eb",
        "symbol": "ALPH/USDT",
        "side": "sell",
        "price": "100",
        "amount": "10",
    }
    assert order_from_ccxt(order_ccxt, exchange_name) == order_1
