import os
import sys

from apps.maker.src.structs import CancellationMessage, OrderMessage

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import json

from src.utils import (
    cancellation_from_order,
    is_cancellation_message,
    is_order_message,
    parse_message,
)


def test_parse_message(order_raw: dict[str, str]):
    parsed_order = parse_message(json.dumps(order_raw))

    assert type(parsed_order) is dict


def test_cancellation_from_order(order_1: OrderMessage):
    cancellation = cancellation_from_order(order_1)

    assert cancellation["kind"].value == "cancellation"


def test_is_cancellation_message(cancellation_1: CancellationMessage):
    assert is_cancellation_message(cancellation_1) is True


def test_is_order_message(order_1: OrderMessage):
    assert is_order_message(order_1) is True
