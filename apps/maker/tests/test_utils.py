import sys
import os
from maker.tests.test_data import order_raw, order_1, cancellation_1

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import json
from src.utils import is_cancellation_message, is_order_message, parse_message, cancellation_from_order

def test_parse_message():

    parsed_order = parse_message(json.dumps(order_raw))

    assert type(parsed_order) == dict

def test_cancellation_from_order():

    cancellation = cancellation_from_order(order_1)

    assert cancellation['kind'].value == "cancellation"

def test_is_cancellation_message():

    assert is_cancellation_message(cancellation_1) is True

def test_is_order_message():

    assert is_order_message(order_raw) is True
