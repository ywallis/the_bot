import json
from decimal import Decimal

import pytest
from copy import deepcopy

from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.structs import CancellationMessage, OrderMessage, Response


@pytest.fixture
def order_raw_item():
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
def order_raw(order_raw_item):
    return deepcopy(order_raw_item)


@pytest.fixture
def order_raw_string(order_raw_item):
    order_raw_string = json.dumps(order_raw_item)
    return deepcopy(order_raw_string)


@pytest.fixture
def order_1():
    """This fixture returns a deepcopy of an OrderMessage"""
    order_1: OrderMessage = OrderMessage(
        kind=MessageType.ORDER,
        strategy="ALPH_gate",
        exchange="gate",
        id="gate_test_1",
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
    order_2: OrderMessage = OrderMessage(
        kind=MessageType.ORDER,
        strategy="ALPH_gate",
        exchange="gate",
        id="gate_test_2",
        exchange_id="_",
        pair="ALPH/USDT",
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal(100),
        amount=Decimal(10),
    )
    return deepcopy(order_2)


@pytest.fixture
def cancellation_1():
    cancellation_1: CancellationMessage = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy="ALPH_gate",
        exchange="gate",
        id="gate_test_2",
        pair="ALPH/USDT",
    )
    return deepcopy(cancellation_1)


@pytest.fixture
def order_1_response_positive():
    order_1_response_positive: Response = Response(
        kind=MessageType.ORDER, text='{"id": "mock_response_1"}'
    )
    return deepcopy(order_1_response_positive)
