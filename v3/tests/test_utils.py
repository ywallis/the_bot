import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pytest
import json
from src.utils import parse_message, cancellation_from_order
from src.structs import CancellationMessage, OrderMessage
from src.enums import MessageType, OrderSide
from decimal import Decimal

order1: dict[str, str] = {
    "kind": "order",
    "strategy": "ALPHm",
    "exchange": "mexc",
    "id": "abc1",
    "pair": "ALPH/USDT",
    "side": "sell",
    "price": "3",
    "amount": "10",
} 


order2: OrderMessage = OrderMessage(kind=MessageType.ORDER, strategy="ALPHm", exchange="mexc", id="abc2", pair="ALPH/USDT", side=OrderSide.BUY, price=Decimal(4), amount=Decimal(10))

def test_parse_message():

    parsed_order = parse_message(json.dumps(order1))

    assert type(parsed_order) == dict

def test_cancellation_from_order():

    cancellation = cancellation_from_order(order2)

    assert cancellation['kind'].value == "cancellation"
