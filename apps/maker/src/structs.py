from collections import deque
from decimal import Decimal
from typing import TypedDict

from apps.maker.src.enums import MessageType, OrderSide, OrderType


class OrderMessage(TypedDict):
    kind: MessageType
    strategy: str
    exchange: str
    id: str
    exchange_id: str
    pair: str
    side: OrderSide
    order_type: OrderType
    price: Decimal
    amount: Decimal


class OrderBatchMessage(TypedDict):
    kind: MessageType
    strategy: str
    id: str
    orders: list[OrderMessage]


class CancellationMessage(TypedDict):
    kind: MessageType
    strategy: str
    exchange: str
    id: str
    pair: str


class Response(TypedDict):
    kind: MessageType
    text: str


class LimitedSet:
    def __init__(self, max_size):
        self.max_size = max_size
        self.items = set()
        self.order = deque()

    def add(self, item):
        if item not in self.items:
            if len(self.order) == self.max_size:
                oldest_item = self.order.popleft()
                self.items.remove(oldest_item)
            self.order.append(item)
            self.items.add(item)

    def __contains__(self, item):
        return item in self.items

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.order)  # Optional: maintain insertion order
