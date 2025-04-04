from collections import deque
from decimal import Decimal
from typing import Protocol, TypedDict

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


class CustomExchange(Protocol):
    name: str
    id: str

    async def create_order(
        self, symbol: str, type: str, side: str, amount: float, price: float
    ) -> dict: ...
    async def cancel_order(self, id: str, symbol: str) -> dict: ...

    async def fetch_open_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params={},
    ) -> list[dict[str, str]]: ...
    async def watch_order_book(self, symbol: str) -> dict: ...

    async def watch_balance(self) -> dict: ...
    async def fetch_balance(self) -> dict: ...
    async def watch_orders(self, symbol: str, since: int) -> list[dict]: ...
    async def close(self): ...
    

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
