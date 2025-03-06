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

    async def create_order(
        self, symbol: str, type: str, side: str, amount: float, price: float
    ) -> dict: ...
    async def cancel_order(self, id: str, symbol: str) -> dict: ...

    async def watch_order_book(self, symbol: str) -> dict: ...

    async def watch_balance(self) -> dict: ...


class Response(TypedDict):
    kind: MessageType
    text: str
