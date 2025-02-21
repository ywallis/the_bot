from typing import TypedDict, Protocol
from enums import MessageType, OrderSide
from decimal import Decimal

class OrderMessage(TypedDict):
    kind: MessageType
    strategy: str
    exchange: str
    id: str
    exchange_id: str
    pair: str
    side: OrderSide
    price: Decimal
    amount: Decimal


class CancellationMessage(TypedDict):
    kind: MessageType
    strategy: str
    exchange: str
    id: str
    pair: str

class CustomExchange(Protocol):
    name: str
    async def create_limit_order(self, symbol: str, side: str, amount: float, price: float) -> dict: ...
    async def cancel_order(self, id: str, symbol: str) -> dict: ...

