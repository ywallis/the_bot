from typing import TypedDict
from enums import MessageType, OrderSide
from decimal import Decimal

class OrderMessage(TypedDict):
    kind: MessageType
    strategy: str
    exchange: str
    id: str
    side: OrderSide
    price: Decimal
    amount: Decimal


class CancellationMessage(TypedDict):
    kind: MessageType
    strategy: str
    exchange: str
    id: str  # Should the id be created by the strategy or by the order manager? My gut says the latter. No point in generating some for messages that get ignored.
