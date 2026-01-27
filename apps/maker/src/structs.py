"""
This module defines the data structures used within the maker application.
It includes TypedDicts for order-related messages and utility classes like LimitedSet.
"""

from collections import deque
from decimal import Decimal
from typing import TypedDict

from apps.maker.src.enums import MessageType, OrderSide, OrderType


class OrderBook(TypedDict):
    """
    Represents an order book snapshot.

    Attributes
    ----------
    asks : list[list[float]]
        List of ask orders [price, amount].
    bids : list[list[float]]
        List of bid orders [price, amount].
    datetime : str
        ISO format datetime string.
    timestamp : int
        Unix timestamp in milliseconds.
    nonce : int
        Nonce of the update.
    symbol : str
        Trading pair symbol.
    """
    asks: list[list[float]]
    bids: list[list[float]]
    datetime: str
    timestamp: int
    nonce: int
    symbol: str


class OrderMessage(TypedDict):
    """
    Represents an order message.

    Attributes
    ----------
    kind : MessageType
        The type of message (ORDER).
    strategy : str
        The strategy identifier.
    exchange : str
        The exchange name.
    id : str
        The client order ID.
    exchange_id : str
        The exchange-assigned order ID.
    pair : str
        The trading pair symbol.
    side : OrderSide
        The side of the order (BUY/SELL).
    order_type : OrderType
        The type of order (LIMIT/MARKET etc).
    price : Decimal
        The price of the order.
    amount : Decimal
        The amount of the order.
    """
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
    """
    Represents a batch of order messages.

    Attributes
    ----------
    kind : MessageType
        The type of message (ORDERBATCH).
    strategy : str
        The strategy identifier.
    id : str
        The batch ID.
    orders : list[OrderMessage]
        The list of orders in the batch.
    """
    kind: MessageType
    strategy: str
    id: str
    orders: list[OrderMessage]


class CancellationMessage(TypedDict):
    """
    Represents an order cancellation message.

    Attributes
    ----------
    kind : MessageType
        The type of message (CANCELLATION).
    strategy : str
        The strategy identifier.
    exchange : str
        The exchange name.
    id : str
        The order ID to cancel.
    pair : str
        The trading pair symbol.
    """
    kind: MessageType
    strategy: str
    exchange: str
    id: str
    pair: str


class Response(TypedDict):
    """
    Represents a response message from the broker.

    Attributes
    ----------
    kind : MessageType
        The type of the response.
    text : str
        The content of the response.
    """
    kind: MessageType
    text: str


class LimitedSet:
    """
    A set with a maximum size that evicts the oldest items when full.
    """
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
