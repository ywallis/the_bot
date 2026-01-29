"""Data structures for the maker application."""

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
        List of ask price and volume.
    bids : list[list[float]]
        List of bid price and volume.
    datetime : str
        ISO 8601 datetime string.
    timestamp : int
        Timestamp in milliseconds.
    nonce : int
        Nonce for synchronization.
    symbol : str
        The trading pair symbol.
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
        The type of message.
    strategy : str
        The strategy identifier.
    exchange : str
        The exchange name.
    id : str
        The order ID.
    exchange_id : str
        The exchange-assigned order ID.
    pair : str
        The trading pair symbol.
    side : OrderSide
        The side of the order (buy/sell).
    order_type : OrderType
        The type of order.
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
        The type of message.
    strategy : str
        The strategy identifier.
    id : str
        The batch ID.
    orders : list[OrderMessage]
        The list of orders.
    """

    kind: MessageType
    strategy: str
    id: str
    orders: list[OrderMessage]


class CancellationMessage(TypedDict):
    """
    Represents a cancellation message.

    Attributes
    ----------
    kind : MessageType
        The type of message.
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
    Represents a response from the broker.

    Attributes
    ----------
    kind : MessageType
        The type of response.
    text : str
        The response text.
    """

    kind: MessageType
    text: str


class LimitedSet:
    """
    A set with a maximum size that evicts the oldest items.

    Attributes
    ----------
    max_size : int
        The maximum number of items.
    items : set
        The set of items.
    order : deque
        The queue maintaining insertion order.
    """

    def __init__(self, max_size):
        """
        Initialize the LimitedSet.

        Parameters
        ----------
        max_size : int
            The maximum number of items to hold.
        """
        self.max_size = max_size
        self.items = set()
        self.order = deque()

    def add(self, item):
        """
        Add an item to the set.

        If the set is full, the oldest item is removed.

        Parameters
        ----------
        item : Any
            The item to add.
        """
        if item not in self.items:
            if len(self.order) == self.max_size:
                oldest_item = self.order.popleft()
                self.items.remove(oldest_item)
            self.order.append(item)
            self.items.add(item)

    def __contains__(self, item):
        """Check if an item is in the set."""
        return item in self.items

    def __len__(self):
        """Return the number of items in the set."""
        return len(self.items)

    def __iter__(self):
        """Iterate over the items in insertion order."""
        return iter(self.order)  # Optional: maintain insertion order
