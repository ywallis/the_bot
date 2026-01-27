"""
This module defines Enumerations used for message types, order sides, order types,
and order ID components within the maker application.
"""

from enum import Enum


class MessageType(Enum):
    """
    Enum representing different types of messages in the system.
    """
    ORDER = "order"
    ORDERBATCH = "orderbatch"
    CANCELLATION = "cancellation"
    ERROR = "error"

    def __str__(self):
        return self.value


class OrderSide(Enum):
    """
    Enum representing the side of an order (buy or sell).
    """
    SELL = "sell"
    BUY = "buy"

    def __str__(self):
        return self.value


class OrderType(Enum):
    """
    Enum representing the type of order strategy or execution.
    """
    REPLACE = "replace"
    UNIQUE = "unique"
    MARKET = "market"

    def __str__(self):
        return self.value

class OidComponent(Enum):
    """
    Enum representing components of an Order ID.
    """
    TIME = "time"
    STRATEGY = "strategy"
    ORDER = "order"

    def __str__(self):
        return self.value
