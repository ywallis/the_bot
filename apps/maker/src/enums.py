"""Module containing enumerations for the maker application."""

from enum import Enum


class CustomEnum(Enum):
    """Custom base enum class."""

    def __str__(self) -> str:
        """Represent the enum as a string.

        Returns
        -------
        str
            The string representation of the enum

        """
        return self.value


class MessageType(CustomEnum):
    """A message to or from the message processor.

    Attributes
    ----------
    ORDER : A regular order
    ORDERBATCH : A message containing multiple orders
    CANCELLATION : A cancellation
    ERROR : An error

    """

    ORDER = "order"
    ORDERBATCH = "orderbatch"
    CANCELLATION = "cancellation"
    ERROR = "error"


class OrderSide(CustomEnum):
    """The side of an order.

    Attributes
    ----------
    SELL : A sell order
    BUY : A buy order

    """

    SELL = "sell"
    BUY = "buy"


class OrderType(CustomEnum):
    """The type of an order.

    Attributes
    ----------
    REPLACE : A hanging limit order meant to be replaced
    UNIQUE : A limit order meant to be executed once and not cancelled
    MARKET : An immediate market order

    """

    REPLACE = "replace"
    UNIQUE = "unique"
    MARKET = "market"


class OidComponent(CustomEnum):
    """The possible components of the custom oid.

    Attributes
    ----------
    TIME : The time at which the order was generated
    STRATEGY : The strategy the order belongs to
    ORDER : The unique order identifier

    """

    TIME = "time"
    STRATEGY = "strategy"
    ORDER = "order"
