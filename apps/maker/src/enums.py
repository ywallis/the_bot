from enum import Enum


class MessageType(Enum):
    ORDER = "order"
    ORDERBATCH = "orderbatch"
    CANCELLATION = "cancellation"
    ERROR = "error"

    def __str__(self):
        return self.value


class OrderSide(Enum):
    SELL = "sell"
    BUY = "buy"

    def __str__(self):
        return self.value


class OrderType(Enum):
    REPLACE = "replace"
    UNIQUE = "unique"
    MARKET = "market"

    def __str__(self):
        return self.value

class OidComponent(Enum):
    TIME = "time"
    STRATEGY = "strategy"
    ORDER = "order"

    def __str__(self):
        return self.value
