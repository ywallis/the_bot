from enum import Enum

class MessageType(Enum):
    ORDER = "order"
    CANCELLATION = "cancellation"


class OrderSide(Enum):
    SELL = "sell"
    BUY = "buy"
