from enum import Enum

class MessageType(Enum):
    ORDER = "order"
    CANCELLATION = "cancellation"
    CONFIRMATION = "confirmation"
    ERROR = "error"
    def __str__(self):
        return self.value  # Ensures JSON serialization works naturally



class OrderSide(Enum):
    SELL = "sell"
    BUY = "buy"
    def __str__(self):
        return self.value  # Ensures JSON serialization works naturally

