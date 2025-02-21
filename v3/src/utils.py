from ccxt.base import exchange
from enums import MessageType, OrderSide
from structs import OrderMessage, CancellationMessage
from decimal import Decimal
import json


def parse_message(message_raw: str) -> OrderMessage | CancellationMessage | None:

    message: dict[str, str] = json.loads(message_raw)
    match message.get("kind"):
        case "order":
            return OrderMessage(
                kind=MessageType.ORDER,
                strategy=message["strategy"],
                exchange=message["exchange"],
                id=message["id"],
                exchange_id=message['exchange_id'],
                pair=message['pair'],
                side=OrderSide(message["side"]),
                price=Decimal(message["price"]),
                amount=Decimal(message["amount"]),
            )
        case "cancellation":
            return CancellationMessage(
                kind=MessageType.CANCELLATION,
                strategy=message["strategy"],
                exchange=message["exchange"],
                id=message["id"],
                pair=message['pair'],
            )


def cancellation_from_order(order: OrderMessage) -> CancellationMessage:

    return CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=order["strategy"],
        exchange=order["exchange"],
        id=order["id"],
        pair=order['pair'],
    )
