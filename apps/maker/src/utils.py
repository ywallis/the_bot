from pathlib import Path
import tomllib
from typing import TypeGuard, Any
from enums import MessageType, OrderSide
from structs import OrderMessage, CancellationMessage, Response
from decimal import Decimal
import json


def load_config():
    CONFIG_PATH = Path(__file__).parents[3] / "config" / "config.toml"
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
        return config


def parse_message(
    message_raw: str,
) -> OrderMessage | CancellationMessage | None:

    message: dict[str, str] = json.loads(message_raw)
    match message.get("kind"):
        case MessageType.ORDER.value:
            return OrderMessage(
                kind=MessageType.ORDER,
                strategy=message["strategy"],
                exchange=message["exchange"],
                id=message["id"],
                exchange_id=message["exchange_id"],
                pair=message["pair"],
                side=OrderSide(message["side"]),
                price=Decimal(message["price"]),
                amount=Decimal(message["amount"]),
            )
        case MessageType.CANCELLATION.value:
            return CancellationMessage(
                kind=MessageType.CANCELLATION,
                strategy=message["strategy"],
                exchange=message["exchange"],
                id=message["id"],
                pair=message["pair"],
            )


def cancellation_from_order(order: OrderMessage) -> CancellationMessage:

    return CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=order["strategy"],
        exchange=order["exchange"],
        id=order["exchange_id"],
        pair=order["pair"],
    )


def identify_response(string: str) -> Response:

    if string == "":
        return Response(kind=MessageType.ERROR, text="Redis connection failed")

    items = string.split("|")
    return Response(kind=MessageType(items[0]), text=items[1])


def is_order_message(
    message: CancellationMessage | OrderMessage | Any,
) -> TypeGuard[OrderMessage]:
    return message.get("kind").value == MessageType.ORDER.value


def is_cancellation_message(
    message: CancellationMessage | OrderMessage | Any,
) -> TypeGuard[CancellationMessage]:
    return message.get("kind").value == MessageType.CANCELLATION.value
