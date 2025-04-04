from pathlib import Path
import tomllib
from typing import TypeGuard, Any, cast
from apps.maker.src.enums import MessageType, OrderSide, OrderType, OidComponent
from apps.maker.src.structs import (
    OrderBatchMessage,
    OrderMessage,
    CancellationMessage,
    Response,
)
from decimal import Decimal
import json

def info_from_oid(oid: str, desired_component: OidComponent) -> str:
    
    components = oid.split("-")[1].split("_")

    match desired_component:
        case OidComponent.TIME:
           return components[0] 
        case OidComponent.STRATEGY:
           return components[1] 
        case OidComponent.ORDER:
           return components[2] 
    


def load_config():
    CONFIG_PATH = Path(__file__).parents[3] / "config" / "config.toml"
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
        return config


def parse_message(
    message_raw: str | dict,
) -> OrderMessage | CancellationMessage | OrderBatchMessage | None:
    if isinstance(message_raw, dict):
        message = message_raw
    else:
        message = json.loads(message_raw)

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
                order_type=OrderType(message["order_type"]),
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
        case MessageType.ORDERBATCH.value:
            order_list: list[OrderMessage] = []
            for order in message["orders"]:
                if type(order) is not dict:
                    order = json.loads(order)
                if order.get("kind") == MessageType.ORDER.value:  # type: ignore
                    result = parse_message(order)
                    if result is not None:
                        order_list.append(cast(OrderMessage, result))
            return OrderBatchMessage(
                kind=MessageType.ORDERBATCH,
                strategy=message["strategy"],
                id=message["id"],
                orders=order_list,
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


def is_order_message(message: Any) -> TypeGuard[OrderMessage]:
    if not isinstance(message, dict):
        return False
    return message.get("kind") == MessageType.ORDER


def is_cancellation_message(message: Any) -> TypeGuard[CancellationMessage]:
    if not isinstance(message, dict):
        return False
    return message.get("kind") == MessageType.CANCELLATION


def is_orderbatch_message(message: Any) -> TypeGuard[OrderBatchMessage]:
    if not isinstance(message, dict):
        return False
    return message.get("kind") == MessageType.ORDERBATCH


def order_from_ccxt(order: dict[str, str], exchange_name: str) -> OrderMessage:
    strategy: str = order["clientOrderId"].split("-")[-1]
    if strategy[-1] == "t":
        order_type = OrderType.UNIQUE
    else:
        order_type = OrderType.REPLACE

    return OrderMessage(
        kind=MessageType.ORDER,
        strategy=strategy,
        exchange=exchange_name,
        id=order["clientOrderId"],
        exchange_id=order["id"],
        pair=order["symbol"],
        side=OrderSide(order["side"]),
        order_type=order_type,
        price=Decimal(order["price"]),
        amount=Decimal(order["amount"]),
    )
