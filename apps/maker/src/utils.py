"""
This module provides utility functions for the maker application, including
OID parsing, message parsing, and type guards.
"""

import json
from decimal import Decimal
from typing import Any, TypeGuard, cast

from apps.maker.src.enums import MessageType, OidComponent, OrderSide, OrderType
from apps.maker.src.structs import (
    CancellationMessage,
    OrderBatchMessage,
    OrderMessage,
    Response,
)


def info_from_oid(oid: str, desired_component: OidComponent) -> str:
    """
    Extract specific information from an Order ID (OID).

    Parameters
    ----------
    oid : str
        The Order ID to parse.
    desired_component : OidComponent
        The component to extract (TIME, STRATEGY, or ORDER).

    Returns
    -------
    str
        The extracted component value.

    Raises
    ------
    Exception
        If the OID format is invalid.
    """
    components = oid.split("-")[1].split("_")

    if len(components) != 3:
        raise Exception("Invalid OID!")

    match desired_component:
        case OidComponent.TIME:
            return components[0]
        case OidComponent.STRATEGY:
            return components[1]
        case OidComponent.ORDER:
            return components[2]


def parse_message(
    message_raw: str | dict,
) -> OrderMessage | CancellationMessage | OrderBatchMessage | None:
    """
    Parse a raw message into a structured message object.

    Parameters
    ----------
    message_raw : str | dict
        The raw message, either as a JSON string or a dictionary.

    Returns
    -------
    OrderMessage | CancellationMessage | OrderBatchMessage | None
        The parsed message object, or None if parsing fails.
    """
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
    """
    Create a cancellation message from an existing order message.

    Parameters
    ----------
    order : OrderMessage
        The order message to be cancelled.

    Returns
    -------
    CancellationMessage
        The resulting cancellation message.
    """
    return CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=order["strategy"],
        exchange=order["exchange"],
        id=order["exchange_id"],
        pair=order["pair"],
    )


def identify_response(string: str) -> Response:
    """
    Parse a response string from the broker.

    Parameters
    ----------
    string : str
        The raw response string.

    Returns
    -------
    Response
        The parsed response object.
    """
    if string == "":
        return Response(kind=MessageType.ERROR, text="Redis connection failed")

    items = string.split("|")
    return Response(kind=MessageType(items[0]), text=items[1])


def is_order_message(message: Any) -> TypeGuard[OrderMessage]:
    """
    Check if a message is an OrderMessage.

    Parameters
    ----------
    message : Any
        The message to check.

    Returns
    -------
    TypeGuard[OrderMessage]
        True if the message is an OrderMessage, False otherwise.
    """
    if not isinstance(message, dict):
        return False
    return message.get("kind") == MessageType.ORDER


def is_cancellation_message(message: Any) -> TypeGuard[CancellationMessage]:
    """
    Check if a message is a CancellationMessage.

    Parameters
    ----------
    message : Any
        The message to check.

    Returns
    -------
    TypeGuard[CancellationMessage]
        True if the message is a CancellationMessage, False otherwise.
    """
    if not isinstance(message, dict):
        return False
    return message.get("kind") == MessageType.CANCELLATION


def is_orderbatch_message(message: Any) -> TypeGuard[OrderBatchMessage]:
    """
    Check if a message is an OrderBatchMessage.

    Parameters
    ----------
    message : Any
        The message to check.

    Returns
    -------
    TypeGuard[OrderBatchMessage]
        True if the message is an OrderBatchMessage, False otherwise.
    """
    if not isinstance(message, dict):
        return False
    return message.get("kind") == MessageType.ORDERBATCH


def order_from_ccxt(order: dict[str, str], exchange_name: str) -> OrderMessage:
    """
    Convert a CCXT order dictionary to an OrderMessage.

    Parameters
    ----------
    order : dict[str, str]
        The CCXT order dictionary.
    exchange_name : str
        The name of the exchange.

    Returns
    -------
    OrderMessage
        The converted OrderMessage.
    """
    strategy_identifier = info_from_oid(order["clientOrderId"], OidComponent.STRATEGY)
    order_identifier = info_from_oid(order["clientOrderId"], OidComponent.ORDER)
    if order_identifier[-1] == "t":
        order_type = OrderType.UNIQUE
    else:
        order_type = OrderType.REPLACE

    strategy = strategy_identifier + "_" + order_identifier
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
