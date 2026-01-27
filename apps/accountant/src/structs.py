"""
This module defines data structures used within the accountant application.
"""

from typing import TypedDict, Literal, NotRequired

class ccxtFee(TypedDict):
    """
    Represents the fee structure from a CCXT response.

    Attributes
    ----------
    cost : str
        The fee cost.
    currency : str
        The currency of the fee.
    """
    cost: str
    currency: str

class ccxtItem(TypedDict):
    """
    Represents a generic item from a CCXT response, typically an order or trade,
    enriched with fields for database export.

    Attributes
    ----------
    order_id : str, optional
        The order ID.
    fee : ccxtFee, optional
        The fee associated with the item.
    fees : list[ccxtFee], optional
        A list of fees associated with the item.
    cost : str, optional
        The total cost.
    amount : str, optional
        The amount traded or ordered.
    side : Literal["buy", "sell"], optional
        The side of the trade/order.
    fee_cost : str, optional
        The cost of the fee.
    fee_currency : str, optional
        The currency of the fee.
    usdt_value : str, optional
        The value in USDT.
    asset_net_q : str, optional
        The net quantity of the asset.
    exchange : str, optional
        The name of the exchange.
    """
    order_id: NotRequired[str]
    fee: NotRequired[ccxtFee]
    fees: NotRequired[list[ccxtFee]]
    cost: NotRequired[str]
    amount: NotRequired[str]
    side: NotRequired[Literal["buy", "sell"]]
    fee_cost: NotRequired[str]
    fee_currency: NotRequired[str]
    usdt_value: NotRequired[str]
    asset_net_q: NotRequired[str]
    exchange: NotRequired[str]
