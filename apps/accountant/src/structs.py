"""Data structures for the accountant application."""

from typing import TypedDict, Literal, NotRequired

class ccxtFee(TypedDict):
    """
    Represents the fee structure returned by CCXT.

    Attributes
    ----------
    cost : str
        The cost of the fee.
    currency : str
        The currency of the fee.
    """

    cost: str
    currency: str

class ccxtItem(TypedDict):
    """
    Represents a standardized item (order, trade, etc.) from CCXT.

    Attributes
    ----------
    order_id : str, optional
        The ID of the order.
    fee : ccxtFee, optional
        The fee information.
    fees : list[ccxtFee], optional
        A list of fees.
    cost : str, optional
        The total cost of the item.
    amount : str, optional
        The amount of the item.
    side : Literal["buy", "sell"], optional
        The side of the trade (buy or sell).
    fee_cost : str, optional
        The fee cost.
    fee_currency : str, optional
        The fee currency.
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
