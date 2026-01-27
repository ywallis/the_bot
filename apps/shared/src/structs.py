"""
This module defines data structures and protocols used throughout the application,
specifically for interacting with CCXT and exchange data.
"""

from typing import Literal, NotRequired, Protocol, TypedDict


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
    Represents a generic item from a CCXT response, typically an order or trade.

    Attributes
    ----------
    order_id : str, optional
        The order ID.
    order : str, optional
        The order identifier (sometimes used alternatively to order_id).
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
    order: NotRequired[str]
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


class CustomExchange(Protocol):
    """
    Protocol definition for a custom exchange client, wrapping CCXT functionality.
    """
    name: str
    id: str

    async def create_order(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: float,
        params: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """
        Create a new order.

        Parameters
        ----------
        symbol : str
            The trading pair symbol (e.g., 'BTC/USDT').
        type : str
            The type of order (e.g., 'limit', 'market').
        side : str
            The side of the order ('buy' or 'sell').
        amount : float
            The amount to trade.
        price : float
            The price for the order.
        params : dict[str, str] | None, optional
            Additional parameters for the order.

        Returns
        -------
        dict[str, str]
            The created order details.
        """
        ...
    async def cancel_order(self, id: str, symbol: str) -> dict[str, str]:
        """
        Cancel an existing order.

        Parameters
        ----------
        id : str
            The order ID to cancel.
        symbol : str
            The trading pair symbol.

        Returns
        -------
        dict[str, str]
            The result of the cancellation.
        """
        ...

    async def fetch_open_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, str] | None = None,
    ) -> list[dict[str, str]]:
        """
        Fetch open orders.

        Parameters
        ----------
        symbol : str | None, optional
            The trading pair symbol.
        since : int | None, optional
            Timestamp in ms to fetch orders from.
        limit : int | None, optional
            The maximum number of orders to fetch.
        params : dict[str, str] | None, optional
            Additional parameters.

        Returns
        -------
        list[dict[str, str]]
            A list of open orders.
        """
        ...
    async def watch_order_book(self, symbol: str) -> dict[str, str]:
        """
        Watch the order book for a symbol.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.

        Returns
        -------
        dict[str, str]
            The order book data.
        """
        ...

    async def watch_balance(self) -> dict[str, str | int]:
        """
        Watch the account balance.

        Returns
        -------
        dict[str, str | int]
            The account balance updates.
        """
        ...
    async def fetch_balance(self) -> dict[str, int | dict[str, float]]:
        """
        Fetch the current account balance.

        Returns
        -------
        dict[str, int | dict[str, float]]
            The account balance.
        """
        ...
    async def watch_orders(self, symbol: str, since: int) -> list[dict[str, str]]:
        """
        Watch for order updates.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.
        since : int
            Timestamp in ms to start watching from.

        Returns
        -------
        list[dict[str, str]]
            A list of order updates.
        """
        ...
    async def close(self):
        """
        Close the exchange client connection.
        """
        ...
    async def load_markets(self):
        """
        Load market data from the exchange.
        """
        ...
    async def fetch_canceled_and_closed_orders(
        self,
        symbol: str,
        limit: int,
        since: int | None,
        params: dict[str, str | int | None],
    ) -> ccxtItem | list[ccxtItem]:
        """
        Fetch orders that were canceled or closed.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.
        limit : int
            The maximum number of orders to fetch.
        since : int | None
            Timestamp in ms to fetch orders from.
        params : dict[str, str | int | None]
            Additional parameters.

        Returns
        -------
        ccxtItem | list[ccxtItem]
            A single order item or a list of order items.
        """
        ...
    async def fetch_closed_orders(
        self,
        symbol: str,
        limit: int,
        since: int | None,
        params: dict[str, str | int | None],
    ) -> ccxtItem | list[ccxtItem]:
        """
        Fetch closed orders.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.
        limit : int
            The maximum number of orders to fetch.
        since : int | None
            Timestamp in ms to fetch orders from.
        params : dict[str, str | int | None]
            Additional parameters.

        Returns
        -------
        ccxtItem | list[ccxtItem]
            A single order item or a list of order items.
        """
        ...
    async def fetch_my_trades(
        self,
        symbol: str,
        limit: int,
        since: int | None,
        params: dict[str, str | int | None],
    ) -> ccxtItem | list[ccxtItem]:
        """
        Fetch user's trades.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.
        limit : int
            The maximum number of trades to fetch.
        since : int | None
            Timestamp in ms to fetch trades from.
        params : dict[str, str | int | None]
            Additional parameters.

        Returns
        -------
        ccxtItem | list[ccxtItem]
            A single trade item or a list of trade items.
        """
        ...
    async def fetch_order(self, symbol: str, id: str) -> ccxtItem:
        """
        Fetch a specific order by ID.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.
        id : str
            The order ID.

        Returns
        -------
        ccxtItem
            The order details.
        """
        ...
