"""Data structures and types used across the application."""

from typing import Any, Literal, NotRequired, Protocol, TypedDict


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
    order : str, optional
        The order identifier.
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
        The fee cost (legacy/alternative field).
    fee_currency : str, optional
        The fee currency (legacy/alternative field).
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
    Protocol defining the interface for a custom exchange client.

    Attributes
    ----------
    name : str
        The name of the exchange.
    id : str
        The ID of the exchange.
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
        Create a new order on the exchange.

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
            The price for limit orders.
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
            The ID of the order to cancel.
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
        Fetch open orders from the exchange.

        Parameters
        ----------
        symbol : str | None, optional
            The trading pair symbol.
        since : int | None, optional
            Timestamp in milliseconds to fetch orders since.
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
        Watch the order book for updates.

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

    async def watch_trades(self, symbol: str) -> list[dict[str, Any]]:
        """
        Watch public trades for a symbol.

        With CCXT's default ``newUpdates`` option each call resolves with
        only the trades received since the previous call.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.

        Returns
        -------
        list[dict[str, Any]]
            New trades, oldest first.
        """
        ...

    async def watch_balance(self) -> dict[str, str | int]:
        """
        Watch the account balance for updates.

        Returns
        -------
        dict[str, str | int]
            The balance data.
        """
        ...

    async def fetch_balance(self) -> dict[str, int | dict[str, float]]:
        """
        Fetch the current account balance.

        Returns
        -------
        dict[str, int | dict[str, float]]
            The balance data.
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
            Timestamp in milliseconds to start watching from.

        Returns
        -------
        list[dict[str, str]]
            A list of order updates.
        """
        ...

    async def close(self):
        """Close the exchange connection."""
        ...

    async def load_markets(self):
        """Load market data from the exchange."""
        ...

    async def fetch_canceled_and_closed_orders(
        self,
        symbol: str,
        limit: int,
        since: int | None,
        params: dict[str, str | int | None],
    ) -> ccxtItem | list[ccxtItem]:
        """
        Fetch canceled and closed orders.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.
        limit : int
            The maximum number of orders to fetch.
        since : int | None
            Timestamp in milliseconds to fetch orders since.
        params : dict[str, str | int | None]
            Additional parameters.

        Returns
        -------
        ccxtItem | list[ccxtItem]
            The canceled and closed orders.
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
            Timestamp in milliseconds to fetch orders since.
        params : dict[str, str | int | None]
            Additional parameters.

        Returns
        -------
        ccxtItem | list[ccxtItem]
            The closed orders.
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
        Fetch the user's trades.

        Parameters
        ----------
        symbol : str
            The trading pair symbol.
        limit : int
            The maximum number of trades to fetch.
        since : int | None
            Timestamp in milliseconds to fetch trades since.
        params : dict[str, str | int | None]
            Additional parameters.

        Returns
        -------
        ccxtItem | list[ccxtItem]
            The user's trades.
        """
        ...

    async def fetch_order(self, symbol: str, id: str) -> ccxtItem:
        """
        Fetch a specific order.

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
