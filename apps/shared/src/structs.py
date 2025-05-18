from typing import Literal, NotRequired, Protocol, TypedDict


class ccxtFee(TypedDict):
    cost: str
    currency: str


class ccxtItem(TypedDict):
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


class CustomExchange(Protocol):
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
    ) -> dict[str, str]: ...
    async def cancel_order(self, id: str, symbol: str) -> dict[str, str]: ...

    async def fetch_open_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, str] | None = None,
    ) -> list[dict[str, str]]: ...
    async def watch_order_book(self, symbol: str) -> dict[str, str]: ...

    async def watch_balance(self) -> dict[str, str | int]: ...
    async def fetch_balance(self) -> dict[str, int | dict[str, float]]: ...
    async def watch_orders(self, symbol: str, since: int) -> list[dict[str, str]]: ...
    async def close(self): ...
    async def load_markets(self): ...
    async def fetch_canceled_and_closed_orders(
        self, symbol: str, limit: int, since: str | None, params: dict[str, str | None]
    ) -> ccxtItem | list[ccxtItem]: ...
    async def fetch_closed_orders(
        self, symbol: str, limit: int, since: str | None, params: dict[str, str | None]
    ) -> ccxtItem | list[ccxtItem]: ...
    async def fetch_my_trades(
        self, symbol: str, limit: int, since: str | None, params: dict[str, str | None]
    ) -> ccxtItem | list[ccxtItem]: ...
