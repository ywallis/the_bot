from typing import TypedDict, Literal, NotRequired

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

