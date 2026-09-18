"""CCXT order listing helpers shared by the maker and the accountant.

Venues differ in what they expose, so what to fetch is read from the
client's ``has`` map rather than decided per venue name.
"""

from typing import Any


async def fetch_orders_since(
    client: Any, ticker: str, since: int
) -> list[dict[str, Any]]:
    """
    Fetch every order of ours that changed since a time, over REST.

    Venues differ in what they expose. One call to ``fetch_orders`` where
    the venue has it; otherwise the open orders plus whatever closed or
    cancelled ones the venue can list, which between them cover every state
    an order can be in.

    Parameters
    ----------
    client : Any
        The exchange client. Typed loosely because the optional endpoints
        are looked up on the client's ``has`` map.
    ticker : str
        The trading pair symbol.
    since : int
        Milliseconds since the epoch.

    Returns
    -------
    list[dict[str, Any]]
        CCXT unified orders.
    """
    has = getattr(client, "has", {}) or {}
    if has.get("fetchOrders"):
        return list(await client.fetch_orders(ticker, since=since))
    orders: list[dict[str, Any]] = list(await client.fetch_open_orders(ticker))
    if has.get("fetchCanceledAndClosedOrders"):
        orders += await client.fetch_canceled_and_closed_orders(ticker, since=since)
    else:
        if has.get("fetchClosedOrders"):
            orders += await client.fetch_closed_orders(ticker, since=since)
        if has.get("fetchCanceledOrders"):
            orders += await client.fetch_canceled_orders(ticker, since=since)
    return orders
