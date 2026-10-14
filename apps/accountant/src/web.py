"""
Serve the accountant's status as a web page.

The page shows what ``status.py`` and ``dashboard.py`` print in a terminal:
open orders, balances, imbalances without a pending order, and the daily and
monthly performance from the database. A background task refreshes one
snapshot every ``REFRESH_S`` seconds, so a page load never calls a venue.
"""

import argparse
import asyncio
import contextlib
import dataclasses
import html
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from aiohttp import web
from pandas import DataFrame
from psycopg import sql

import apps.shared.src.logging_config as logging_config
from apps.accountant.src.sql_connector import QueryLoader
from apps.accountant.src.utils import load_pg_config, pg_connect
from apps.shared.src.exchange_clients import authenticated_clients, load_clients
from apps.shared.src.structs import CustomExchange
from apps.shared.src.utils import exchange_and_pair

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# Seconds between two snapshots. The page reloads itself at the same pace.
REFRESH_S = 15

# Rows of the daily and monthly overviews to show per symbol.
DAILY_ROWS = 7
MONTHLY_ROWS = 3

# Client order ids the order manager makes start with this. Imbalances on
# other ids come from orders placed by hand, such as inventory purchases.
BOT_ORDER_PREFIX = "t-"

LAST_PRICE_QUERY = sql.SQL(
    "SELECT price FROM trades WHERE symbol = %(symbol)s ORDER BY datetime DESC LIMIT 1"
)


@dataclass
class Snapshot:
    """
    Everything the page shows, taken at one time.

    Attributes
    ----------
    taken_at : datetime | None
        When the snapshot was taken, None before the first one.
    open_orders : list[dict[str, str]]
        One row per open order.
    balances : list[dict[str, str]]
        One row per venue and non-zero asset.
    imbalances : list[dict[str, str]]
        One row per bot client order id whose trades do not net out.
    manual_imbalances : int
        Imbalances on client order ids the bot did not make.
    daily : list[dict[str, str]]
        Daily performance rows of every symbol.
    monthly : list[dict[str, str]]
        Monthly performance rows of every symbol.
    open_orders_complete : bool
        False when a venue's open orders could not be listed, so an
        imbalance missing from ``open_orders`` may still be pending.
    database_ok : bool
        False when the database could not be queried.
    errors : list[str]
        What failed while the snapshot was taken.
    """

    taken_at: datetime | None = None
    open_orders: list[dict[str, str]] = field(default_factory=list)
    balances: list[dict[str, str]] = field(default_factory=list)
    imbalances: list[dict[str, str]] = field(default_factory=list)
    manual_imbalances: int = 0
    daily: list[dict[str, str]] = field(default_factory=list)
    monthly: list[dict[str, str]] = field(default_factory=list)
    open_orders_complete: bool = True
    database_ok: bool = True
    errors: list[str] = field(default_factory=list)


@dataclass
class Latest:
    """
    Holder of the snapshot the page shows.

    aiohttp deprecates replacing app state once the app has started, so
    the app holds this object and the refresh task replaces its content.

    Attributes
    ----------
    snapshot : Snapshot
        The snapshot to show.
    """

    snapshot: Snapshot = field(default_factory=Snapshot)


# Where the app keeps the latest snapshot.
LATEST = web.AppKey("latest", Latest)


def fmt(value: object, places: int = 4) -> str:
    """
    Format a figure for the page.

    Parameters
    ----------
    value : object
        A number, possibly a string, a Decimal or None.
    places : int, optional
        Decimal places. Defaults to 4.

    Returns
    -------
    str
        The figure rounded to ``places``, or the value as text if it is
        not a number, or an empty string for None.
    """
    if value is None:
        return ""
    try:
        return f"{Decimal(str(value)):.{places}f}"
    except InvalidOperation:
        return str(value)


def order_rows(venue: str, orders: list[dict]) -> list[dict[str, str]]:
    """
    Turn CCXT open orders into page rows.

    Parameters
    ----------
    venue : str
        The venue id.
    orders : list[dict]
        CCXT unified orders.

    Returns
    -------
    list[dict[str, str]]
        One row per order.
    """
    return [
        {
            "venue": venue,
            "symbol": str(order.get("symbol") or ""),
            "side": str(order.get("side") or ""),
            "price": fmt(order.get("price"), 6),
            "remaining": fmt(order.get("remaining"), 4),
            "amount": fmt(order.get("amount"), 4),
            "client order id": str(order.get("clientOrderId") or ""),
            "placed": str(order.get("datetime") or ""),
        }
        for order in orders
    ]


def balance_rows(venue: str, balance: dict) -> list[dict[str, str]]:
    """
    Turn a CCXT balance into page rows, leaving out empty assets.

    Parameters
    ----------
    venue : str
        The venue id.
    balance : dict
        A CCXT unified balance.

    Returns
    -------
    list[dict[str, str]]
        One row per asset with a non-zero total.
    """
    totals = balance.get("total") or {}
    free = balance.get("free") or {}
    used = balance.get("used") or {}
    return [
        {
            "venue": venue,
            "asset": asset,
            "free": fmt(free.get(asset), 6),
            "used": fmt(used.get(asset), 6),
            "total": fmt(total, 6),
        }
        for asset, total in sorted(totals.items())
        if total
    ]


def imbalance_rows(
    imbalances: DataFrame | None,
    open_ids: set[str],
    open_orders_complete: bool = True,
) -> tuple[list[dict[str, str]], int]:
    """
    Turn the ``fetch_imbalances`` result into page rows.

    An imbalance is pending when an open order carries its client order
    id, because that order closes it when it fills. If the open orders of
    some venue could not be listed, an imbalance without a listed order is
    unknown rather than unaddressed: its order may rest on that venue.
    Imbalances on ids the bot did not make are counted, not listed.

    Parameters
    ----------
    imbalances : DataFrame | None
        Result of the ``fetch_imbalances`` query, None when it is empty.
    open_ids : set[str]
        Client order ids of every open order.
    open_orders_complete : bool, optional
        Whether every venue's open orders were listed. Defaults to True.

    Returns
    -------
    tuple[list[dict[str, str]], int]
        Rows for bot orders, and the number of other imbalances.
    """
    if imbalances is None:
        return [], 0
    rows: list[dict[str, str]] = []
    manual = 0
    for record in imbalances.to_dict("records"):
        client_order_id = record.get("clientorderid")
        if not isinstance(client_order_id, str) or not client_order_id.startswith(
            BOT_ORDER_PREFIX
        ):
            manual += 1
            continue
        delta = Decimal(str(record["delta"]))
        if client_order_id in open_ids:
            state = "pending"
        elif open_orders_complete:
            state = "unaddressed"
        else:
            state = "unknown"
        rows.append(
            {
                "symbol": str(record["symbol"]),
                "client order id": client_order_id,
                "delta": fmt(delta, 4),
                "quote value": fmt(record.get("usdt_delta"), 2),
                "to close": "sell" if delta > 0 else "buy",
                "state": state,
            }
        )
    return rows, manual


def performance_rows(
    overview: DataFrame | None, period: str, last_price: Decimal | None
) -> list[dict[str, str]]:
    """
    Turn a daily or monthly overview into page rows.

    The quote gain alone counts inventory bought as a loss. The marked
    column adds the inventory change valued at the last trade price.

    Parameters
    ----------
    overview : DataFrame | None
        Result of ``daily_overview`` or ``monthly_overview``.
    period : str
        Name of the period column in ``overview``: ``date`` or ``month``.
    last_price : Decimal | None
        Last trade price of the symbol, None if it has no trade.

    Returns
    -------
    list[dict[str, str]]
        One row per period.
    """
    if overview is None:
        return []
    gain_column = "usdt_gain" if "usdt_gain" in overview else "total_usdt_gain"
    delta_column = (
        "inventory_delta" if "inventory_delta" in overview else "total_inventory_delta"
    )
    rows: list[dict[str, str]] = []
    for record in overview.to_dict("records"):
        gain = Decimal(str(record[gain_column] or 0))
        delta = Decimal(str(record[delta_column] or 0))
        marked = gain + delta * last_price if last_price is not None else None
        row = {
            "symbol": str(record["symbol"]),
            period: str(record[period]),
            "quote gain": fmt(gain, 4),
            "inventory delta": fmt(delta, 4),
            "marked at last price": fmt(marked, 4),
        }
        if "item_count" in record:
            row["trades"] = str(record["item_count"])
        rows.append(row)
    return rows


async def fetch_venue(
    venue: str, client: CustomExchange, symbols: list[str], snapshot: Snapshot
) -> None:
    """
    Add the open orders and balances of one venue to a snapshot.

    Parameters
    ----------
    venue : str
        The venue id.
    client : CustomExchange
        Its authenticated client.
    symbols : list[str]
        Symbols to list open orders on.
    snapshot : Snapshot
        The snapshot to fill.
    """
    for symbol in symbols:
        try:
            orders = await client.fetch_open_orders(symbol)
            snapshot.open_orders.extend(order_rows(venue, orders))
        except Exception as e:
            snapshot.open_orders_complete = False
            snapshot.errors.append(f"{venue} {symbol} open orders: {e}")
    try:
        snapshot.balances.extend(balance_rows(venue, await client.fetch_balance()))
    except Exception as e:
        snapshot.errors.append(f"{venue} balance: {e}")


def run_query(
    pg_config: dict[str, str],
    query: sql.SQL | sql.Composed,
    parameters: dict[str, object],
) -> DataFrame | None:
    """
    Run one query and return its rows.

    Unlike ``send_sql_query``, a database error is raised, not printed: an
    empty table must mean no row, never an unreachable database.

    Parameters
    ----------
    pg_config : dict[str, str]
        PostgreSQL configuration.
    query : sql.SQL | sql.Composed
        The query.
    parameters : dict[str, object]
        Its named parameters.

    Returns
    -------
    DataFrame | None
        The rows, or None when there is none.
    """
    with pg_connect(pg_config) as conn, conn.cursor() as cur:
        cur.execute(query, parameters)
        rows = cur.fetchall()
        if not rows or cur.description is None:
            return None
        return DataFrame(rows, columns=[d.name for d in cur.description])


def named_query(queries: QueryLoader, name: str) -> sql.SQL | sql.Composed:
    """
    Return a loaded query with its ``{symbol}`` and ``{range}`` bound.

    Parameters
    ----------
    queries : QueryLoader
        Loaded queries.
    name : str
        File name of the query without ``.sql``.

    Returns
    -------
    sql.SQL | sql.Composed
        The query.

    Raises
    ------
    Exception
        If no query has that name.
    """
    text = queries.get_query(name)
    if text is None:
        raise Exception(f"Query {name} could not be loaded")
    if "{symbol}" not in text:
        return sql.SQL(text)
    return sql.SQL(text).format(
        symbol=sql.Placeholder("symbol"), range=sql.Placeholder("range")
    )


def query_database(
    pg_config: dict[str, str], queries: QueryLoader, symbols: list[str]
) -> dict[
    str, tuple[DataFrame | None, DataFrame | None, DataFrame | None, Decimal | None]
]:
    """
    Run the database queries of every symbol.

    Parameters
    ----------
    pg_config : dict[str, str]
        PostgreSQL configuration.
    queries : QueryLoader
        Loaded queries.
    symbols : list[str]
        Symbols to query.

    Returns
    -------
    dict
        Per symbol: imbalances, daily overview, monthly overview and the
        last trade price, each None when the query returned nothing.

    Raises
    ------
    psycopg.Error
        If the database cannot be queried.
    """
    imbalances = named_query(queries, "fetch_imbalances")
    daily = named_query(queries, "daily_overview")
    monthly = named_query(queries, "monthly_overview")
    results = {}
    for symbol in symbols:
        last = run_query(pg_config, LAST_PRICE_QUERY, {"symbol": symbol})
        results[symbol] = (
            run_query(pg_config, imbalances, {"symbol": symbol}),
            run_query(pg_config, daily, {"symbol": symbol, "range": DAILY_ROWS}),
            run_query(pg_config, monthly, {"symbol": symbol, "range": MONTHLY_ROWS}),
            Decimal(str(last["price"].iloc[0])) if last is not None else None,
        )
    return results


async def take_snapshot(
    clients: dict[str, CustomExchange],
    pairs: set[tuple[str, str]],
    pg_config: dict[str, str],
    queries: QueryLoader,
) -> Snapshot:
    """
    Take one snapshot from the venues and the database.

    Parameters
    ----------
    clients : dict[str, CustomExchange]
        Authenticated clients by venue id.
    pairs : set[tuple[str, str]]
        Venue and symbol pairs to report on.
    pg_config : dict[str, str]
        PostgreSQL configuration.
    queries : QueryLoader
        Loaded queries.

    Returns
    -------
    Snapshot
        The snapshot.
    """
    snapshot = Snapshot(taken_at=datetime.now(UTC))
    per_venue: dict[str, list[str]] = {}
    for venue, symbol in sorted(pairs):
        per_venue.setdefault(venue, []).append(symbol)
    symbols = sorted({symbol for _, symbol in pairs})

    # The venues are asked while the database thread runs.
    venues = asyncio.gather(
        *(
            fetch_venue(venue, clients[venue], venue_symbols, snapshot)
            for venue, venue_symbols in per_venue.items()
        )
    )
    try:
        results = await asyncio.to_thread(query_database, pg_config, queries, symbols)
    except Exception as e:
        snapshot.database_ok = False
        snapshot.errors.append(f"database: {e}")
        results = {}
    await venues

    open_ids = {row["client order id"] for row in snapshot.open_orders}
    for imbalances, daily, monthly, last_price in results.values():
        rows, manual = imbalance_rows(
            imbalances, open_ids, snapshot.open_orders_complete
        )
        snapshot.imbalances.extend(rows)
        snapshot.manual_imbalances += manual
        snapshot.daily.extend(performance_rows(daily, "date", last_price))
        snapshot.monthly.extend(performance_rows(monthly, "month", last_price))
    return snapshot


def table(rows: list[dict[str, str]], empty: str) -> str:
    """
    Render rows as an HTML table.

    Parameters
    ----------
    rows : list[dict[str, str]]
        Rows sharing the keys of the first one.
    empty : str
        Text to show when there is no row.

    Returns
    -------
    str
        The table, or a paragraph with ``empty``.
    """
    if not rows:
        return f"<p class='empty'>{html.escape(empty)}</p>"
    head = "".join(f"<th>{html.escape(key)}</th>" for key in rows[0])
    body = "".join(
        "<tr>"
        + "".join(f"<td>{html.escape(row.get(key, ''))}</td>" for key in rows[0])
        + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render(snapshot: Snapshot) -> str:
    """
    Render a snapshot as a full HTML page.

    Parameters
    ----------
    snapshot : Snapshot
        The snapshot.

    Returns
    -------
    str
        The page.
    """
    taken = (
        snapshot.taken_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        if snapshot.taken_at
        else "not yet"
    )
    errors = "".join(f"<li>{html.escape(error)}</li>" for error in snapshot.errors)
    unavailable = "Database unavailable."
    no_imbalance = (
        "No imbalance on bot orders." if snapshot.database_ok else unavailable
    )
    no_trade = "No trade." if snapshot.database_ok else unavailable
    incomplete = (
        ""
        if snapshot.open_orders_complete
        else "<p class='errors'>Open orders are incomplete: an imbalance "
        "without a listed order may still be pending, so it shows unknown.</p>"
    )
    manual = (
        f"<p class='empty'>{snapshot.manual_imbalances} more on orders placed "
        "by hand, not listed.</p>"
        if snapshot.manual_imbalances
        else ""
    )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="{REFRESH_S}">
<title>Accountant</title>
<style>
:root {{ color-scheme: light dark; --line: #8884; --muted: #888; }}
body {{ font: 14px/1.4 system-ui, sans-serif; margin: 16px; }}
h1 {{ font-size: 20px; margin: 0 0 4px; }}
h2 {{ font-size: 16px; margin: 24px 0 8px; }}
.meta, .empty {{ color: var(--muted); }}
.errors {{ color: #c33; }}
.scroll {{ overflow-x: auto; }}
table {{ border-collapse: collapse; font-variant-numeric: tabular-nums; }}
th, td {{ padding: 4px 10px; border-bottom: 1px solid var(--line);
  text-align: right; white-space: nowrap; }}
th:first-child, td:first-child {{ text-align: left; }}
</style>
</head>
<body>
<h1>Accountant</h1>
<p class="meta">Snapshot {html.escape(taken)}, refreshed every {REFRESH_S} s.</p>
{f"<ul class='errors'>{errors}</ul>" if errors else ""}
<h2>Open orders</h2>
<div class="scroll">{table(snapshot.open_orders, "No open order.")}</div>
<h2>Balances</h2>
<div class="scroll">{table(snapshot.balances, "No balance.")}</div>
<h2>Imbalances</h2>
{incomplete}
<div class="scroll">{table(snapshot.imbalances, no_imbalance)}</div>
{manual}
<h2>Daily performance</h2>
<div class="scroll">{table(snapshot.daily, no_trade)}</div>
<h2>Monthly performance</h2>
<div class="scroll">{table(snapshot.monthly, no_trade)}</div>
</body>
</html>
"""


async def refresh(
    app: web.Application, pg_config: dict[str, str], queries: QueryLoader
) -> None:
    """
    Replace the app's snapshot every ``REFRESH_S`` seconds.

    A failed refresh keeps the last snapshot and replaces its errors with
    one that says the page is stale, so the errors never pile up.

    Parameters
    ----------
    app : web.Application
        The application holding the snapshot.
    pg_config : dict[str, str]
        PostgreSQL configuration.
    queries : QueryLoader
        Loaded queries.
    """
    while True:
        try:
            app[LATEST].snapshot = await take_snapshot(
                authenticated_clients, exchange_and_pair, pg_config, queries
            )
        except Exception as e:
            logger.exception("Snapshot failed")
            failed_at = datetime.now(UTC).strftime("%H:%M:%S UTC")
            app[LATEST].snapshot = dataclasses.replace(
                app[LATEST].snapshot,
                errors=[f"refresh failed at {failed_at}, page is stale: {e}"],
            )
        await asyncio.sleep(REFRESH_S)


async def page(request: web.Request) -> web.Response:
    """
    Serve the latest snapshot.

    Parameters
    ----------
    request : web.Request
        The request.

    Returns
    -------
    web.Response
        The page.
    """
    return web.Response(
        text=render(request.app[LATEST].snapshot), content_type="text/html"
    )


async def background(app: web.Application):
    """
    Run the refresh task for the lifetime of the app.

    The configuration and the venues' markets load before the app serves:
    if they fail, the process exits and cron starts it again, rather than
    serving an empty page from a dead task.

    Parameters
    ----------
    app : web.Application
        The application.

    Yields
    ------
    None
        Control while the app serves.
    """
    pg_config = load_pg_config()
    queries = QueryLoader()
    queries.load_queries()
    await load_clients()
    task = asyncio.create_task(refresh(app, pg_config, queries))
    yield
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    for client in authenticated_clients.values():
        await client.close()


def make_app() -> web.Application:
    """
    Build the web application.

    Returns
    -------
    web.Application
        The app, with an empty snapshot until the first refresh.
    """
    app = web.Application()
    app[LATEST] = Latest()
    app.router.add_get("/", page)
    app.cleanup_ctx.append(background)
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Address to listen on; the page has no login.",
    )
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on.")
    args = parser.parse_args()
    web.run_app(make_app(), host=args.host, port=args.port, print=None)
