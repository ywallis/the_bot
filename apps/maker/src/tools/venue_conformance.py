"""Live conformance check for a venue's websocket feeds through CCXT.

Run this before adding a venue to config, after upgrading CCXT, or whenever a
feed misbehaves. It exercises exactly the behaviours the feed handlers depend
on and that unit tests cannot see, and prints a verdict per check with the
config or code change to make when a check fails.

Usage
-----
    uv run -m apps.maker.src.tools.venue_conformance venue_a BASE/QUOTE
    uv run -m apps.maker.src.tools.venue_conformance venue_a BASE/QUOTE --seconds 120
    uv run -m apps.maker.src.tools.venue_conformance venue_a BASE/QUOTE --no-auth
    uv run -m apps.maker.src.tools.venue_conformance venue_a BASE/QUOTE:QUOTE \
        --market-type swap --place-orders

Public checks need no keys. The auth check reads ``{VENUE}_KEY``,
``{VENUE}_SECRET`` and ``{VENUE}_PASSWORD`` from ``.env`` like the real
clients do and is skipped when the key is missing.

Clients are built with the ``options`` of the venue as declared in config,
when it is declared, so the tool tests the client the system will run;
``--option key=value`` adds or overrides one. A unified account needs the
venue's own switch for it, for example ``--option uta=true`` on a venue
whose CCXT class calls it that, or CCXT keeps calling the classic
endpoints, which such an account refuses.

``--market-type swap`` points every client at the venue's futures account
and adds the derivatives checks: the position and funding endpoints, the
contract size, and, with ``--place-orders``, that the account may set its
leverage and place, read back and cancel a far-from-touch limit order under
our client order id and a reduce-only order. Nothing is placed without that
flag, and what is placed cannot fill.

See ``docs/runbooks/new-venue.md`` for how to act on the output.
"""

import argparse
import asyncio
import collections
import os
import statistics
import sys
import time
from typing import Any

import ccxt.pro as ccxt  # pyright: ignore[reportMissingTypeStubs]
from dotenv import load_dotenv

from apps.maker.src.watcher import RECONNECT_ERRORS, RESUBSCRIBE_ERRORS, trade_key
from apps.shared.src.config import (
    SPOT_MARKET,
    SWAP_MARKET,
    is_contract,
    load_app_config,
)
from apps.shared.src.events import Side
from apps.shared.src.fees import base_quote, fee_currency_for
from apps.shared.src.runtime import time_stamp

load_dotenv()

CONTROL_SYMBOLS = {SPOT_MARKET: "BTC/USDT", SWAP_MARKET: "BTC/USDT:USDT"}
CONTROL_SECONDS = 20
FIRST_MESSAGE_TIMEOUT = 45

# How far from the touch the conformance orders are priced, so they rest
# without any chance of filling while the tool reads them back.
SAFE_DISTANCE = 0.25

# CCXT ``options`` every client of this run is built with; ``--market-type
# swap`` sets ``defaultType`` here so the venue's futures endpoints answer.
_client_options: dict[str, Any] = {}


class Report:
    """
    Collects check results and prints them as a table with a verdict.

    Attributes
    ----------
    rows : list[tuple[str, str, str, str]]
        Check name, status, measurement, action.
    """

    def __init__(self) -> None:
        """Start with no rows."""
        self.rows: list[tuple[str, str, str, str]] = []

    def add(self, check: str, ok: bool | None, measure: str, action: str = "") -> None:
        """
        Record one check.

        Parameters
        ----------
        check : str
            Short check name.
        ok : bool | None
            True for pass, False for fail, None for informational.
        measure : str
            What was measured.
        action : str
            What to do if it failed, empty if nothing.
        """
        status = "PASS" if ok else "FAIL" if ok is False else "INFO"
        self.rows.append((check, status, measure, action))

    def print(self, venue: str, symbol: str) -> bool:
        """
        Print the table.

        Parameters
        ----------
        venue : str
            Venue id.
        symbol : str
            Symbol checked.

        Returns
        -------
        bool
            True if no check failed.
        """
        print(f"\n=== {venue} {symbol}  ccxt {ccxt.__version__}")
        width = max(len(r[0]) for r in self.rows)
        for check, status, measure, action in self.rows:
            print(f"  {status:4s} {check:{width}s}  {measure}")
            if action and status == "FAIL":
                print(f"       {'':{width}s}  -> {action}")
        failed = sum(1 for r in self.rows if r[1] == "FAIL")
        print(f"  {'OK' if not failed else f'{failed} FAILED'}")
        return failed == 0


# Markets loaded once per run and shared by every client, so the tool makes a
# single REST call instead of one per client and does not trip rate limits.
_markets: dict[str, Any] | None = None
_currencies: dict[str, Any] | None = None


async def preload_markets(venue: str) -> None:
    """
    Load markets once for the venue so later clients skip the REST call.

    Parameters
    ----------
    venue : str
        CCXT short id.
    """
    global _markets, _currencies
    client = getattr(ccxt, venue)()
    try:
        _markets = await client.load_markets()
        _currencies = client.currencies
    finally:
        await client.close()


def make_client(venue: str, auth: bool, options: dict[str, Any] | None = None) -> Any:
    """
    Build a CCXT pro client, optionally with credentials from the environment.

    Parameters
    ----------
    venue : str
        CCXT short id.
    auth : bool
        Whether to attach API credentials.
    options : dict[str, Any] | None
        CCXT ``options`` to pass.

    Returns
    -------
    Any
        The exchange instance, with markets preset if ``preload_markets`` ran.
    """
    params: dict[str, Any] = {}
    if auth:
        params["apiKey"] = os.getenv(f"{venue.upper()}_KEY")
        params["secret"] = os.getenv(f"{venue.upper()}_SECRET")
        password = os.getenv(f"{venue.upper()}_PASSWORD")
        if password:
            params["password"] = password
    merged = {**_client_options, **(options or {})}
    if merged:
        params["options"] = merged
    client = getattr(ccxt, venue)(params)
    if _markets is not None:
        client.set_markets(_markets, _currencies)
    return client


async def check_control(venue: str, market_type: str, report: Report) -> None:
    """
    Confirm the transport works by watching a liquid pair briefly.

    Parameters
    ----------
    venue : str
        CCXT short id.
    market_type : str
        ``spot`` or ``swap``, which picks the liquid control symbol.
    report : Report
        Where to record the result.
    """
    control_symbol = CONTROL_SYMBOLS[market_type]
    client = make_client(venue, auth=False)
    count = 0
    rate_limited = 0
    end = time.time() + CONTROL_SECONDS
    try:
        while time.time() < end:
            try:
                await asyncio.wait_for(
                    client.watch_order_book(control_symbol), end - time.time() + 0.1
                )
                count += 1
            except asyncio.TimeoutError:
                break
            except ccxt.DDoSProtection:
                # Venues rate-limit the subscribe handshake when many clients
                # connect at once; back off and try again within the window.
                rate_limited += 1
                await asyncio.sleep(5)
            except Exception as e:  # noqa: BLE001
                report.add(
                    "control feed",
                    False,
                    f"{type(e).__name__}: {str(e)[:80]}",
                    "transport or venue problem, fix before reading other checks",
                )
                return
    finally:
        await client.close()
    if rate_limited:
        report.add(
            "control rate limit",
            None,
            f"{rate_limited} rate-limit responses while subscribing; space out runs against this venue",
        )
    report.add(
        "control feed",
        count >= 10,
        f"{count} {control_symbol} book updates in {CONTROL_SECONDS}s",
        "fewer than 10 updates on a liquid pair means the transport or venue is down; do not trust a quiet result below",
    )


async def check_book(venue: str, symbol: str, seconds: int, report: Report) -> None:
    """
    Watch the book with checksum verification on and classify what happens.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        Symbol.
    seconds : int
        Observation window.
    report : Report
        Where to record results.
    """
    client = make_client(venue, auth=False)
    errors: collections.Counter[str] = collections.Counter()
    updates = 0
    depth = 0
    row_len = 0
    ts_null = 0
    lags: list[float] = []
    first_at: float | None = None
    start = time.time()
    end = start + seconds
    try:
        while time.time() < end:
            try:
                book = await asyncio.wait_for(
                    client.watch_order_book(symbol), max(1.0, end - time.time())
                )
            except asyncio.TimeoutError:
                break
            except Exception as e:  # noqa: BLE001
                errors[type(e).__name__] += 1
                await asyncio.sleep(0.2)
                continue
            recv_ms = time.time() * 1000
            if first_at is None:
                first_at = time.time() - start
            updates += 1
            depth = max(depth, len(book["bids"]))
            if book["bids"]:
                row_len = len(book["bids"][0])
            if book.get("timestamp") is None:
                ts_null += 1
            else:
                lags.append(recv_ms - book["timestamp"])
    finally:
        await client.close()

    checksum_errors = errors.get("ChecksumError", 0) + errors.get("UnsubscribeError", 0)
    other = {
        k: v
        for k, v in errors.items()
        if k not in ("ChecksumError", "UnsubscribeError")
    }
    report.add(
        "book first message",
        first_at is not None,
        f"{first_at:.1f}s" if first_at is not None else f"none within {seconds}s",
        "if the control feed passed, the symbol may not exist on this venue or is extremely quiet; check the CCXT symbol",
    )
    report.add(
        "book checksum",
        checksum_errors == 0,
        f"{updates} updates, {checksum_errors} checksum/unsubscribe errors",
        "upgrade ccxt first (Bitget was fixed in 4.5.77); otherwise set options = { watchOrderBook = { checksum = false } } on the venue in config",
    )
    report.add(
        "book other errors",
        not other,
        f"{dict(other) if other else 'none'}",
        "add the error type to RESUBSCRIBE_ERRORS or RECONNECT_ERRORS in watcher.py, or it will crash the watcher",
    )
    report.add(
        "book depth",
        depth >= 20,
        f"{depth} levels, rows of {row_len}",
        "fewer than book_depth levels; pass a limit or lower market_data.book_depth",
    )
    report.add(
        "book ts_exch",
        ts_null == 0,
        f"{ts_null}/{updates} updates without exchange timestamp",
        "lead-lag research on this venue will have to use ts_recv only",
    )
    if lags:
        report.add(
            "book lag",
            None,
            f"p50 {statistics.median(lags):.0f} ms, max {max(lags):.0f} ms (exchange to receive)",
        )


async def check_trades(venue: str, symbol: str, seconds: int, report: Report) -> None:
    """
    Watch trades, then resubscribe and measure cache replay and id presence.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        Symbol.
    seconds : int
        Maximum time to wait for the first batch on each subscription.
    report : Report
        Where to record results.
    """
    client = make_client(venue, auth=False)
    first: list[dict[str, Any]] = []
    second: list[dict[str, Any]] = []
    consecutive_overlap: int | None = None
    try:
        try:
            first = await asyncio.wait_for(
                client.watch_trades(symbol), min(seconds, FIRST_MESSAGE_TIMEOUT)
            )
            try:
                nxt = await asyncio.wait_for(client.watch_trades(symbol), 30)
                consecutive_overlap = len(
                    {trade_key(t) for t in first} & {trade_key(t) for t in nxt}
                )
            except asyncio.TimeoutError:
                pass
        except asyncio.TimeoutError:
            report.add(
                "trades first batch",
                None,
                f"no trades within {min(seconds, FIRST_MESSAGE_TIMEOUT)}s, quiet market; rerun on a busier pair to test trades",
            )
            return
        await client.close()
        client = make_client(venue, auth=False)
        try:
            second = await asyncio.wait_for(client.watch_trades(symbol), 30)
        except asyncio.TimeoutError:
            pass
    finally:
        await client.close()

    ids_present = sum(1 for t in first if t.get("id") is not None)
    replay = len({trade_key(t) for t in first} & {trade_key(t) for t in second})
    report.add("trades first batch", None, f"{len(first)} trades on subscribe")
    report.add(
        "trade ids",
        None,
        f"{ids_present}/{len(first)} carry an id"
        + (
            ""
            if ids_present == len(first)
            else "; dedupe falls back to timestamp/price/amount"
        ),
    )
    report.add(
        "trades newUpdates",
        consecutive_overlap in (None, 0),
        "consecutive calls return only new trades"
        if consecutive_overlap in (None, 0)
        else f"{consecutive_overlap} trades repeated between consecutive calls",
        "the watcher relies on CCXT newUpdates; investigate this venue's watch_trades in ccxt.pro",
    )
    report.add(
        "trades cache replay",
        None,
        f"{replay} trades replayed after resubscribe"
        + (" (handled by watcher dedupe)" if replay else ""),
    )


async def check_shared_client(
    venue: str, symbol: str, seconds: int, report: Report
) -> None:
    """
    Run book and trade loops on one client without closing it, as the watcher does.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        Symbol.
    seconds : int
        Observation window.
    report : Report
        Where to record results.
    """
    client = make_client(venue, auth=False)
    errors: collections.Counter[str] = collections.Counter()
    counts: collections.Counter[str] = collections.Counter()
    end = time.time() + seconds

    async def loop(name: str, factory: Any) -> None:
        while time.time() < end:
            try:
                await asyncio.wait_for(factory(), max(1.0, end - time.time()))
                counts[name] += 1
            except asyncio.TimeoutError:
                return
            except Exception as e:  # noqa: BLE001
                errors[f"{name}:{type(e).__name__}"] += 1
                await asyncio.sleep(0.2)

    try:
        await asyncio.gather(
            loop("book", lambda: client.watch_order_book(symbol)),
            loop("trades", lambda: client.watch_trades(symbol)),
        )
    finally:
        await client.close()

    known = tuple(e.__name__ for e in RESUBSCRIBE_ERRORS + RECONNECT_ERRORS)
    unknown = {k: v for k, v in errors.items() if k.split(":")[1] not in known}
    report.add(
        "shared client",
        not unknown,
        f"book {counts['book']}, trades {counts['trades']}, errors {dict(errors) if errors else 'none'}",
        "unknown error types would crash the watcher; classify them in watcher.py",
    )


async def check_auth(venue: str, report: Report) -> None:
    """
    Verify credentials and the balance feed.

    Parameters
    ----------
    venue : str
        CCXT short id.
    report : Report
        Where to record results.
    """
    if not os.getenv(f"{venue.upper()}_KEY"):
        report.add("auth", None, f"skipped, {venue.upper()}_KEY not set")
        return
    client = make_client(venue, auth=True)
    try:
        balance = await asyncio.wait_for(client.fetch_balance(), 30)
        assets = {
            k: v.get("total")
            for k, v in balance.items()
            if isinstance(v, dict) and v.get("total")
        }
        report.add("auth fetch_balance", True, f"ok, non-zero: {assets or 'nothing'}")
        try:
            await asyncio.wait_for(client.watch_balance(), 20)
            report.add("auth watch_balance", True, "first push within 20s")
        except asyncio.TimeoutError:
            report.add(
                "auth watch_balance",
                None,
                "no push within 20s (normal: venues push on change only, the handler seeds with fetch_balance)",
            )
    except Exception as e:  # noqa: BLE001
        report.add(
            "auth fetch_balance",
            False,
            f"{type(e).__name__}: {str(e)[:100]}",
            "check key, secret, passphrase and IP whitelist for the sub-account",
        )
    finally:
        await client.close()


async def check_fees(venue: str, symbol: str, report: Report) -> None:
    """
    Report the venue's fee schedule and check what fills were charged.

    The rates decide how hedges are sized and quotes are priced, and the
    currency a fee is charged in decides whether the base balance erodes
    with every fill or the USDT leg pays. Both are venue behaviour unit
    tests cannot see, and both move: rates with the account's volume tier,
    currency with the account's fee mode.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        Symbol checked.
    report : Report
        Where to record results.
    """
    market = (_markets or {}).get(symbol)
    if market is None:
        report.add("fees market defaults", False, f"no market {symbol}")
        return
    report.add(
        "fees market defaults",
        None,
        f"maker {market.get('maker')}, taker {market.get('taker')}",
    )

    client = make_client(venue, auth=True)
    try:
        has = getattr(client, "has", {}) or {}
        if not has.get("fetchTradingFees"):
            report.add(
                "fees endpoint",
                None,
                "fetchTradingFees not supported",
                "declare static maker_fee/taker_fee overrides on the venue in config",
            )
        else:
            try:
                fees = await asyncio.wait_for(client.fetch_trading_fees(), 30)
                if isinstance(fees.get("trading"), dict):
                    fees = fees["trading"]
                account = fees.get(symbol, {})
                tiered = account.get("maker") != market.get("maker")
                report.add(
                    "fees account rates",
                    None,
                    f"maker {account.get('maker')}, taker {account.get('taker')}"
                    + (
                        " (account tier differs from the market default)"
                        if tiered
                        else ""
                    ),
                )
            except Exception as e:  # noqa: BLE001
                report.add(
                    "fees endpoint",
                    False,
                    f"{type(e).__name__}: {str(e)[:80]}",
                    "declare static maker_fee/taker_fee overrides on the venue in config",
                )

        try:
            trades = await asyncio.wait_for(
                client.fetch_my_trades(symbol, limit=20), 30
            )
        except Exception as e:  # noqa: BLE001
            report.add(
                "fees observed", None, f"no recent trades to read: {type(e).__name__}"
            )
            return
        by_side: dict[str, set[str]] = {"buy": set(), "sell": set()}
        for trade in trades:
            fee = trade.get("fee") or {}
            side = trade.get("side")
            if fee.get("currency") and side in by_side:
                by_side[side].add(str(fee["currency"]))
        report.add(
            "fees observed",
            None,
            f"buy charged {sorted(by_side['buy']) or 'nothing'}, "
            f"sell charged {sorted(by_side['sell']) or 'nothing'} "
            f"over the last {len(trades)} trades",
        )
    finally:
        await client.close()

    try:
        config = load_app_config()
    except Exception:  # noqa: BLE001
        return
    venue_config = next((v for v in config.venues if v.id == venue), None)
    if venue_config is None:
        report.add(
            "fees policy", None, "venue not declared in config, nothing to compare"
        )
        return
    base, quote = base_quote(symbol)
    mismatches = []
    for side_name in ("buy", "sell"):
        expected = fee_currency_for(
            venue_config.fee_currency, Side(side_name), base, quote
        )
        for seen in sorted(by_side[side_name]):
            if seen != expected:
                mismatches.append(f"{side_name} charged {seen}, config says {expected}")
    if mismatches:
        report.add(
            "fees policy",
            False,
            "; ".join(mismatches),
            "update fee_currency on the venue in config; the matcher sizes hedges with it",
        )
    else:
        report.add(
            "fees policy",
            True,
            f"configured {venue_config.fee_currency} matches what the venue charged",
        )


def configured_options(venue: str, market_type: str) -> dict[str, Any]:
    """
    Return the CCXT options of the declared venue this run stands for.

    Parameters
    ----------
    venue : str
        CCXT short id.
    market_type : str
        ``spot`` or ``swap``.

    Returns
    -------
    dict[str, Any]
        The ``options`` of the first configured venue built from this CCXT
        class whose market type matches, or that is a unified account and
        so serves both; empty when none is declared or config is missing.
    """
    try:
        config = load_app_config()
    except Exception:  # noqa: BLE001, no config means no declared options
        return {}
    for declared in config.venues:
        if declared.exchange != venue:
            continue
        if declared.market_type == market_type or declared.derivatives:
            return dict(declared.options)
    return {}


def parse_option(text: str) -> tuple[str, Any]:
    """
    Parse one ``--option key=value`` argument.

    Parameters
    ----------
    text : str
        ``key=value``; ``true``, ``false`` and numbers are converted.

    Returns
    -------
    tuple[str, Any]
        Key and typed value.

    Raises
    ------
    argparse.ArgumentTypeError
        If there is no ``=``.
    """
    key, sep, raw = text.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError(f"expected key=value, got {text!r}")
    lowered = raw.lower()
    value: Any
    if lowered in ("true", "false"):
        value = lowered == "true"
    else:
        try:
            value = int(raw)
        except ValueError:
            try:
                value = float(raw)
            except ValueError:
                value = raw
    return key, value


def conformance_order_id() -> str:
    """
    Return a client order id in the shape the order watcher parses.

    Returns
    -------
    str
        ``t-<stamp>_conf_x``: the real format with a strategy identifier
        no configured strategy uses, so a leftover is attributable and
        harmless.
    """
    return f"t-{int(time_stamp(time.time_ns())):018d}_conf_x"


async def _cancel_quietly(client: Any, order_id: str, symbol: str) -> str | None:
    """
    Cancel an order and return the error, if any, rather than raising.

    Parameters
    ----------
    client : Any
        The CCXT client.
    order_id : str
        Venue order id.
    symbol : str
        Symbol.

    Returns
    -------
    str | None
        The error description, None on success.
    """
    try:
        await asyncio.wait_for(client.cancel_order(order_id, symbol), 30)
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {str(e)[:80]}"
    return None


async def check_derivatives(
    venue: str, symbol: str, place_orders: bool, report: Report
) -> None:
    """
    Check what the derivatives account exposes and, opted in, that it trades.

    The read-only part reports the contract's terms, the position and
    funding endpoints and the account-level setters in the ``has`` map.
    With ``place_orders`` it also sets the leverage the config declares
    for the venue (or 1 if none), places a post-only limit buy far below
    the bid under our client order id, reads it back, cancels it, and then
    tries a reduce-only sell far above the ask. On a flat account the
    reduce-only order should be refused; an acceptance is reported so the
    operator knows the flag is not enforced the way the order manager
    assumes.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        Contract symbol.
    place_orders : bool
        Whether to place and cancel the test orders.
    report : Report
        Where to record results.
    """
    market = (_markets or {}).get(symbol)
    if market is None or not market.get("contract"):
        report.add(
            "swap market",
            False,
            f"{symbol} is not a contract market on {venue}",
            "use the CCXT contract symbol, BASE/QUOTE:SETTLE",
        )
        return
    limits = market.get("limits") or {}
    min_amount = ((limits.get("amount") or {}).get("min")) or 1
    report.add(
        "swap market",
        market.get("linear") is True,
        f"contractSize {market.get('contractSize')}, min amount {min_amount}, "
        f"amount precision {(market.get('precision') or {}).get('amount')}, "
        f"settle {market.get('settle')}, linear {market.get('linear')}",
        "only linear perpetuals hedge a spot holding one for one in base",
    )

    if not os.getenv(f"{venue.upper()}_KEY"):
        report.add("swap account", None, f"skipped, {venue.upper()}_KEY not set")
        return
    client = make_client(venue, auth=True)
    try:
        has = getattr(client, "has", {}) or {}
        setters = {
            k: bool(has.get(k))
            for k in (
                "fetchPositions",
                "watchPositions",
                "setLeverage",
                "setMarginMode",
                "setPositionMode",
                "fetchFundingRate",
                "fetchFundingHistory",
            )
        }
        report.add(
            "swap has map",
            setters["fetchPositions"] and setters["fetchFundingRate"],
            ", ".join(f"{k} {'yes' if v else 'no'}" for k, v in setters.items()),
            "the position and funding feeds poll these; a venue without them cannot host the hedge",
        )
        try:
            positions = await asyncio.wait_for(client.fetch_positions([symbol]), 30)
            open_positions = [
                p for p in positions if p.get("contracts") not in (None, 0, 0.0)
            ]
            report.add(
                "swap fetch_positions",
                True,
                f"{len(open_positions)} open on {symbol}"
                + (
                    "; "
                    + ", ".join(
                        f"{p.get('side')} {p.get('contracts')} @ {p.get('entryPrice')}"
                        for p in open_positions
                    )
                    if open_positions
                    else ""
                ),
            )
        except Exception as e:  # noqa: BLE001
            report.add(
                "swap fetch_positions",
                False,
                f"{type(e).__name__}: {str(e)[:100]}",
                "the futures API may be closed to this account; nothing below can work until it opens",
            )
            return
        try:
            funding = await asyncio.wait_for(client.fetch_funding_rate(symbol), 30)
            report.add(
                "swap funding",
                None,
                f"rate {funding.get('fundingRate')}, next {funding.get('fundingDatetime')}, "
                f"interval {funding.get('interval')}, mark {funding.get('markPrice')}",
            )
        except Exception as e:  # noqa: BLE001
            report.add("swap funding", False, f"{type(e).__name__}: {str(e)[:100]}")

        if not place_orders:
            report.add(
                "swap orders", None, "skipped; pass --place-orders to exercise them"
            )
            return

        leverage = 1
        try:
            config = load_app_config()
            declared = next((v for v in config.venues if v.exchange == venue), None)
            if declared is not None and declared.leverage is not None:
                leverage = declared.leverage
        except Exception:  # noqa: BLE001, a missing config means leverage 1
            pass
        if setters["setLeverage"]:
            try:
                await asyncio.wait_for(client.set_leverage(leverage, symbol), 30)
                report.add("swap set_leverage", True, f"{leverage}x on {symbol}")
            except Exception as e:  # noqa: BLE001
                report.add(
                    "swap set_leverage",
                    False,
                    f"{type(e).__name__}: {str(e)[:100]}",
                    "the broker fails the venue on this at start; set it by hand or fix the config",
                )

        book = await asyncio.wait_for(client.fetch_order_book(symbol, 5), 30)
        if not book["bids"] or not book["asks"]:
            report.add("swap orders", False, "empty book, cannot price a safe order")
            return
        bid, ask = book["bids"][0][0], book["asks"][0][0]
        buy_price = float(client.price_to_precision(symbol, bid * (1 - SAFE_DISTANCE)))
        sell_price = float(client.price_to_precision(symbol, ask * (1 + SAFE_DISTANCE)))
        # The smallest order the venue takes is bounded by amount and by
        # notional, and the notional is measured at our far-off price, so
        # size to clear both with a margin, as the matcher does for dust.
        min_cost = (limits.get("cost") or {}).get("min") or 0
        contract_size = market.get("contractSize") or 1
        by_cost = min_cost * 1.1 / (buy_price * contract_size)
        amount = float(client.amount_to_precision(symbol, max(min_amount, by_cost)))
        if amount < by_cost:
            step = float(client.amount_to_precision(symbol, min_amount)) or min_amount
            amount = float(client.amount_to_precision(symbol, amount + step))

        client_id = conformance_order_id()
        try:
            placed = await asyncio.wait_for(
                client.create_order(
                    symbol,
                    "limit",
                    "buy",
                    amount,
                    buy_price,
                    {"clientOrderId": client_id, "postOnly": True},
                ),
                30,
            )
        except Exception as e:  # noqa: BLE001
            report.add(
                "swap place order",
                False,
                f"{type(e).__name__}: {str(e)[:100]}",
                "the account cannot place futures orders through the API; check permissions and the venue's futures API status",
            )
            return
        venue_id = placed.get("id")
        try:
            fetched = await asyncio.wait_for(client.fetch_order(venue_id, symbol), 30)
            echoed = fetched.get("clientOrderId")
            report.add(
                "swap client order id",
                echoed == client_id,
                f"sent {client_id}, venue reports {echoed}",
                "the order watcher attributes fills by this id; the futures endpoint alters it",
            )
        except Exception as e:  # noqa: BLE001
            report.add(
                "swap client order id",
                False,
                f"fetch_order: {type(e).__name__}: {str(e)[:80]}",
            )
        error = await _cancel_quietly(client, venue_id, symbol)
        report.add(
            "swap place order",
            error is None,
            f"placed {amount} @ {buy_price} post-only, "
            + ("cancelled" if error is None else f"CANCEL FAILED: {error}"),
            "cancel the leftover order by hand now",
        )

        try:
            placed = await asyncio.wait_for(
                client.create_order(
                    symbol,
                    "limit",
                    "sell",
                    amount,
                    sell_price,
                    {"clientOrderId": conformance_order_id(), "reduceOnly": True},
                ),
                30,
            )
        except Exception as e:  # noqa: BLE001
            report.add(
                "swap reduce_only",
                True,
                f"refused on a flat account as it should be: {type(e).__name__}: {str(e)[:80]}",
            )
        else:
            error = await _cancel_quietly(client, placed.get("id"), symbol)
            report.add(
                "swap reduce_only",
                False,
                "accepted on a flat account"
                + ("" if error is None else f"; CANCEL FAILED: {error}"),
                "the venue does not enforce reduceOnly; the unwinding hedge cannot rely on it here",
            )
    finally:
        await client.close()


async def run(
    venue: str,
    symbol: str,
    seconds: int,
    auth: bool,
    market_type: str = SPOT_MARKET,
    place_orders: bool = False,
    options: dict[str, Any] | None = None,
) -> bool:
    """
    Run every check and print the report.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        Symbol to check.
    seconds : int
        Observation window for the book and shared-client checks.
    auth : bool
        Whether to run the credential check.
    market_type : str
        ``spot`` or ``swap``; the latter adds the derivatives checks.
    place_orders : bool
        Whether the derivatives check may place and cancel test orders.
    options : dict[str, Any] | None
        CCXT options added to, and overriding, the configured venue's.

    Returns
    -------
    bool
        True if no check failed.
    """
    if not hasattr(ccxt, venue):
        print(f"ccxt.pro has no exchange {venue!r}")
        return False
    if is_contract(symbol) != (market_type == SWAP_MARKET):
        print(
            f"{symbol!r} does not match --market-type {market_type}: a contract "
            "symbol carries a settle suffix (BASE/QUOTE:QUOTE) and a spot one does not"
        )
        return False
    if market_type == SWAP_MARKET:
        _client_options["defaultType"] = SWAP_MARKET
    _client_options.update(configured_options(venue, market_type))
    _client_options.update(options or {})
    if _client_options:
        print(f"client options: {_client_options}")
    report = Report()
    try:
        await preload_markets(venue)
    except Exception as e:  # noqa: BLE001
        print(f"load_markets failed: {type(e).__name__}: {str(e)[:120]}")
        return False
    if symbol not in (_markets or {}):
        # Every check would fail on this in its own way, the trade check by
        # raising out of the whole run; one line up front says it plainly.
        kind = "perpetual" if market_type == SWAP_MARKET else "spot market"
        base, _quote = base_quote(symbol)
        listed = sorted(
            s
            for s, m in (_markets or {}).items()
            if s.startswith(f"{base}/")
            and bool(m.get("contract")) == is_contract(symbol)
        )
        print(
            f"{venue} does not list {symbol}: no {kind} for {base} on this venue"
            + (f"; it does list {listed}" if listed else "")
        )
        return False
    tasks = [
        check_control(venue, market_type, report),
        check_book(venue, symbol, seconds, report),
        check_trades(venue, symbol, seconds, report),
        check_shared_client(venue, symbol, seconds, report),
    ]
    if auth:
        tasks.append(check_auth(venue, report))
        tasks.append(check_fees(venue, symbol, report))
    if market_type == SWAP_MARKET:
        tasks.append(check_derivatives(venue, symbol, place_orders and auth, report))
    await asyncio.gather(*tasks)
    return report.print(venue, symbol)


def main() -> None:
    """Parse arguments and run."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("venue", help="CCXT short id")
    parser.add_argument(
        "symbol", help="CCXT symbol, BASE/QUOTE or BASE/QUOTE:SETTLE for a swap"
    )
    parser.add_argument(
        "--seconds", type=int, default=90, help="observation window (default 90)"
    )
    parser.add_argument(
        "--no-auth", action="store_true", help="skip the credential check"
    )
    parser.add_argument(
        "--market-type",
        choices=sorted((SPOT_MARKET, SWAP_MARKET)),
        default=SPOT_MARKET,
        help="which account the clients address; swap adds the derivatives checks",
    )
    parser.add_argument(
        "--place-orders",
        action="store_true",
        help="let the swap check place and cancel far-from-touch test orders",
    )
    parser.add_argument(
        "--option",
        action="append",
        type=parse_option,
        default=[],
        metavar="KEY=VALUE",
        help="CCXT client option, repeatable; overrides the configured venue's",
    )
    args = parser.parse_args()
    ok = asyncio.run(
        run(
            args.venue,
            args.symbol,
            args.seconds,
            not args.no_auth,
            args.market_type,
            args.place_orders,
            dict(args.option),
        )
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
