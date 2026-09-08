"""Live conformance check for a venue's websocket feeds through CCXT.

Run this before adding a venue to config, after upgrading CCXT, or whenever a
feed misbehaves. It exercises exactly the behaviours the feed handlers depend
on and that unit tests cannot see, and prints a verdict per check with the
config or code change to make when a check fails.

Usage
-----
    uv run -m apps.maker.src.tools.venue_conformance bitget ALPH/USDT
    uv run -m apps.maker.src.tools.venue_conformance mexc ALPH/USDT --seconds 120
    uv run -m apps.maker.src.tools.venue_conformance gate BTC/USDT --no-auth

Public checks need no keys. The auth check reads ``{VENUE}_KEY``,
``{VENUE}_SECRET`` and ``{VENUE}_PASSWORD`` from ``.env`` like the real
clients do and is skipped when the key is missing.

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

load_dotenv()

CONTROL_SYMBOL = "BTC/USDT"
CONTROL_SECONDS = 20
FIRST_MESSAGE_TIMEOUT = 45


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
    if options:
        params["options"] = options
    client = getattr(ccxt, venue)(params)
    if _markets is not None:
        client.set_markets(_markets, _currencies)
    return client


async def check_control(venue: str, report: Report) -> None:
    """
    Confirm the transport works by watching a liquid pair briefly.

    Parameters
    ----------
    venue : str
        CCXT short id.
    report : Report
        Where to record the result.
    """
    client = make_client(venue, auth=False)
    count = 0
    rate_limited = 0
    end = time.time() + CONTROL_SECONDS
    try:
        while time.time() < end:
            try:
                await asyncio.wait_for(
                    client.watch_order_book(CONTROL_SYMBOL), end - time.time() + 0.1
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
        f"{count} {CONTROL_SYMBOL} book updates in {CONTROL_SECONDS}s",
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


async def run(venue: str, symbol: str, seconds: int, auth: bool) -> bool:
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

    Returns
    -------
    bool
        True if no check failed.
    """
    if not hasattr(ccxt, venue):
        print(f"ccxt.pro has no exchange {venue!r}")
        return False
    report = Report()
    try:
        await preload_markets(venue)
    except Exception as e:  # noqa: BLE001
        print(f"load_markets failed: {type(e).__name__}: {str(e)[:120]}")
        return False
    tasks = [
        check_control(venue, report),
        check_book(venue, symbol, seconds, report),
        check_trades(venue, symbol, seconds, report),
        check_shared_client(venue, symbol, seconds, report),
    ]
    if auth:
        tasks.append(check_auth(venue, report))
    await asyncio.gather(*tasks)
    return report.print(venue, symbol)


def main() -> None:
    """Parse arguments and run."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("venue", help="CCXT short id, e.g. bitget")
    parser.add_argument("symbol", help="CCXT symbol, e.g. ALPH/USDT")
    parser.add_argument(
        "--seconds", type=int, default=90, help="observation window (default 90)"
    )
    parser.add_argument(
        "--no-auth", action="store_true", help="skip the credential check"
    )
    args = parser.parse_args()
    ok = asyncio.run(run(args.venue, args.symbol, args.seconds, not args.no_auth))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
