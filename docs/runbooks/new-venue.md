# Runbook: adding or re-validating a venue

The feed handlers depend on behaviours of each venue's websocket
implementation in CCXT that unit tests cannot see. Everything below was
learned the hard way on 2026-09-06 with MEXC and Bitget. Run it before a new
venue goes into `config.toml`, after every CCXT upgrade, and whenever a feed
misbehaves in production.

## 1. Run the conformance tool

Public checks need no keys. Pick the symbol you intend to trade; add
`--no-auth` if the sub-account does not exist yet.

```bash
uv run -m apps.maker.src.tools.venue_conformance <venue> <SYMBOL> --seconds 90
```

It runs five checks concurrently and prints a PASS/FAIL/INFO table:

| Check | What it verifies | If it fails |
|---|---|---|
| control feed | BTC/USDT book updates arrive, so the transport is alive | Fix network or venue status first. A quiet result on your symbol means nothing while this fails. |
| book first message | The symbol exists and pushes at all | Check the CCXT symbol spelling. Illiquid pairs can be silent for a minute. |
| book checksum | CCXT's delta checksum holds with verification on | Upgrade CCXT first (Bitget was fixed in 4.5.77). Otherwise set `options = { watchOrderBook = { checksum = false } }` on the venue in config. |
| book other errors | No error types outside the watcher's known lists | Add the type to `RESUBSCRIBE_ERRORS` or `RECONNECT_ERRORS` in `apps/maker/src/watcher.py`, else it crashes the watcher. |
| book depth / ts_exch / lag | At least `book_depth` levels, exchange timestamps present, exchange-to-receive lag | Informational. A venue without timestamps can only be researched on `ts_recv`. |
| trade ids / newUpdates / cache replay | Trades carry ids, consecutive calls return only new trades, resubscribe replays the cache | Missing ids and replay are handled by the watcher's dedupe. A newUpdates failure needs a look at `ccxt.pro.<venue>.watch_trades`. |
| shared client | Book and trade loops on one client without `close()`, as the watcher runs them | Same as "book other errors". |
| auth | `fetch_balance` with the sub-account keys and a `watch_balance` push | Check key, secret, passphrase and IP whitelist. |

Run it twice if the market is thin: a `quiet market` INFO on trades is not
a failure, but the checksum and shared-client checks need real updates to
say anything.

## 2. Interpret quiet markets correctly

ALPH/USDT goes silent for 30 to 60 seconds regularly on both venues. Three
probes in a row with 40-second timeouts once looked like a broken transport
after a dependency upgrade; a raw socket on BTC/USDT showed 250 updates in
25 seconds. Always compare against a liquid pair before blaming code.

## 3. Add the venue to config

In the strategies repo `config/config.toml`:

```toml
[[venues]]
id = "gate"          # CCXT short id, also the prefix of GATE_KEY etc. in .env
name = "Gate.io"
# options = { watchOrderBook = { checksum = false } }   # only if check 1 says so
```

Then reference it from a subscription. The watchers and the recorder pick
up new streams on restart; nothing else needs to change.

## 4. Sub-account and keys

- One sub-account per venue holding only what the test strategy needs
  (`max_size_usdt` plus fees, in both assets if it quotes both sides).
- Trade permission only, no withdrawal, IP whitelisted if supported.
- Keys go in `.env` as `{VENUE}_KEY`, `{VENUE}_SECRET` and, where the
  venue needs a passphrase, `{VENUE}_PASSWORD`. Never paste them into a
  chat or commit them.

## 5. Live run

With Redis up (`docker compose -f docker/redis/docker-compose.yml up -d`)
and `TESTING=True` in `.env`:

1. Start the three feed handlers alone first and watch for two minutes:
   ```bash
   uv run -m apps.maker.src.watcher & uv run -m apps.maker.src.recorder & uv run -m apps.maker.src.balance &
   ```
   Check `XLEN` grows on `md:book:<venue>:<symbol>`, that the log has no
   repeating warnings, that `seq` has no gaps and trade ids no duplicates,
   and that `data/md/...` line counts match the streams.
2. Kill and restart the recorder; it must resume from the last recorded id.
3. Only then run the orchestrator with strategies, for a bounded time
   (`timeout -s TERM 480 ...`), and afterwards confirm on the venue that no
   order is left resting and reconcile fills against balances.

## Known venue quirks

- **Bitget**: full-depth book checksum failed on every ALPH/USDT update with
  ccxt 4.5.11, fixed by 4.5.77 (pinned in `pyproject.toml`). Replays its
  50-trade cache on every resubscription. Book rows have three columns.
- **MEXC**: trades carry no id. The first book message can take about 18
  seconds. Pushes balance updates on every order placement.
- Both: about 120 ms median exchange-to-receive lag for books.
