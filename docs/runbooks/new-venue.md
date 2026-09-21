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

It runs four checks concurrently without keys, and two more with them, and
prints a PASS/FAIL/INFO table:

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
| fees (auth) | The venue's fee rates, whether the account's tier can be fetched, which currencies fills were actually charged in, and whether that matches `fee_currency` in config | See section 4. |

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
# fee_currency = "quote"   # see section 4; default is "quote"
# maker_fee = 0.0          # only when the fees check says the API hides the schedule
# taker_fee = 0.002
```

Then reference it from a subscription. The watchers, the fee watcher and
the recorder pick up new streams on restart; nothing else needs to change.

## 4. Fees

Which currency a venue charges fees in decides how the system hedges: a fee
in the quote asset comes out of the USDT leg and is a cost of doing
business, a fee in the base asset erodes the asset whose balance is kept
stable and must be compensated in every hedge. Rates move with the
account's rolling volume and balance, and the charged currency can move
with the account's fee mode, so neither is fixed in code:

- The fee watcher (`apps/maker/src/fees.py`) fetches each venue's schedule
  at startup and every `fees.refresh_s` seconds (default hourly),
  publishing a `FeeScheduleEvent` on `acct:fees:<venue>` and the snapshot
  key `fees-<venue>`. Rates resolve from `fetch_trading_fees` (the
  account's tier) where the venue supports it, from the loaded market's
  defaults otherwise, and from static `maker_fee`/`taker_fee` overrides in
  config where the API hides the schedule.
- The matcher waits for every venue's schedule at startup (bounded), sizes
  hedges from it, and logs a warning whenever a fill's actually reported
  fee contradicts the schedule — treat that warning as the schedule or the
  account having moved, not as noise.
- Strategies read the same schedule through `runtime.fees(venue, symbol)`
  and `runtime.fee_schedule(venue)`.

The conformance tool's `fees` check reads the last 20 trades on the symbol
and compares the fee currencies the venue actually charged against the
configured `fee_currency` policy. A FAIL there means the config or the
account's fee mode moved: fix `fee_currency` in `config.toml` and restart
the fee watcher and the matcher. A venue that charges in its own token is
declared with that asset code (`fee_currency = "GT"`); the accountant then
reports its fee columns untouched for that venue, which is correct — but
the token has to be bought back like any other balance, so prefer the
default modes when the venue offers a choice.

## 5. Sub-account and keys

- One sub-account per venue holding only what the test strategy needs
  (`max_size_usdt` plus fees, in both assets if it quotes both sides).
- Trade permission only, no withdrawal, IP whitelisted if supported.
- Keys go in `.env` as `{VENUE}_KEY`, `{VENUE}_SECRET` and, where the
  venue needs a passphrase, `{VENUE}_PASSWORD`. Never paste them into a
  chat or commit them.

## 6. Live run

With Redis up (`docker compose -f docker/redis/docker-compose.yml up -d`)
and `TESTING=True` in `.env`:

1. Start the four feed handlers alone first and watch for two minutes:
   ```bash
   uv run -m apps.maker.src.watcher & uv run -m apps.maker.src.recorder & uv run -m apps.maker.src.balance & uv run -m apps.maker.src.fees &
   ```
   Check `XLEN` grows on `md:book:<venue>:<symbol>`, that the log has no
   repeating warnings, that `seq` has no gaps and trade ids no duplicates,
   and that `data/md/...` line counts match the streams.
2. Kill and restart the recorder; it must resume from the last recorded id.
3. Only then run the orchestrator with strategies, for a bounded time
   (`timeout -s TERM 480 ...`), and afterwards confirm on the venue that no
   order is left resting and reconcile fills against balances.

## 7. Futures accounts

A linear perpetual on a venue is a second `[[venues]]` entry when the
exchange keeps spot and futures in separate wallets, and the same entry
with `account = "unified"` when it offers one margin pool. Either way, run
the tool against the contract symbol first:

```bash
uv run -m apps.maker.src.tools.venue_conformance <venue> <BASE/QUOTE:QUOTE> \
    --market-type swap --place-orders
```

`--market-type swap` points every client at the futures endpoints and adds
these checks:

| Check | What it verifies | If it fails |
|---|---|---|
| swap market | The symbol is a linear contract; reports contract size, minimum amount and precision | Use the CCXT contract symbol. Inverse contracts are out of scope. |
| swap has map | `fetchPositions`, `fetchFundingRate` and the account setters exist in CCXT | Without positions and funding the hedge cannot be reconciled on this venue. |
| swap fetch_positions | The account may read its futures positions at all | Usually the futures API is closed to the account or the key lacks futures permission. Nothing else works until this passes. |
| swap set_leverage | The leverage declared for the venue in config can be set | The broker fails the venue on this at start. Lower the leverage or set it by hand and remove it from config. |
| swap place order / client order id | A post-only limit far below the bid is accepted under our `t-<stamp>_<strategy>_<slot>` id, read back with that id, and cancelled | If the venue alters the id the order watcher cannot attribute fills. If the cancel fails, cancel by hand now. |
| swap reduce_only | A reduce-only order on a flat account is refused | An acceptance means the venue ignores the flag; an unwinding hedge cannot rely on it there. |

Without `--place-orders` the tool reads and never writes. Then declare the
venue:

```toml
[[venues]]
id = "venueaperp"          # keys every stream and order
ccxt_id = "venuea"         # the CCXT class, shared with the spot entry
name = "Venue A linear perpetuals"
market_type = "swap"
credentials = "venuea"     # only if it shares the spot entry's key
fee_currency = "quote"
margin_mode = "cross"
leverage = 2
```

Subscriptions on it must use contract symbols. At start the broker sets
the margin mode, leverage and one-way position mode on every contract
symbol the venue trades and exits if the venue refuses, which the
orchestrator turns into a full wind-down. A unified account is one entry
with `account = "unified"`, takes both symbol kinds, and needs no
`market_type`; put the venue's own account-type switch, if CCXT has one,
under `options`.

Keys for a futures account carry trade permission on the futures wallet
and nothing else: no withdrawal, and no transfer unless the design is
changed to want one (`docs/design/inventory-hedging.md`, section 4).

## Known venue quirks

- **Bitget**: full-depth book checksum failed on every ALPH/USDT update with
  ccxt 4.5.11, fixed by 4.5.77 (pinned in `pyproject.toml`). Replays its
  50-trade cache on every resubscription. Book rows have three columns.
- **MEXC**: trades carry no id. The first book message can take about 18
  seconds. Pushes balance updates on every order placement.
- Both: about 120 ms median exchange-to-receive lag for books.
