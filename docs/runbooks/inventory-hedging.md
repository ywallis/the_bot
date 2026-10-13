# Runbook: inventory hedged on a perpetual

This runbook starts, checks and stops an inventory build or unwind. The
design is in `docs/design/inventory-hedging.md`. Venue and asset names are
in the private config only.

## 1. Before the first start

1. Run the futures conformance check on the perpetual venue:
   ```bash
   uv run -m apps.maker.src.tools.venue_conformance <venue> <BASE/QUOTE:QUOTE> \
       --market-type swap --place-orders
   ```
   Every `swap` line must be `PASS`. If a line fails, follow
   `docs/runbooks/new-venue.md` section 7.
2. Read a week of funding for the candidate in the screener output:
   ```bash
   uv run -m apps.maker.src.tools.market_screener --minutes 10 --perp unified
   ```
   If `f/wk` is negative, do not build.
3. If you want alerts on a phone, set `TELEGRAM_BOT_TOKEN` and
   `TELEGRAM_CHAT_ID` in `.env`. Without them, alerts go to the log only.

## 2. Configure

Use three entries in the private config:

- a `[[venues]]` entry for the perpetual, with `market_type = "swap"`,
  `credentials` and `leverage`. On a unified account, add CCXT's account
  switch to `options` and do not set `margin_mode`.
- a `delta_reconciler` strategy with `perp`, `spot_venues`,
  `deadband_base` and `max_correction_base`. Subscribe the perpetual with
  the `position` feed and every spot venue that holds the asset.
- an `inventory` strategy with `spot`, `perp`, `should_match = true`, a
  `hedge` table that names the perpetual, `direction`, `target_quote` and
  `max_unhedged_base`. Subscribe the spot symbol with the `delta` feed.

Set `deadband_base` above one minimum perpetual order. A deadband under the
minimum makes the reconciler find a delta it cannot correct.

## 3. Start

1. Start with `act = false` on the reconciler and `mode = "observe"` on the
   inventory strategy.
2. Start the system. Wait one reconciler interval.
3. Make sure the log shows a healthy delta and `would quote` lines.
4. Stop the system. Set `act = true` and `mode = "live"`. Start again.

## 4. Check while it runs

- Read the delta:
  ```bash
  uv run python -c "import redis; from apps.shared.src.events import from_stream_fields as f; r=redis.Redis(); print(f(r.xrevrange('acct:delta:<BASE>', count=1)[0][1]))"
  ```
  `healthy` must be `True`. `delta` must stay inside `deadband`.
- After a spot fill, the log shows `Contract hedge sent` from the matcher,
  then the position feed shows the short.
- An alert with the key `hedge:` or `leg:` means a fill is not hedged. If
  the reconciler does not correct it within two intervals, close the
  difference on the perpetual by hand.

## 5. Stop

Stop the system with SIGTERM to the orchestrator. Do not use `pkill -f` or
`pgrep -f` with a pattern that the shell command itself contains: it kills
the shell.

The short stays open on purpose. It hedges the inventory. To leave the
position, run the inventory strategy with `direction = "unwind"`, and with
`fast = true` if the basis does not matter.

## 6. After a session

Run the PnL report. The carry section shows the funding the short received
or paid.

```bash
uv run -m apps.maker.src.tools.pnl --start <ISO time of the start>
```
