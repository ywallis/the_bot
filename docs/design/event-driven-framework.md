# Event-driven framework: design

Status: draft, 2026-09-06
Scope: the `maker` engine, the `shared` layer and the strategy contract. The
accountant is out of scope until a later phase.

## 1. Motivation

The framework was built for two-venue liquidity arbitrage. That shows up in
three places:

- Config loading assumes every strategy has `exchange_1`, `exchange_2` and one
  `symbol`.
- The message processor tracks one open order per strategy key and only knows
  the order types `replace`, `unique` and `market`.
- The matcher is a hardcoded taker hedge for maker fills and is the only
  consumer of fill data.

The next strategy family is cross-pair, cross-venue latency arbitrage: a move
on a fast, liquid source (for example BTC on Binance) predicts a move on a
slower pair and venue (for example SOL on MEXC) some hundreds of milliseconds
later. That family needs:

- Event delivery instead of polling. The edge is measured in the time between
  the leading event and the order reaching the lagging venue.
- Receive timestamps on every market data event, so lead-lag can be measured
  and monitored.
- Fills and positions delivered back to the strategy. The strategy takes a
  position and must exit it.
- Recording of all market data, so lead-lag relationships can be researched
  offline and strategies can be backtested.
- A measured model of our own order round trip per venue, because that is the
  binding constraint on whether an anticipated move can be captured.
- Language independence at the wire level, so compute-heavy strategies can be
  written outside Python.

## 2. Goals and non-goals

Goals

- Keep the existing five-process topology: feed handlers, bus, order
  management, execution gateway, ledger.
- Replace polling of snapshot keys with consumption of Redis Streams.
- Define typed, versioned event schemas as the cross-language contract.
- Make the config declare venues and subscriptions explicitly.
- Record all streams to disk for research and replay.
- Run the same strategy code live and in backtest.
- Keep existing strategies working at every step of the migration.

Non-goals

- Implementing any new strategy.
- Changing the accountant, other than keeping its imports working.
- Sub-millisecond performance. Python and CCXT remain the baseline; the target
  is niche venues and pairs where the lag is hundreds of milliseconds.

## 3. Target architecture

```
 exchanges ──ws──> watchers ──XADD──> md:book:*  md:trade:*  ──XREAD──> strategies
                   balance  ──XADD──> acct:balance:*                       │
                                                                           │ XADD
                   recorder <──XREAD── every stream                        ▼
                                                                    oms:intents (consumer group)
                                                                           │
 exchanges <──rest── broker <── oms (message processor) <──────────────────┘
      │                 │              │
      └──ws fills──> order watcher ──XADD──> oms:events ──XREAD──> strategies, matcher, ledger
```

Every arrow between processes is a Redis Stream carrying a typed event. Redis
is local to the trading host, so hop latency is around 100 microseconds and
not a concern.

### 3.1 Streams and naming

| Stream                       | Producer            | Consumers                       | Notes                                   |
|------------------------------|---------------------|---------------------------------|-----------------------------------------|
| `md:book:{venue}:{symbol}`   | watcher             | strategies, recorder            | top-N levels, every update              |
| `md:trade:{venue}:{symbol}`  | trade watcher       | strategies, recorder            | one event per trade or CCXT batch       |
| `acct:balance:{venue}`       | balance watcher     | strategies, recorder            | full balance snapshot                   |
| `oms:intents`                | strategies          | oms (consumer group `oms`)      | order and cancel intents                |
| `oms:events`                 | oms, order watcher  | strategies, matcher, recorder   | order lifecycle and fills               |
| `oms:latency`                | oms, broker         | recorder                        | timing records per intent               |

Rules

- A stream is the unit of subscription and of recording partitioning, so the
  key always carries venue and symbol where applicable. Symbols keep the CCXT
  form (`BTC/USDT`).
- Every stream is trimmed with `MAXLEN ~ N` so memory is bounded. The recorder
  is the durable copy.
- Fan-out streams (market data, order events) are read with plain `XREAD`.
  Each consumer keeps its own position. Consumer groups are used only where
  exactly one consumer must act on each entry, which is `oms:intents`.
- During migration the watcher also keeps writing the latest snapshot to the
  existing `{symbol}-{venue}` key, so current strategies continue to work.

### 3.2 Entry format

Each stream entry has exactly two fields:

- `type`: the event type string, so a consumer can dispatch without decoding.
- `data`: the event payload encoded as JSON.

JSON is the default because it is trivially readable from any language and
from the Redis CLI. The schema layer is codec-agnostic; MessagePack can be
enabled per stream later if payload size or decode time become a problem.

### 3.3 Timestamps

Every event carries

- `ts_recv`: local receive time in nanoseconds since the epoch, taken as soon
  as the CCXT call returns. This is the clock strategies reason in.
- `ts_exch`: exchange-provided time in milliseconds, or null if the venue does
  not supply one.
- `seq`: a per-stream monotonically increasing integer from the producer, so
  gaps can be detected.

Strategies never read the wall clock. They receive a `Clock` whose `now()` is
driven by the `ts_recv` of the last event delivered. In live mode that is
effectively the wall clock. In backtest it is the replayed time. Order id
generation and staleness checks go through the clock.

## 4. Event schemas

Schemas live in `apps/shared/src/events.py` as `msgspec.Struct` types. They
are the contract; a non-Python strategy implements them from the JSON shape.
Each struct has a `type` tag and a `v` schema version.

Market data

- `BookEvent`: venue, symbol, seq, ts_recv, ts_exch, bids, asks. Levels are
  `[price, size]` pairs as floats, top-N only (default 20). Floats are used
  because that is what CCXT delivers and what strategies compute on. Order
  quantities and prices that are sent to a venue remain `Decimal`.
- `TradeEvent`: venue, symbol, seq, ts_recv, ts_exch, trade_id, side, price,
  amount.

Account

- `BalanceEvent`: venue, seq, ts_recv, ts_exch, balances as
  `{asset: {free, used, total}}`.

Order management

- `OrderIntent`: intent_id, strategy, venue, symbol, side, order_type
  (`limit`, `market`), time_in_force, price, amount, ts_created,
  replace_of (optional intent_id this intent supersedes), tags.
- `CancelIntent`: intent_id, strategy, venue, symbol, target_intent_id,
  ts_created.
- `OrderEvent`: intent_id, strategy, venue, symbol, state (`accepted`,
  `rejected`, `open`, `partially_filled`, `filled`, `cancelled`, `expired`),
  venue_order_id, filled, remaining, avg_price, last_fill (optional
  `Fill`), reason, ts_recv, ts_exch.
- `Fill`: price, amount, fee, fee_currency, liquidity (`maker`, `taker`),
  venue_trade_id.
- `LatencyRecord`: intent_id, venue, ts_created, ts_oms_recv, ts_broker_send,
  ts_venue_ack.

The current `OrderMessage`, `CancellationMessage` and `OrderBatchMessage`
TypedDicts stay until phase 3, when the message processor moves to the new
intents.

## 5. Configuration

The config becomes typed (`apps/shared/src/config.py`) with this shape:

```toml
[redis]
host = "localhost"
port = 6379

[market_data]
book_depth = 20
stream_maxlen = 10000

[[venues]]
id = "gate"
name = "Gate.io"

[[strategies]]
identifier = "lam"
type = "single_edge_liquidity"
production = true
subscriptions = [
  { venue = "gate", symbol = "ALPH/USDT", feeds = ["book"] },
  { venue = "mexc", symbol = "ALPH/USDT", feeds = ["book"] },
]

[strategies.params]
spread = 1.0025
min_size_usdt = 11
```

Rules

- `subscriptions` is the only source of truth for which venue and symbol
  feeds the watchers run. Nothing is inferred from strategy parameters.
- There is no fallback from strategy parameters to subscriptions. The
  strategies repo was migrated to this shape on 2026-09-06, so a strategy
  without `subscriptions`, or with a parameter outside `[strategies.params]`,
  is a config error.
- Strategy parameters are free-form and passed through untouched. The
  strategy validates them.
- The existing module-level globals in `apps/shared/src/utils.py`
  (`strategies`, `exchange_and_pair`, `pairs`, `refresh_speed`) become thin
  views over the typed config so all current imports keep working.

## 6. Order management

The message processor becomes an order manager. It

- consumes `oms:intents` through a consumer group and acknowledges after the
  broker has replied, so a crash mid-flight replays the intent;
- keeps an order state machine per intent rather than one open order per
  strategy;
- publishes every state transition to `oms:events`;
- keeps the current per-strategy lock and latest-wins coalescing as an
  opt-in behaviour, selected through `replace_of` on the intent, because it
  is exactly what quote-replacement strategies want and harmful for others;
- writes `LatencyRecord`s.

The order watcher (today inside `matcher.py`) becomes a feed handler that
turns CCXT `watch_orders` updates into `OrderEvent`s. The matching logic
consumes `oms:events` like any strategy and moves to the strategies repo.

## 7. Recorder

A single process (`apps/maker/src/recorder.py`) that `XREAD`s every
configured stream and appends entries to files partitioned by stream and UTC
day, for example `data/md/book/gate/ALPH-USDT/2026-09-06.jsonl`. Each line is
`{"id": <redis stream id>, "type": <tag>, "data": <payload>}`; the payload is
copied verbatim without decoding, so the recorder never rejects an event and
stays cheap. The day partition comes from the stream id's millisecond prefix.
On start the recorder resumes every stream from the last id in its newest
file, so a restart neither duplicates nor drops what Redis still holds.

Files are uncompressed for now (Python 3.12 has no zstd in the standard
library and tailing the last line must stay trivial); compressing closed days
offline is a separate step. Parquet can replace JSONL once the schemas are
stable. The recorder is the only durable store for market data and is what
research notebooks and the backtester read.

## 8. Strategy runtime

A small Python SDK (`apps/shared/src/runtime.py`, later) that

- reads the strategy's declared subscriptions from config,
- runs one `XREAD` loop over all subscribed streams plus `oms:events`
  filtered to the strategy id,
- dispatches to `on_book`, `on_trade`, `on_balance`, `on_order_event` and
  `on_timer`,
- exposes `submit(intent)` and `cancel(intent_id)` which `XADD` to
  `oms:intents`,
- owns the `Clock`.

The existing `async def strategy(redis, config)` entry point stays valid;
the runtime is an additional way to write a strategy, not a replacement.
Non-Python strategies implement the stream contract directly.

## 9. Backtesting

Backtesting is replay plus simulation, reusing the live components:

- A replayer reads recorder files and `XADD`s them into streams under a
  separate key prefix (`bt:{run_id}:`), preserving relative `ts_recv`
  spacing or running as fast as possible.
- A simulated broker consumes `bt:{run_id}:oms:intents`. For each intent it
  draws an arrival delay from the per-venue latency model built from
  `oms:latency` records, looks up the recorded book at `ts_created + delay`,
  and fills against it. Limit orders that do not cross rest in a simple
  queue and fill when the recorded book trades through them.
- The strategy runs unchanged, pointed at the `bt:` prefix, with the clock
  driven by replayed timestamps.

The latency model is the part that decides whether a latency arbitrage
backtest is honest. It must come from measured data, never from a constant.

## 10. Language interoperability

The contract for a non-Python component is: connect to Redis, `XREAD` the
streams it needs, decode the JSON `data` field according to the schema
version, and `XADD` intents. No shared code is required. The Python
`events.py` module is the reference implementation and the tests in
`apps/shared/tests/test_events.py` double as schema fixtures.

## 11. Migration phases

1. Schemas, config model, constants. No behaviour change. Existing globals
   preserved as views. This document.
2. Watcher publishes `BookEvent`s to streams alongside the current snapshot
   keys. Add the trade watcher. Add the recorder. Move Redis host and port
   to config. Done 2026-09-06: the balance watcher also publishes
   `BalanceEvent`s, feed handlers are driven by declared `subscriptions`
   (a `trade` feed starts a `watch_trades` loop), and unknown feed names
   are a config error.
3. Message processor and broker move to `oms:intents` and `oms:events`.
   Order watcher publishes `OrderEvent`s. Matcher consumes them. Latency
   records start flowing.
4. Strategy runtime and clock. Port one existing strategy as validation.
5. Replayer and simulated broker.

Each phase leaves the system runnable with the current strategies.

## 12. Decisions

- Redis Streams over pubsub: persistence, ordering, replay and consumer
  groups for the price of a dependency already in use.
- msgspec for schemas: fast, validates on decode, one definition yields JSON
  and MessagePack, and produces plain JSON that other languages read.
- JSON as the initial codec: debuggability wins until measurements say
  otherwise.
- Floats in market data, `Decimal` in intents: matches CCXT output and
  keeps the money-touching path exact.
- Nanosecond `ts_recv`: the lag being traded is in the tens to hundreds of
  milliseconds, so millisecond resolution would hide the distribution.
- Recorder as the durable store, streams trimmed: keeps Redis memory bounded
  and decouples retention from the bus.

## 13. Open questions

- Retention and compression format for recorder files once volumes are known.
- Whether the orchestrator should restart crashed processes in production
  once intents are durable and replayable.
- Whether balances should also be published as deltas for strategies that
  care about inventory changes.
