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

The system spans two hosts. The **trading host** runs everything in the
diagram above: watchers, OMS, strategies, Redis and the recorder. It is
colocated with the venue for latency, so its disk, CPU and egress are all
treated as scarce. The **archive host** holds the long-term recording and
runs research and backtests. The two are connected over the public internet,
not a LAN, which makes egress volume a real cost and rules out a shared
filesystem. Section 7 describes the link between them.

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
  side, venue_order_id, filled, remaining, avg_price, last_fill (optional
  `Fill`), reason, tags, ts_recv, ts_exch. `side` was added in phase 3:
  anything reacting to a fill needs its direction, and the matcher would
  otherwise have to look the order up to hedge it the right way round.
  `filled` is cumulative and `last_fill` is the difference from the previous
  update of that order, which is the only way to recover a fill size from
  CCXT's order updates.
- `Fill`: price, amount, fee, fee_currency, liquidity (`maker`, `taker`),
  venue_trade_id.
- `LatencyRecord`: intent_id, venue, ts_created, ts_oms_recv, ts_broker_send,
  ts_broker_ack, ts_venue_ack. `ts_created` is the intent's own `ts_recv`.
  `ts_broker_send` is measured when the request leaves the order manager
  rather than when the broker reaches the venue: the broker's protocol
  carries no timing back, so its internal queueing shows up inside
  `ts_broker_ack` instead. An intent that arrives through the legacy bridge
  is stamped when the bridge receives it, so its `ts_created` excludes the
  pubsub hop the bridge adds.

The `OrderMessage`, `CancellationMessage` and `OrderBatchMessage`
TypedDicts survive phase 3 in two places, both of them adapters rather than
contracts: `legacy_bridge.py` translates them into intents on the way in, and
the order manager builds them again to speak to the broker, which still uses
the request-response pubsub protocol. They leave when the strategies emit
intents (phase 4) and when the broker moves to the bus.

## 5. Configuration

The config becomes typed (`apps/shared/src/config.py`) with this shape:

```toml
[redis]
host = "localhost"
port = 6379

[market_data]
book_depth = 20
stream_maxlen = 10000

[oms]
# Consumer name inside the `oms` group. Stable across restarts on purpose,
# so a restarted order manager reclaims its own unacknowledged intents.
consumer = "oms"
# Intents older than this are rejected rather than sent to a venue.
max_intent_age_s = 5.0

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

The message processor is an order manager. It

- consumes `oms:intents` through a consumer group and acknowledges after the
  broker has replied, so a crash mid-flight replays the intent;
- keeps an order state machine per intent rather than one open order per
  strategy;
- publishes every state transition to `oms:events`;
- keeps the per-strategy lock and latest-wins coalescing as an opt-in
  behaviour, selected through `replace_of` on the intent, because it is
  exactly what quote-replacement strategies want and harmful for others;
- writes `LatencyRecord`s.

Orders are keyed by venue **and** intent id, not by intent id alone: the
matcher hedges a fill by placing an order that reuses the filled order's
client id on the other venue, so an intent id is only unique per venue.

The book is fed from both directions. A placement enters it when the broker
confirms, and it leaves when the order watcher reports a terminal state on
`oms:events`. Without the second half, a filled order would sit in the book
until shutdown tried to cancel it. Reading a stream the same process writes
is harmless here because applying a state to the order it already describes
is idempotent.

Two guards bound what a replay can do. Acknowledging late means an intent can
be delivered twice, so an intent older than `oms.max_intent_age_s` is
rejected rather than sent to a venue: a stale quote is worse than a missing
one. And the consumer name is stable across restarts, so a restarted order
manager reclaims its own pending list instead of stranding it under a name
nothing will ever use again.

The order watcher is a feed handler (`order_watcher.py`) that turns CCXT
`watch_orders` updates into `OrderEvent`s. The matching logic consumes
`oms:events` like any strategy; it no longer holds an exchange connection at
all, which is what makes it movable to the strategies repo.

### 6.1 The legacy bridge

Strategies still publish dicts on the `messageprocessor` pubsub channel.
`legacy_bridge.py` runs inside the order manager, subscribes to that channel
and republishes each message to `oms:intents`, so the order manager has
exactly one input path and every order is recorded as an intent whatever
produced it. The shim is deleted in phase 4.

Two pieces of legacy vocabulary have no field on `OrderIntent` and travel as
tags. `replace` versus `unique` versus `market` becomes `legacy_order_type`,
because a legacy sender does not know the intent id it supersedes and so
cannot fill `replace_of`. And a cancellation with an empty id, which means
"cancel whatever I have resting", becomes a `CancelIntent` with an empty
`target_intent_id` that the order manager resolves against its own book.

## 7. Recording tier

Recording is two tiers on two hosts: the recorder writes a short, hot,
uncompressed window on the trading host, and a shipper moves sealed files to
the archive host, which is what research and the backtester read. The
trading host is never the long-term store.

### 7.1 Recorder

A single process (`apps/maker/src/recorder.py`) that `XREAD`s every
configured stream and appends entries to files partitioned by stream and UTC
hour, for example `data/md/book/gate/ALPH-USDT/2026-09-06T14.jsonl`. Each
line is `{"id": <redis stream id>, "type": <tag>, "data": <payload>}`; the
payload is copied verbatim without decoding, so the recorder never rejects an
event and stays cheap. The bucket comes from the stream id's millisecond
prefix. On start the recorder resumes every stream from the last id in its
newest file, so a restart neither duplicates nor drops what Redis still
holds.

Hourly rather than daily buckets, because the partition size sets how long
data sits unshipped on the trading host, and because it makes a backtest over
a sub-day range a filename filter rather than a seek. It costs nothing in
compression: measured on real book data, the zstd ratio is flat above roughly
300 KB per file (33.9x at 689 KB, 34.1x at 344 KB) and only degrades on
inputs far smaller than an hour of any active feed.

### 7.2 Sealed files

**The newest file in a stream directory is never touched by anything but the
recorder.** Rotation is driven by entry arrival, so a quiet stream can hold
its file open long after the bucket elapsed; a later file existing in the
directory is the only proof that an earlier one was closed. This single
invariant is what keeps three separate things correct:

- the recorder's open append fd is never unlinked underneath it, which would
  send every subsequent write to an unlinked inode with no error,
- `last_recorded_id` always has an uncompressed file to tail, so resume never
  falls back to `0-0` and re-records what Redis still holds,
- the shipper never transfers a file that is still being appended to.

Because three components depend on it, it is one tested predicate in
`streams.py` (`sealed_files`), not a rule each of them reimplements. The
recorder also seals on a timer, closing a file whose bucket elapsed more than
a grace period ago, so an idle stream does not pin its data on the trading
host. A lagging recorder can still receive an entry belonging to a sealed
bucket; it reopens the file in append mode, and the shipper detects the
change by checksum rather than by existence.

The predicate is deliberately conservative: it never reports a live file as
sealed, but it does not report every sealed file. "Not the newest bucket" is
a proxy for "the recorder has closed this", and the proxy has one blind spot
— the newest file itself, even long after the recorder closed it. So the tail
of the recording is never shippable while the system is stopped. That is
correct but incomplete, and section 7.4 is what the shipper needs in order to
close it.

### 7.3 Shipper

Sealed files are compressed with zstd `-3` on the trading host and
transferred to the archive host, which recompresses to `-19` for long-term
storage. The split follows from the measurements: `-3` is effectively free
and already achieves 25.1x, `-10` runs at 77.6 MB/s per core, and `-19` runs
at 2.8 MB/s for 40.3x. Spending `-19` on the trading host would cost hours of
a core per day; spending nothing and shipping raw would cost 25x the egress
on a metered link. Compressing cheaply before the wire and thoroughly after
it is the only option that keeps both scarce resources bounded.

A file is deleted from the trading host only once the archive host confirms
it by checksum, so local retention is "transferred", not an age. Until the
shipper exists there is no second copy, so nothing may be deleted and the
trading host grows without bound: see the precondition in section 11.

Uncompressed JSONL is the hot format because tailing the last line to resume
must stay trivial, which a compressed frame cannot do without decompressing
it whole. Compression itself needs no dependency: the project pins Python
3.14, whose standard library provides `compression.zstd`. Parquet can replace
JSONL in the archive tier once the schemas are stable; compaction is the
natural place to convert.

### 7.4 What the shipper needs first

Four changes, none of them urgent while nothing deletes anything, all of them
prerequisites for a shipper that does.

**1. Persist the cursor per stream, independent of the data files.** Resume
currently tails the newest recording. That couples it to which files happen
to be on local disk, which is precisely what a shipper changes. Left alone
the sequence is: the recorder stops at 17:03, the shipper takes and deletes
`T17`, the recorder restarts into an empty directory, resumes from `0-0` and
re-records everything Redis still holds. Writing the cursor on flush, to a
small per-stream file or a Redis hash the shipper never touches, removes the
coupling. It also makes resume O(1) rather than a backward scan, and lets
retention delete anything at all without consulting the recorder.
`last_recorded_id` stays as the fallback when no cursor is present.

**2. Make sealing final: the recorder never writes to a bucket it sealed.**
A late entry goes to the current bucket instead, carrying its true id and
`ts_recv` as always. Today's reopen is what makes "is this file closed?"
unanswerable from outside the process — a recorder catching up after a stall
receives entries whose ids belong to hours that ended long ago, and the file
flaps between closed by the timer and reopened by the next old entry. Nothing
can safely ship a file that might be reopened. The only thing given up is
that a file's name stops being an exact statement about its contents' time
range, and the name was never load-bearing: the replayer merges on `ts_recv`,
so the bucket is a coarse index, not a guarantee.

**3. Then `sealed_files` can widen** to "not the newest bucket, **or** the
bucket elapsed more than the grace plus a margin ago". With reopen
impossible, elapsed time is a sound proof of sealed in every case: the
recorder is running and past the grace (its timer closed the file), or it is
stopped (it is not writing at all), or it is lagging (it can no longer reach
that bucket). The stranded tail is then released roughly an hour after the
recorder stops rather than never.

**4. The replayer reads one bucket either side of a requested range.** Needed
once (2) lets an entry land in the following bucket, and worth doing anyway
given the skew between `ts_recv` and the XADD time the bucket is derived
from.

Two alternatives were considered and rejected. An advisory `flock` on the
open file is the race-free textbook answer, but it puts a lock acquisition in
the recorder's write path, and a trading process must never be able to stall
on the shipper. A `.sealed` sidecar marker per bucket does not eliminate the
reopen race, only moves it, and doubles the inode count.

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
  spacing or running as fast as possible. It runs on the archive host and
  resolves files through a helper that opens `.jsonl` and `.jsonl.zst`
  interchangeably, so it never hardcodes a tier's layout. Selecting a time
  range is a filename filter over hourly buckets; ordering across streams is
  a merge on `ts_recv`, not on the Redis entry id, which carries publish
  jitter and would desynchronise the merge from the `Clock`. A replay that
  crosses a `seq` gap or reset is reported, never silently spliced.
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
3. Message processor moves to `oms:intents` and `oms:events`. Order watcher
   publishes `OrderEvent`s. Matcher consumes them. Latency records start
   flowing. Done 2026-09-06: the order manager consumes intents through the
   `oms` consumer group, keeps a per-intent state machine keyed by venue and
   intent id, publishes every transition and a `LatencyRecord` per placement,
   and rejects intents older than `oms.max_intent_age_s`. The order watcher
   moved out of `matcher.py` into its own feed handler, leaving the matcher
   as a consumer of `oms:events` that emits hedges as intents. Legacy
   strategies reach the order manager through `legacy_bridge.py` (section
   6.1) rather than through a second code path inside it. The broker still
   speaks its request-response pubsub protocol; moving it onto the bus is
   deferred, since it is one hop behind the order manager and changing it
   buys nothing until the strategies move.

   Live-tested against MEXC and Bitget on 2026-09-06 and 07: intents, the
   bridge, placement, supersede, cancel-by-strategy, the order watcher, a
   forced fill hedged by the matcher, book pruning on fill, the staleness
   guard rejecting replayed intents, shutdown cancelling resting orders, and
   `fake_maker` quoting unchanged through the bridge. The run found two bugs
   no unit test could see, both since fixed and covered: entry ids arrive as
   bytes because `decode_responses` is ignored when a `ConnectionPool` is
   passed, and the pending-list drain re-read entries that were still
   unacknowledged.

   First measured order round trip, which section 9's latency model needs:
   **MEXC limit order, 326 to 492 ms** from the order manager publishing to
   the broker until the broker's reply, over four samples. That is the whole
   REST leg plus one local pubsub hop. It sits at the top of the range the
   motivation in section 1 assumes, so the round trip is worth measuring per
   venue before committing to any cross-venue lag under half a second.
4. Strategy runtime and clock. Port one existing strategy as validation.
5. Replayer and simulated broker.

Each phase leaves the system runnable with the current strategies.

The shipper and the archive host (section 7.3) are not part of this sequence.
They depend on nothing in phases 3 to 5 and block nothing in them, so they
are scheduled against a different trigger: **the shipper must exist before
any busy feed is added to `subscriptions`.** Until it does there is no second
copy of the recording, so nothing on the trading host may be deleted, and the
disk is bounded only by feed volume. At the two feeds running today that is
roughly 250 MB/day and over a year of headroom; at ten feeds on active pairs
it is roughly 15 GB/day and about a week.

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
- Recording split across two hosts: the trading host is colocated and its
  disk, CPU and egress are scarce, so it holds hours of data rather than
  years. Everything durable lives on the archive host.
- Hourly buckets and zstd `-3` local, `-19` remote: chosen from measurements
  on real book data, see sections 7.1 and 7.3. Both follow from the link
  between the hosts being a metered WAN rather than a LAN.
- JSON Lines with the payload spliced in verbatim: the recorder never decodes
  an event, so a producer can add a field without the recording tier knowing
  about it, and a replay is byte-identical to what was on the bus.
- Consumers resolve the tail of a stream once and then advance through
  concrete entry ids, rather than passing `$` on every `XREAD`. `$` is
  re-resolved per call, so an entry published while a consumer sits between
  two reads is skipped with nothing to show for it. On `oms:events` that is
  a fill nobody hedges.

## 13. Open questions

- Whether the orchestrator should restart crashed processes in production
  once intents are durable and replayable.
- Whether balances should also be published as deltas for strategies that
  care about inventory changes.
