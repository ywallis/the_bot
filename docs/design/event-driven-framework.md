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
  replace_of, tags. `replace_of` has three values: `null` is an independent
  order, placed as is and left alone at shutdown; an intent id names the
  quote this one supersedes; and the empty string (`REPLACE_RESTING` in
  `events.py`) is a quote with nothing to name yet, which still rests under
  its strategy key so that the next quote supersedes it and shutdown cancels
  it. It mirrors the empty `target_intent_id` of a `CancelIntent`.
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
TypedDicts survive in one place, as an adapter rather than a contract: the
order manager builds them to speak to the broker, which still uses the
request-response pubsub protocol. They leave when the broker moves to the
bus. The `legacy_order_type` tag survives on orders recollected from a venue
at startup, so an adopted order is classified the way it was placed; new
intents never carry it.

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

A superseding intent cancels whatever rests under its strategy key, whether
or not `replace_of` names it. A strategy's view of what rests can lag the
order manager: when two quotes queue behind a busy slot the older is
rejected unplaced, and the newer names it while the venue still holds the
quote before both. Only the resting slot knows that order, and a slot never
holds more than one, so the slot is what gets cleared.

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

After every websocket drop it survives, the order watcher reconciles over
REST: it fetches every order that changed since five seconds before the
last update the socket delivered and publishes whatever has not been
published, deduplicated by the same update keys the socket path uses. The
venue's replay after a resubscription only covers open orders, so an order
placed and finished inside the gap, a fill included, never comes back over
the socket. The overnight run of 2026-09-08 saw two such orders in one
four-second reconnect. Venues differ in what they expose, so the fetch is
`fetch_orders` where the venue has it (MEXC) and open plus cancelled-and-
closed orders otherwise (Bitget). A failed reconciliation is logged and the
socket loop carries on; the next drop reconciles again.

### 6.1 The legacy bridge

Until phase 4, strategies published dicts on the `messageprocessor` pubsub
channel and `legacy_bridge.py`, running inside the order manager, republished
each as an intent so that the order manager had one input path. With every
strategy on the runtime the bridge was deleted along with the channel and
the manual simulators that drove it. Anything that wants an order placed
publishes an intent.

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

`apps/shared/src/runtime.py` is a small SDK for writing a strategy against
the streams. A strategy subclasses `Strategy`, overrides the hooks it needs
(`on_start`, `on_book`, `on_trade`, `on_balance`, `on_order_event`,
`on_timer`) and never touches Redis. The `Runtime`

- derives the streams to read from the strategy's declared `subscriptions`:
  one book or trade stream per subscribed feed, the balance stream of every
  subscribed venue, and `oms:events` filtered to the strategy's own orders
  (those whose strategy key starts with its identifier);
- runs one `XREAD` over all of them, resolving each tail once and then
  advancing through concrete entry ids (section 12);
- owns the `Clock` and advances it with the `ts_recv` of every delivered
  event before dispatching;
- builds intents through `order_intent`, which stamps them with the clock,
  names the strategy key `<identifier>_<slot>` and generates the client
  order id `t-<stamp>_<identifier>_<slot>` that the order watcher parses.
  The stamp is clock time to the microsecond, bumped as needed so two ids
  from one runtime never collide even when the clock has not moved, which
  under replay it may not have. Identifiers containing `_` or `-` are
  refused at construction because the id format splits on them;
- exposes `submit(intent)`, `cancel(intent_id)`, which rebuilds venue and
  symbol from the runtime's own record of what it submitted, and
  `cancel_resting(venue, symbol, slot)`, the empty-target cancel a strategy
  sends at start to clear what an earlier run left behind;
- primes the strategy at start with the latest entry of every snapshot
  stream, the subscribed balances first and then the books, before reading
  new entries. A balance or a book entry supersedes every earlier one, so
  the last one is complete state; a trade or an order event is not and is
  never primed. Without priming, a strategy that starts after the feed
  handlers, which the orchestrator guarantees, would not see a balance
  until one changed, and a balance changes when an order fills, which no
  quote is sent without a balance to fund it;
- reads and writes every stream under an optional key prefix, so a backtest
  is a prefix and a replay clock away.

The clock has two modes. Live, `now()` is the wall clock, which is what the
order manager's staleness guard and the latency records expect. Under replay
it is the `ts_recv` of the last delivered event and reads 0 before the
first. `advance` never moves time backwards: streams are merged by entry id,
which carries publish jitter, so an event can arrive after one it was
received before. Timers are clock time too: `on_timer` fires when
`timer_interval_s` has elapsed on the runtime's clock, so under replay a
timer fires by recorded time, and an idle replay fires none.

Strategy modules export a `STRATEGY` class; the launcher constructs it with
the strategy's `StrategyConfig` and runs it. A module without `STRATEGY` is
run the old way, `async def <type>(redis, strategy_dict)`, so both shapes
stay valid and the runtime is an additional way to write a strategy, not a
replacement. Non-Python strategies implement the stream contract directly.

The two maker strategies were ported in phase 4 and share a skeleton,
`MakerQuoter` in the strategies repo: latest book per venue, latest balance
per venue, at most one resting order per side, requote on every maker or
taker book update, and a per-strategy `quotes` and `action` that supply the
pricing and the keep, replace or cancel rule. The port changed one thing
about how they behave. The legacy loop polled both books in one tick and
saw a move on two venues as one replace; event by event the two updates
arrive separately, and against a half-updated pair of books the strategy
may cancel a side it will requote a moment later. That is the strategy
seeing the market as it is rather than an artefact: the venues do move at
different times, and the coalescing in the order manager absorbs the
churn. The strategies also learned to free a slot on a terminal order
event, which the polling versions never did, so a filled quote is not
"replaced" by naming an order that is gone.

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
   Done 2026-09-07: `runtime.py` with `Clock`, `Strategy` and `Runtime`
   (section 8), the launcher running either shape of strategy module, and
   both configured maker strategies, `fake_maker` and
   `single_edge_liquidity`, ported onto a shared `MakerQuoter` skeleton in
   the strategies repo, then `take_take` as well, and the legacy bridge
   deleted with the pubsub channel it served (section 6.1). Two contract
   changes fell out of the port: the `REPLACE_RESTING` value of
   `replace_of` (section 4), and the order manager clearing the resting
   slot on any superseding intent rather than only the order it names
   (section 6). `take_take` places its two legs as independent intents
   sharing one id, one per venue, and keeps its throttles as clock-time
   rules: both books and both balances must postdate the last pair. The
   strategy tests now drive the real runtime on `fakeredis` and assert on
   the intents stream; the legacy cases were translated one for one and
   produce the same orders.

   Live-tested on 2026-09-07 through the orchestrator against MEXC and
   Bitget, in two runs. The first stood still for eight minutes: the
   balance watcher publishes one snapshot at its own start and then only on
   change, the strategies start sixty seconds later at the stream tail, and
   a strategy with no balance judges every quote insolvent. Nothing in the
   unit suite could see this, because every test funded the strategy after
   it started. The runtime now primes snapshot streams at start (section
   8). The second run went end to end: both strategies primed with four
   snapshots, `fake_maker` quoted a 273 ALPH sell on MEXC, the order
   manager placed it, the order watcher confirmed it independently, and
   shutdown cancelled it. No tracebacks, balances unchanged, nothing left
   open at either venue, and every phase 3 invariant held (one placement,
   one latency record, nothing in limbo, one resting order per slot,
   nothing resting after shutdown). The quote rested unchanged for four
   minutes because the Bitget ask it was priced from did not move. Nothing
   filled, so the strategies' slot-freeing on a fill and the matcher's hedge
   remain tested only against `fakeredis`.

   A third, fifteen minute run the same afternoon exercised the paths the
   second had not: `fake_maker` quoted the instant it was primed, replaced
   its quote twice on size changes (each `replace_of` naming the order it
   superseded, and the order manager cancelling that order before placing),
   cancelled it twice when the quote condition went away, and requoted from
   an empty slot with `REPLACE_RESTING`. Four placements, four order
   watcher confirmations, four cancellations confirmed both ways, one
   resting order per slot throughout, nothing resting at shutdown, no
   errors, balances unchanged. Still no fill.

   A fourth run of 91 minutes (16:09 to 17:40) held every invariant at
   volume: 191 order intents from both strategies, 181 placements each
   confirmed by the order watcher, 181 cancellations confirmed both ways,
   10 superseded in the queue, no duplicate placements, nothing in limbo,
   one resting order per slot throughout, venue snapshots at three
   checkpoints matching the order manager's book exactly, nothing open at
   shutdown, no errors, balances unchanged. Still no fill: ALPH fell from
   0.0510 to 0.0493 through the afternoon and a sell quote 0.5% above the
   Bitget ask sat 0.66% above MEXC's own ask. On the basis of that run's
   books the `fmb` spread was lowered to 1.003 with `min_spread` 1.002: the
   hedge costs 0.10% Bitget taker fee plus up to 0.02% slippage, and the
   Bitget ask moved less than 0.3% over the one second hedge latency in
   97% of moments, 95th percentile 0.28% even after a MEXC ask jump.

   The run also quantified the event-by-event churn described in section
   8: 136 cancels on request against 45 replacements, so three quotes in
   four were cancelled and requoted into an empty slot rather than
   replaced, because one venue's book update briefly failed the quote
   condition before the other's restored it. Correct, but each such pair
   costs the same two round trips as a replace and leaves the slot empty
   in between. A grace period before cancelling on a vanished condition is
   the obvious mitigation and is not yet built.

   Latency over 181 native placements, no bridge hop: strategy to order
   manager 0.3 to 2.2 ms, median 0.6; inside the order manager median 0.6
   ms, up to 961 ms for a replace waiting on its cancel round trip; broker
   round trip median 313 ms, range 290 to 813 ms.

   A fifth run at the new spread went overnight, 17:44 on 2026-09-07 to
   07:43 on 2026-09-08, just under fourteen hours, with a checkpoint every
   thirty minutes that compared the order manager's book with the venue's
   open orders. 2233 quotes, 2159 placements each with a latency record,
   2158 cancellations, 74 superseded in the queue, no duplicate placement,
   nothing in limbo, one resting order per slot at all 27 checkpoints and
   the venue agreeing every time, balances unchanged, no traceback in
   15,600 log lines. Broker round trip over 2159 placements: median 305
   ms, range 285 to 1261 ms. Still no fill: the nearest a MEXC trade came
   to a resting quote was 0.04% below it, and the median gap was 0.40%,
   because the quote is repriced off the Bitget ask on every update and
   moves with the market. It fills on an aggressive sweep, and there was
   none in 500-odd MEXC trades.

   Findings from that run, none of them in the unit suite's reach:

   - **The order watcher was blind while it reconnected.** Venues closed
     websockets 26 times in fourteen hours, in clusters at roughly ten past
     the hour, and every loop reconnected. But two quotes placed and
     cancelled inside the seconds of a MEXC order-feed reconnect at 07:20
     never appeared on the watcher's side of `oms:events`: the venue's
     cache replay after resubscription does not include orders already
     closed. The order manager's book was right, because it works over
     REST. A fill in that window would have been invisible to the matcher
     and gone unhedged. Fixed the same day: the watcher reconciles over
     REST after every reconnect (section 6).
   - **Shutdown killed the recorder and the watcher before the order
     manager finished.** The orchestrator signalled every process at once.
     The order manager then spent about 300 ms cancelling what rested and
     published the `cancelled` event at 07:42:53.030; the recorder had
     stopped at 07:42:52.728 and the last event on disk was from 07:42:44.
     The final cancellation was on the bus and at the venue but not in the
     recording, and the watcher's confirmation of it was never produced.
     Fixed the same day: the orchestrator now stops in phases, strategies,
     then the order manager with a twenty second grace, then feed handlers
     and broker, then the recorder, each phase fully down before the next
     is signalled (section 12).
   - **A stream's Redis window is short at this quote rate.** `oms:events`
     holds 10,000 entries, which was about four hours of the overnight run;
     any analysis over a longer span must read the recorder files, which
     is what they are for, and the checkpoint tooling was switched to them
     mid-run once the counts started sliding.
   - **The cancel-then-requote churn is a quiet-market effect.** In slow
     hours three quotes in four were cancelled and requoted into an empty
     slot; in the busy morning session replacements outnumbered cancels.
     Over the whole run 1173 replacements against 985 cancels on request. A
     grace period before cancelling on a vanished condition remains the
     mitigation.
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
- The clock is the wall clock live and event time under replay, rather
  than event time in both modes. Stamping a live intent with the last
  book's `ts_recv` would put the strategy's own reaction time onto the
  strategy-to-OMS leg of every latency record, and would let an idle
  strategy's intents age into the staleness guard.
- `replace_of` distinguishes "independent order" (`null`) from "quote with
  nothing to name yet" (empty string) instead of adding a boolean field:
  the order manager already reads an empty `target_intent_id` as "whatever
  rests", so the vocabulary exists, and a field that is only meaningful
  when another is null is a worse contract than a sentinel.
- The orchestrator winds down in phases rather than signalling everything
  at once: strategies, then the order manager, then feed handlers and the
  broker, then the recorder. The order manager needs the broker alive to
  cancel what rests, the order watcher alive to confirm it, and the
  recorder alive to record it; stopping them together lost the final
  cancellations from the recording in the first overnight run.
- The order watcher reconciles over REST after every reconnect instead of
  trusting the venue's replay, because the replay covers open orders only
  and a fill that happened during the gap is exactly the order that is no
  longer open.
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
