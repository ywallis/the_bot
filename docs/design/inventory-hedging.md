# Inventory hedging: design

Status: draft, 2026-09-20, revised the same day with the operator's answers
(section 12 lists what is still open)
Scope: acquiring base inventory passively and holding it delta-neutral
against a short linear perpetual, on top of the event-driven framework
(`docs/design/event-driven-framework.md`). Venue and asset names are not in
this document on purpose; they live in the private config.

## 1. Motivation

Every strategy the framework runs needs inventory on both sides: base on the
venue it sells on, quote on the venue it buys on. The spot-to-spot hedge in
the matcher keeps the *total* base balance constant across venues, so once
inventory is in place the strategies themselves add no directional exposure.
The inventory itself does. Holding base is a long position in the asset, and
the runs of 2026-09-20 showed that the markets worth quoting are small,
volatile ones with a persistent offset between venues, exactly where holding
the asset overnight is the risk that dominates the few basis points a fill
earns.

Two things follow:

- The base inventory should be hedged with a short of the same size in a
  linear perpetual, so the operator is flat in the asset and exposed only to
  the quote currency, funding and fees.
- Acquiring the inventory should itself be done at maker prices, hedged
  fill by fill as it accumulates, rather than bought at market and hedged
  afterwards. Done that way the build is a cash-and-carry trade: buy spot
  below the perpetual, sell the perpetual, collect the basis and the
  funding. Whether that is a marginal profit or a marginal cost is a
  measurement, not a design choice, and section 8 says how to take it.

## 2. Goals and non-goals

Goals

- Hold the total spot base inventory across venues delta-neutral against a
  perpetual short, within a configured deadband, at all times the system
  is up, and establish the delta on start.
- Build inventory to a target in quote units, and unwind it, with every
  fill hedged on the perpetual within one round trip, reusing the matcher
  path that already does this for the maker strategies.
- Take the basis outright whenever it pays more than the fees to do so, on
  the way in and on the way out.
- Make derivatives a first-class instrument in the framework: positions,
  funding, contract sizing, reduce-only, margin mode and leverage, all
  behind config and the CCXT `has` map, never a hardcoded venue.
- Notify the operator of the conditions that need a human: funding turned
  against the position, delta out of band, margin thin, a hedge that could
  not be placed.
- Measure basis and funding before trading them, with the same screener
  discipline as the spot markets.

Non-goals

- Inverse (coin-margined) perpetuals or dated futures. Linear perpetuals
  settled in the quote currency are the only instrument that hedges a spot
  position one for one in base units.
- A pure carry strategy. The short is sized to the inventory the maker
  strategies need, not to the funding on offer. The design does not forbid
  sizing beyond that later, but nothing here optimises for it.
- Automatic unwinding on a funding or basis condition. Those notify; the
  operator decides.
- Any API key with withdrawal permission, ever. Programmatic transfers
  between wallets of one account are avoided too, section 4.

## 3. Concepts

**Instrument.** A venue id and a CCXT symbol. Spot symbols are
`BASE/QUOTE`; linear perpetuals are `BASE/QUOTE:QUOTE` in CCXT's unified
form, the suffix naming the settlement currency. Everything in the bus
already carries venue and symbol, so a perpetual travels through the
existing `OrderIntent` and `OrderEvent` unchanged. `base_quote` in
`apps/shared/src/fees.py` learns to strip the settle suffix.

**Venue as an account.** Today a venue is one CCXT client, spot by default.
Which account model the exchange offers decides how a perpetual is
configured, and the config supports both:

- *Unified account.* One margin pool covers spot and derivatives; spot
  holdings count as collateral for the short. One venue entry serves both
  symbol kinds on one client, and the perpetual is nothing more than a
  symbol with a settle suffix in a subscription. CCXT detects the account
  type from the venue's account settings at first use; `account =
  "unified"` in the venue config asserts it, so a key that is not on a
  unified account fails at start rather than trading on the wrong
  assumptions. One of the two configured venues offers this and is the
  preferred home for the perpetual (section 12).
- *Split wallets.* Spot and futures are different wallets, endpoints and
  websockets. A second venue entry names the same CCXT class with
  `market_type = "swap"`; `ccxt_id` defaults to `id`, so existing configs
  are untouched:

  ```toml
  [[venues]]
  id = "venuea"
  name = "Venue A spot"

  [[venues]]
  id = "venueaperp"
  ccxt_id = "venuea"
  name = "Venue A linear perpetuals"
  market_type = "swap"
  fee_currency = "quote"
  margin_mode = "cross"
  leverage = 2
  ```

  Every feed handler, the order manager, the broker and the order watcher
  then key on the venue id as they do now, and an intent id stays unique
  per venue. The API key's rate limit is shared between the two clients
  while CCXT's throttler is per instance, so conformance must run with
  both connected.

**Delta.** For one asset, the signed sum of spot base held across the
venues that hold it (from `BalanceEvent`s) plus the perpetual position in
base units (contracts times contract size, negative for a short). The
target delta is zero. Everything in this design is a way to move delta
towards zero cheaply and to know when it is not.

**Hedge target.** The matcher today maps a strategy identifier to a taker
*venue* and hedges on the same symbol. It becomes a map to an instrument,
so a fill on spot can be hedged on a perpetual. `taker_exchange` keeps
working as the shorthand for "same symbol, other venue".

## 4. Keys, wallets and collateral

The operator's constraint is that no key held by a 24/7 process may move
funds. The design respects it in this order of preference:

1. **Unified account, no transfers.** The spot long and the perpetual
   short sit in one margin pool. The short's collateral is the inventory
   it hedges, so a rally that lifts the spot holding lifts the collateral
   by the same amount and the pair is, in the venue's own margin
   arithmetic, close to riskless. No transfer is ever needed and the key
   carries trade permission only. This is the reason to prefer the
   unified venue over the other, and it makes "does the asset have a
   perpetual on the unified venue" a screening criterion for the next
   inventory candidate.
2. **Split wallets, manual collateral.** The futures wallet is funded by
   hand, at the leverage the operator chooses, and the margin guard
   (section 7.4) only notifies. The key carries trade permission on both
   wallets and nothing else. Both configured venues grant transfer and
   withdrawal as permissions separate from trading, so a trade-only key
   is available on both.
3. **Split wallets, programmatic transfer.** Not designed for. Both
   venues expose internal transfer as a permission distinct from
   withdrawal, so a key could be scoped to it, but the operator prefers not
   to grant it and the first option removes the need.

## 5. New events and feeds

Two events join `apps/shared/src/events.py`, both snapshots by contract
like `BalanceEvent`, so the runtime primes them at start.

- `PositionEvent` on `acct:position:{venue}`: venue, symbol, seq,
  ts_recv, ts_exch, side (`long`, `short`, `flat`), contracts (Decimal),
  contract_size (Decimal), base_amount (contracts times contract size,
  signed), entry_price, mark_price, liquidation_price, unrealized_pnl,
  margin_mode, leverage, collateral. One event per symbol per venue; a
  venue with no position publishes `flat` so consumers can distinguish
  "no position" from "no feed yet".
- `FundingEvent` on `md:funding:{venue}:{symbol}`: venue, symbol, seq,
  ts_recv, funding_rate, next_funding_ts, interval_s, mark_price,
  index_price. Basis is derived by consumers from mark against the spot
  book rather than published, since the spot side lives on another
  stream.

A third, small one carries what the reconciler computes:

- `DeltaEvent` on `acct:delta:{asset}`: asset, ts_recv, spot_base (per
  venue and total), perp_base, delta, in_flight_base (hedges sent and not
  yet reported), deadband. It is a snapshot; the accumulator reads it as
  its kill switch and the notifier reads it for the out-of-band alert.

Two feed names join `KNOWN_FEEDS` so subscriptions declare them:
`position` and `funding`. Both are REST polled. Neither configured venue
exposes `watchPositions` or a funding websocket through CCXT, so the
position feed runs `fetch_positions` on an interval (a few seconds is
enough: the position changes only when our own orders fill) and once more
immediately after every `OrderEvent` with a fill on that venue, which is
the moment a consumer needs the fresh figure. The funding feed polls
`fetch_funding_rate` every minute or so; the rate itself changes once per
funding interval.

The balance watcher needs no change: on a `swap` venue `fetch_balance`
returns the futures wallet, on a unified venue the unified pool, and in
both cases it holds collateral and not the position, which is what a
`BalanceEvent` for that venue should say.

All new events are recorded like every other stream and added to the
schema tests that double as fixtures.

## 6. The order path on a perpetual

Four things the spot path never had to know.

**Contracts.** CCXT sizes contract orders in contracts, and
`market["contractSize"]` says how much base one contract is. `SymbolFees`
gains `contract_size` (None on spot) so hedge sizing can convert base to
contracts and quantize to the market's amount step, and the residual base
that does not fit in a whole contract goes on the delta rather than being
silently dropped. `OrderIntent.amount` stays "the amount CCXT expects for
this market", contracts on a perpetual, and the conversion happens where
the intent is sized, with `base_amount` reported on the `PositionEvent` so
the two are never confused downstream.

**Reduce-only.** An intent that unwinds a hedge must not be allowed to open
a position the other way if the venue's view of the position differs from
ours. `OrderIntent` gains `reduce_only: bool = False`, carried to the broker
message and to CCXT's `reduceOnly` param, and rejected by the order manager
on a spot symbol. The venue's rejection of a reduce-only that would flip is
a loud signal that delta is wrong, which is what we want. One configured
venue does not advertise the capability in its `has` map; conformance
checks the param is honoured rather than ignored.

**Leverage and margin mode.** Set once, at broker start, from the venue's
config: `set_margin_mode`, `set_leverage` and, where the venue has it,
one-way position mode. Failure to set them is fatal for that venue: a
short opened in hedge mode or at 20x is not the position the design
describes. Both configured venues expose the three setters through CCXT.

**Client order ids.** Futures endpoints can have different length and
character rules for client order ids from the spot endpoints of the same
venue. The `t-<stamp>_<strategy>_<slot>` format is 18 digits plus two
identifiers; `venue_conformance` grows a `--market-type swap` mode that
sets leverage, places and cancels a minimum order with that id, places and
cancels a reduce-only order, and reads the position back, before any money
goes on. On the venue whose futures order endpoint CCXT documents as
intermittently "under maintenance" this run is the only way to learn
whether the account may place futures orders through the API at all.

## 7. Components

```
 spot venues ──ws──> watchers, balance ──XADD──> md:book:*, acct:balance:*
 perp venue  ──rest─> position feed    ──XADD──> acct:position:{perp}
             ──rest─> funding feed     ──XADD──> md:funding:{perp}:{sym}
                                                        │
                       inventory strategy ◄─────────────┤  build or unwind, quote or take
                       delta reconciler ◄───────────────┘  timer, deadband, margin guard
                             │ XADD                 │ XADD      matcher ◄── oms:events
                             ▼                      ▼              │ hedge target = perp
                        oms:intents ◄───────────────────────────────┘
                                          acct:delta:{asset} ──> notifier ──> operator
```

### 7.1 Matcher: hedge on an instrument

`should_match` currently yields `{strategy identifier: taker venue}`. It
becomes `{strategy identifier: HedgeTarget(venue, symbol)}`, built from a
`hedge = { venue = "...", symbol = "..." }` param, with `taker_exchange`
kept as the spot shorthand. `hedge_quantity` takes the target's schedule
entry, converts to contracts when `contract_size` is set, and applies
reduce-only when the fill *reduces* inventory (a spot sell hedged by a
perpetual buy). The `HedgeBook`, incremental hedging, the fee check and the
id scheme all carry over untouched. The matcher stays a separate process
for the reason it already is one: a strategy that crashes mid-fill must not
leave the fill unhedged.

Two additions:

- **Residual carry-over.** A fill of 37 base units against a contract of
  10 hedges 30. The `HedgeBook` records the base actually hedged, not the
  base reported filled, so the 7 are added to the next fill of the same
  strategy and hedged when they make a whole contract. Until then they are
  on the delta and the reconciler can see them. The `hedge_of` tag and the
  PnL report's merge of several hedge legs already handle a fill hedged in
  pieces.
- **Self-hedged intents.** A strategy that places both legs itself
  (section 7.2, take mode) tags the spot leg `self_hedged`, and the matcher
  skips it. Without that the taken basis would be hedged twice.

### 7.2 Inventory strategy (private repo)

One strategy type with a `direction` of `build` or `unwind` and a
`target_quote`. It subscribes to the spot book, the perpetual book and
funding, the balances of every venue that holds the asset, and the delta
stream. Progress is measured as total spot base across those balances
times the spot bid, against `target_quote`; it stops quoting when the
target is reached and stops at once when the delta stream reports more
than `max_unhedged_base` or has gone stale, so a perpetual that is
refusing orders does not keep filling spot.

It has two ways of trading, evaluated on every book update, take first:

**Quote.** A one-sided `MakerQuoter`: post-only bid on spot when building,
post-only ask when unwinding, priced off the perpetual's touch rather than
another spot venue's, since the perpetual is what the fill is hedged into.
The quote condition is the carry edge in basis points of the spot price:

```
build:   edge = perp_bid  * (1 - perp_taker_fee) - spot_bid * (1 + spot_maker_fee)
unwind:  edge = spot_ask  * (1 - spot_maker_fee)  - perp_ask * (1 + perp_taker_fee)
```

with `min_edge_bps` from config, which may be negative: a build that pays
a few basis points to be flat is still what the operator asked for, and
the parameter is where that price is set. Size follows the existing
`min_size_usdt` and `max_size_usdt`, quantized to whole contracts so a
fill hedges exactly, and `hedgeable` checks the perpetual's depth covers
the resting size. The fill is hedged by the matcher, one round trip after
it is reported.

**Take.** When the basis pays more than the fees to cross both books,

```
build:   perp_bid * (1 - perp_taker_fee) > spot_ask * (1 + spot_taker_fee) + take_edge_bps
unwind:  spot_bid * (1 - spot_taker_fee)  > perp_ask * (1 + perp_taker_fee) + take_edge_bps
```

the strategy takes at once, the way `take_take` does: both legs as
independent intents sharing one id, sized to the smaller of the two
touches and the remaining target, the perpetual leg reduce-only on an
unwind, the spot leg tagged `self_hedged`. Both legs are IOC limit orders
at the touch, so a book that moved in the round trip leaves nothing
resting. Leg risk is the one way this opens exposure: one leg fills and
the other does not. The strategy hears both on `oms:events`; an unfilled
perpetual leg after a filled spot leg is hedged at market immediately, an
unfilled spot leg after a filled perpetual leg is closed reduce-only at
market immediately, and either case is an alert. Since the whole point of
taking is that the basis is wide, the retry crosses a book that just paid
for it.

**Fast unwind.** `direction = "unwind"` with `fast = true` skips the edge
gate and the passive ask: it sells spot at market in slices bounded by
`max_slice_usdt` and the visible depth at `max_slippage_bps`, each slice
paired with a reduce-only perpetual buy as in take mode, until the
inventory is gone. It exists for the day the operator wants out and does
not care what the basis is.

### 7.3 Delta reconciler (private repo)

The reconciler is to inventory what the order watcher's periodic REST
reconcile is to orders: the fill-by-fill path is fast and sees only what
it is shown, and something independent has to check the whole state
against the venues on a timer and correct what the fast path could not
see. It subscribes to the balance of every venue that holds the asset and
to the perpetual's position feed, and on every `interval_s` (sixty seconds
by default) it computes the delta of section 3 and publishes it as a
`DeltaEvent`. When the delta has been outside `deadband_base` for a full
interval, it hedges the difference with a market order on the perpetual,
reduce-only when shrinking the short, tagged so the PnL report can tell a
reconciliation from a fill hedge.

What it exists to catch, none of which a fill-triggered hedge can see:

- the residual under one contract that the matcher carries over, if no
  further fill ever comes to complete it;
- fees a spot venue charges in base, which erode the holding a few basis
  points at a time;
- a hedge the matcher could not place: rejected by the venue, lost to a
  network error, or sized to zero;
- a deposit, a withdrawal, a manual trade, or a spot-to-spot hedge whose
  taker leg only partly filled;
- fills that happened while the system was down, which the order watcher
  reports on restart but nothing hedges;
- and the very first minute after start, when it establishes what the
  delta is before any strategy is allowed to add to it.

The full-interval rule, rather than a deadband alone, is what keeps it
from acting on transients: a spot-to-spot hedge in flight for three
hundred milliseconds, or a fill hedge the matcher sent whose fill the
position feed has not yet reported. It also subtracts `in_flight_base`,
the base of hedge intents it has seen on `oms:intents` without a terminal
event on `oms:events`, so it never sends what the matcher is already
sending.

Scope is one asset per reconciler instance. Several assets are several
`[[strategies]]` entries of the same type; nothing is shared between them,
so a wrong parameter on one cannot touch another.

### 7.4 Margin guard

Part of the reconciler, since it already holds the position. On a
unified account the spot long is the short's collateral and the guard is
mostly a formality; on split wallets it is what stands between a rally
and a liquidation. It reads `collateral`, `mark_price` and
`liquidation_price` from the `PositionEvent` and, when the distance to
liquidation falls under `margin_floor_pct`, publishes an alert and stops
the inventory strategy's building through the delta stream. It moves no
funds (section 4). At `leverage = 2` on split wallets the distance is
around forty percent of the entry price.

### 7.5 Notifier

A small consumer of `acct:delta:*`, `md:funding:*` and `oms:events`
that publishes `AlertEvent`s on an `alerts` stream and delivers them to
the operator over a channel still to be chosen (section 12). It is the
only process that talks to anything outside the trading host and Redis.
Alerts, each with a cooldown so a persisting condition is one message,
not one a minute:

- funding below `min_funding_bps` for the last interval, and again when
  it turns positive;
- delta outside its deadband for more than `alert_after_intervals`;
- margin distance under the floor;
- a hedge rejected or unplaceable, a take that left one leg open;
- the position or funding feed stale for more than `stale_after_s`.

### 7.6 Orchestration and shutdown

The position and funding feeds are feed handlers and stop with the feeds.
The inventory strategy and reconciler are strategies run by the launcher
and stop in the first phase; the reconciler's last act on `SIGTERM` is
nothing, on purpose: a short left standing against inventory is the
correct state to shut down in, and a `wind_down` that flattened it would
create the exposure the design exists to remove. The order manager's
sweep still cancels the inventory strategy's resting quote, which is a
`REPLACE_RESTING` quote like any other. The notifier stops with the
recorder, so an alert raised during the wind-down still goes out.

## 8. Economics

The build is a cash-and-carry trade. For `N` base, in basis points of the
spot price:

```
entry   = basis_at_hedge - spot_fee - perp_taker_fee - perp_slippage
carry   = sum of funding received while the short is held (positive rate pays the short)
exit    = -basis_at_unwind - spot_fee - perp_taker_fee - perp_slippage
total   = entry + carry + exit
```

where `spot_fee` is the maker rate in quote mode and the taker rate in
take mode. The maker fee on the spot venue is zero on one of the venues
configured today, so a quoted round trip is dominated by two perpetual
taker fees and whatever the basis does between entry and exit. The trade
is a marginal profit when the perpetual trades at a premium at entry and
funding stays positive for the holding period; it is a small, bounded
cost otherwise, which is the price of being flat. Funding on small
perpetuals is volatile and can be negative for days, so this is a
distribution to measure, not a constant to assume.

The screener therefore gains, for every spot market whose asset has a
linear perpetual on a configured venue: the perpetual's touch and depth,
the current basis against each spot venue's bid, the current and
trailing-week funding rate, the round-trip cost above, and whether the
perpetual is on the unified venue. Two new columns in `market_screener`
and one new source of data (funding history over REST, which both venues
expose). A candidate for the inventory strategy wants a week of funding
history before it is built, the same way a spot candidate wants a day of
recording before it is quoted.

## 9. Accounting

`apps/maker/src/tools/pnl.py` values a maker fill against the hedge that
shares its id. It learns to value a hedge leg on a perpetual (contracts
times contract size at the fill price), to pair the two legs of a take by
their shared id, and grows a second report: the carry, from
`fetch_funding_history`, and the unrealized basis, from the position's
entry price against the spot holding marked at the spot bid. The
reconciler's delta stream is what the report reads to say, at any time,
how far from flat the book was, and its tagged hedges say how much of the
flattening was drift correction.

## 10. Risks

- **Liquidation of the short.** Near-absent on a unified account, where
  the spot long is the collateral. On split wallets bounded by leverage
  and the margin guard, which notifies and stops building but cannot add
  collateral. Auto deleveraging by the venue in a squeeze is not something
  the guard can prevent; low leverage is the only defence.
- **Funding flips negative.** The hedge becomes a cost. Notified past
  `min_funding_bps`; never unwound automatically, because an unhedged
  inventory is worse than a negative carry.
- **Leg risk in take mode.** One leg fills, the other does not. Closed at
  market within a round trip and alerted (section 7.2); bounded by the
  take size.
- **The perpetual is delisted or halts.** The venue's markets reload
  reports it, building stops, the position remains. Manual.
- **The futures API is not open to the account.** One configured venue's
  futures order endpoint is documented by CCXT as intermittently under
  maintenance. Conformance decides before anything is built there.
- **Basis widens against the position.** Unrealised, and realised only at
  unwind. Reported by section 9; not hedged.
- **Two clients on one API key.** Split-wallet case only. Rate limit
  shared, throttler not. Conformance runs with both.
- **Contract rounding.** Residual is carried over and on the delta, never
  dropped (section 7.1).
- **Key permissions.** Trade only. No withdrawal, and no transfer unless
  the operator changes section 4.

## 11. Phases

1. Instrument plumbing in the public framework: `ccxt_id`, `market_type`
   and `account` on venues, settle-aware `base_quote`, `contract_size` on
   `SymbolFees`, `reduce_only` and the `self_hedged` tag on intents,
   leverage and margin setup at broker start, `venue_conformance
   --market-type swap`. No behaviour change for spot configs. Run
   conformance on both venues with the operator's keys: this is where the
   venue question of section 12 gets its answer.
2. Position, funding and delta events and feeds, recorded and primed. The
   screener's basis, funding and unified-venue columns.
3. Matcher hedges on an instrument; contract sizing with residual
   carry-over; reduce-only on shrinking hedges; `self_hedged` skip. Tested
   against fakeredis with a swap schedule.
4. Delta reconciler and margin guard, run first in a mode that only
   publishes the delta and logs what it would do. Notifier with the chosen
   channel.
5. Inventory strategy, quote mode, first in `observe` on a candidate with
   a week of funding history, then with a small `target_quote`. Then take
   mode, then unwind and fast unwind.
6. PnL report: perpetual legs, takes, carry, unrealized basis.

Each phase leaves the spot system running unchanged.

## 12. Decided and still open

Decided on 2026-09-20 with the operator:

- The perpetual lives on the same exchange as the inventory, preferably
  one with a unified account, for capital efficiency and to avoid
  transfers altogether.
- Fill by fill through the matcher, with the reconciler behind it. The
  alternative, target tracking alone, was considered and rejected: it
  leaves every fill unhedged for up to an interval, and its one merit,
  fewer and larger perpetual orders, is had anyway from the matcher's
  residual carry-over.
- No key moves funds. Unified account first; split wallets funded by hand
  otherwise; the margin guard notifies and stops building, nothing more.
- Take the basis whenever it pays more than the fees, on the way in and
  out.
- Target sized in quote units; inventory, not carry, is the purpose.
- Unwind in scope, patient with an edge gate and fast without one.
- Negative funding notifies; nothing unwinds automatically.
- One reconciler per asset, several assets as several entries.
- Alerts go to a Telegram bot. The notifier's one dependency and one
  secret follow from that.
- `min_edge_bps = 0` for the first build: the accumulator waits for the
  basis to cover the fees and pays nothing to be flat.

Still open:

1. **Which venue's perpetual for the current candidate.** The candidate
   from the 2026-09-20 screen has a linear perpetual on the venue
   *without* a unified account, and that venue's futures order API needs
   the conformance run of phase 1 before it can be counted on. If it fails,
   the choice is between a candidate whose perpetual is on the unified
   venue and the split-wallet setup on the other.

## 13. Phase 1, 2026-09-20

Built the same day, on `feat/inventory-hedging-phase-1`, with no behaviour
change for a spot config:

- `VenueConfig` gained `ccxt_id`, `market_type`, `account`, `margin_mode`,
  `leverage` and `credentials`, with `exchange`, `env_prefix`,
  `derivatives` and `accepts` derived from them. Duplicate venue ids and a
  symbol subscribed on a venue whose market type cannot trade it are
  config errors. `is_contract` lives in `config.py`; `base_quote` strips
  the settle suffix and `settle_of` returns it.
- `SymbolFees.contract_size`, read from CCXT's `contractSize` on markets
  flagged `contract`; None on spot.
- `OrderIntent.reduce_only`, carried through the broker message to CCXT's
  `reduceOnly`, rejected by the order manager on a spot symbol.
  `SELF_HEDGED_TAG` is defined; the matcher learns to honour it in phase 3.
- The client builder constructs a venue from `exchange`, reads credentials
  under `env_prefix`, sets `defaultType` for a swap venue and overrides
  the CCXT client's `id` with the venue id, since every feed handler
  publishes under `client.id`. Signing was checked to be unaffected on
  both configured venues. The broker recollects open orders per venue over
  that venue's own symbols rather than every symbol on every client.
- `apps/shared/src/derivatives.py` applies margin mode, leverage and
  one-way position mode per contract symbol at broker start, tolerates
  "already set", skips setters a venue lacks with a warning, and raises on
  a refusal, which exits the broker.
- `venue_conformance --market-type swap` with the checks of the runbook's
  section 7, writing nothing unless `--place-orders` is passed.

Not run yet: the conformance tool against either venue's futures account
with the operator's keys. That run answers the remaining open question.
