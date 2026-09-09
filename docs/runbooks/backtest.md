# Runbook: running a backtest

A backtest replays a recording under a key prefix, `bt:<run_id>`, and runs
the strategies, a simulated broker and the matcher against it. Every process
below is a live module with a replay flag, so the code under test is the code
that trades. Design: `docs/design/event-driven-framework.md`, section 9.

## 0. What you need

- A recording root with the streams of the range you want: `data/` on the
  trading host, or a copy of it. The replayer reads `.jsonl` and
  `.jsonl.zst` alike. The whole `oms/latency` directory should be there
  too, whatever the range: it is the latency model.
- A Redis that is not the trading one. A backtest writes under `bt:` only,
  but an unpaced replay of a day is a lot of memory, and nothing in it
  belongs next to live streams.
- `[backtest.fees]` in `config.toml` for every venue, as fractions. A venue
  without an entry trades free, and the broker says so at startup.
- A view on `backtest.participation`, the share of a recorded trade a
  resting order of ours may take once the queue ahead of it is gone. The
  default, 1, takes the whole print: the recording cannot contradict it,
  since it never saw our order, but reality would have put us in a queue.
  `--participation` overrides it per run. Section 5 says what the one
  calibration so far found.
- A round trip for any venue that has no measured placement. Check with
  `uv run -m apps.maker.src.sim_broker <run_id>` alone: it refuses to start
  and names the venue. Then decide the number deliberately and pass
  `--assume-rtt-ms VENUE=MS`; it ends up in the report labelled `assumed`.

## 1. Pick a run id and a range

The run id is one word without colons; it must be new. Republishing into a
used prefix is refused by Redis because the entries keep their original ids,
and the broker's report would be nonsense over two runs' worth of events.

Times are ISO 8601, UTC unless a zone is given, start inclusive, end
exclusive. The replayer reads one hourly bucket either side of the range and
publishes, first, the last book and balance before the start, so a strategy
started mid-day has a balance to fund its quotes.

Either give the opening balances or start the range just after a full
balance snapshot. `--balance VENUE:ASSET=AMOUNT`, repeatable, or
`[backtest.balances]` in the config, opens a named venue with what you say
and ignores the recording's snapshot, which makes a range startable
anywhere and makes two runs over different windows comparable. Pair it with
`--balances none` on the replayer so nothing recorded reaches the strategy
first. It does make the run a hypothetical rather than a reconstruction: a
comparison against what the live account actually did wants the recorded
figures, holds excluded, which is what a run without the flag now does.

If you do open from the recording, start just after a full balance
snapshot, or the strategy quotes one side all run. A venue that publishes its balance as a delta records only
the currencies that changed, and priming replays the last record before the
start whatever it holds: pick a round hour and the strategy is liable to be
handed base with no quote currency, judge every buy insolvent, and sell for
eight hours. The full snapshots are the `seq` 1 records, one per reconnect:

```bash
grep -h '"seq":1,' <root>/acct/balance/*/*.jsonl | head
```

The first run of 2026-09-08 12:00 did exactly this and quoted 1153 sells
against 0 buys; from 12:04:45.200, a fifth of a second after that hour's
snapshot, it quoted both sides.

## 2. Start the consumers first

Each consumer writes its progress under the prefix; the replayer waits for
all of them before publishing anything, so the order does not matter, but
starting them first avoids a wait. `<index>` is the strategy's position
among those matching the production flag, as for the orchestrator.

```bash
uv run -m apps.maker.src.launcher <index> --replay <run_id>
uv run -m apps.maker.src.sim_broker <run_id> --follow <strategy identifier> \
    --drain matching --root <recording root> --report <run_id>.json \
    [--assume-rtt-ms VENUE=MS]
uv run -m apps.maker.src.matcher --replay <run_id>
```

`--follow` takes the identifier of every strategy in the run, repeated. It
is what keeps the broker from processing a market event before the strategy
has reacted to it; without it the broker processes whatever has arrived,
which is only right for a paced replay.

`--drain matching` holds the run open until the matcher has reached the end
of the recording. Without it a fill in the last seconds of a window is
hedged into a broker that has already closed, and the run ends holding a
position: one 8 hour window ended 282.88 base units long that way, on three
runs out of five, the difference between them being wall clock timing
alone. Unlike `--follow` it does not gate market events, so the matcher
waiting on fills cannot deadlock the broker producing them.

## 3. Start the replayer

```bash
uv run -m apps.maker.src.replayer <run_id> --root <recording root> \
    --start 2026-09-07T16:00Z --end 2026-09-07T17:00Z \
    --balances prime --follow <strategy identifier> --follow sim
```

- `--balances prime` is what a simulated run wants: the balance in force at
  the start and nothing after it, since the recorded changes are the live
  run's fills. Leave it at the default `all` only to replay what happened.
- `--follow` names every consumer, the strategies and the broker (`sim`,
  or whatever `--name` it was given). The replayer then never publishes more
  than `--lookahead` recorded seconds (default 1) beyond the slowest.
- `--speed 0`, the default, is as fast as the consumers allow. `--speed 1`
  is real time, for watching a strategy against yesterday's data, and then
  neither `--follow` nor a broker is needed.
- `--include-oms` also replays the recorded order streams. Never with a
  simulated broker: it would see orders it never placed.

## 4. Let it end

The replayer sets the done key when the recording is exhausted. The broker
finishes once every followed strategy is past that time and no intent has
arrived for `backtest.idle_s`, cancels what rests as at shutdown, writes the
report and sets the closed key. The strategies and the matcher stop on the
closed key. Nothing needs a signal; if something is still running a minute
after the replayer exited, look at its log for which key it is waiting on.

## 5. Read the report

The JSON has the counts (intents, placements, rejections, cancellations,
fills, market orders that outran the recorded depth), volume and fees per
venue, the opening and closing totals per asset and venue, `net` (closing
minus opening per asset, summed over venues) and the latency model per
venue with its `source`. Three rules for reading it:

- **Read `net` first.** A hedged strategy ends a fee's worth from flat.
  Anything larger is a position the run acquired and never closed, and a
  profit and loss figure computed from the closing balances of such a run
  is that position marked at a price the reader chose, not a result. The
  broker warns when it is more than a percent of what the run traded.

- A number for a venue whose latency is `assumed` is a number about the
  assumption. Say so wherever it is quoted.
- Every fill is an upper bound. The recording did not react to the
  simulated order: a trade that fills it in the simulation filled someone
  else in reality.
- How large that bound is, from the calibration on the eight hours of
  2026-09-08 12:04-20:00 against the venues' own trade history, where live
  made -0.024 on 1286.25 base units: +0.039 as it stands, +0.061 without
  the print cap, +0.009 at `--participation 0.5` and +0.026 at 0.25. A
  share below 1 buys an error in one place with an error in another, so
  treat it as a sensitivity rather than a calibration. Every configuration
  shows a profit where live took a loss, and every one misses the two live
  fills that hurt, including the quote that crossed the book as a taker.
  The one out-of-sample window, 2026-09-08 04:00-06:08, filled nothing live
  and nothing simulated at any share: no false positives, and no way to
  tell the shares apart. Section 9 of the design has the
  detail, including a quote-cadence divergence worth knowing about before
  reading any fill count.
- Check what a range actually holds. Hourly buckets exist for periods in
  which the bot ran for under a minute.

The recorder can record a backtest too: point it at the prefix's streams and
the run is on disk in the same format as live.

## 6. Cleaning up

```bash
redis-cli --scan --pattern 'bt:<run_id>:*' | xargs redis-cli del
```

Streams under a backtest prefix are never trimmed; a run is bounded by its
range, and a consumer that fell behind must still find every entry. Delete a
run once its report is kept.
