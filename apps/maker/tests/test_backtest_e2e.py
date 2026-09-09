"""The whole backtest pipeline in one process, on a fake Redis.

Replayer, a strategy on the runtime, the simulated broker and the matcher run
concurrently against one prefix, coordinated only through the bus and the
replay keys, exactly as four processes would be. The recording is synthetic:
one venue's book ticks along, a trade sweeps through the quote the strategy
rests, the other venue's book is there to absorb the hedge.
"""

import asyncio
from decimal import Decimal
from pathlib import Path

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import replayer as rp
from apps.maker.src import sim_broker as sb
from apps.maker.src.latency import LatencyModel
from apps.maker.src.matcher import consume_order_events, matching_venues
from apps.maker.src.message_processor import TERMINAL_STATES
from apps.maker.src.recorder import format_line
from apps.shared.src import events
from apps.shared.src.config import (
    AppConfig,
    BacktestConfig,
    FeeConfig,
    MarketDataConfig,
    OmsConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
    VenueConfig,
)
from apps.shared.src.events import (
    LATENCY_STREAM,
    ORDER_EVENTS_STREAM,
    REPLACE_RESTING,
    AssetBalance,
    BalanceEvent,
    BookEvent,
    OrderEvent,
    OrderIntent,
    OrderKind,
    OrderState,
    Side,
    TradeEvent,
    from_stream_fields,
    prefixed,
)
from apps.shared.src.runtime import Clock, Runtime, Strategy
from apps.shared.src.streams import StreamPublisher, replay_closed_key, stream_path

PREFIX = "bt:e2e"
A, B = "venue_a", "venue_b"
SYMBOL = "BASE/QUOTE"
T0 = 1_788_703_200_000_000_000
S = 1_000_000_000
MS = 1_000_000


def config() -> AppConfig:
    """Return the two venues and the probe strategy that hedges on B."""
    return AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(VenueConfig(id=A, name="A"), VenueConfig(id=B, name="B")),
        strategies=(
            StrategyConfig(
                identifier="probe",
                type="probe",
                production=True,
                subscriptions=(
                    Subscription(venue=A, symbol=SYMBOL, feeds=("book", "trade")),
                    Subscription(venue=B, symbol=SYMBOL, feeds=("book",)),
                ),
                params={"should_match": True, "taker_exchange": B},
            ),
        ),
        oms=OmsConfig(block_ms=10, batch=100),
        backtest=BacktestConfig(
            fees={A: FeeConfig(maker=0.0, taker=0.001), B: FeeConfig(taker=0.001)},
            idle_s=0.2,
        ),
    )


class Probe(Strategy):
    """Rest a sell one tick above the best ask on A, requote once it is gone."""

    def __init__(self) -> None:
        """Start with nothing resting."""
        self.runtime: Runtime | None = None
        self.resting: OrderIntent | None = None
        self.terminal: list[OrderEvent] = []

    async def on_start(self, runtime: Runtime) -> None:
        """Keep the runtime."""
        self.runtime = runtime

    async def on_book(self, event: BookEvent) -> None:
        """Quote when the slot is free."""
        assert self.runtime is not None
        if event.venue != A or self.resting is not None or not event.asks:
            return
        price = Decimal(str(event.asks[0][0])) + Decimal("0.01")
        intent = self.runtime.order_intent(
            venue=A,
            symbol=SYMBOL,
            side=Side.SELL,
            amount=10,
            price=price,
            order_identifier="es",
            replace_of=REPLACE_RESTING,
        )
        await self.runtime.submit(intent)
        self.resting = intent

    async def on_order_event(self, event: OrderEvent) -> None:
        """Free the slot on a terminal event."""
        if event.state in TERMINAL_STATES:
            self.terminal.append(event)
            if self.resting is not None and event.intent_id == self.resting.intent_id:
                self.resting = None


def write(
    root: Path, stream: str, bucket: str, recorded: list[events.AnyEvent]
) -> None:
    """Write events as a recorder file, ids derived from receive time."""
    directory = stream_path(root, stream)
    directory.mkdir(parents=True, exist_ok=True)
    lines = [
        format_line(
            f"{e.ts_recv // MS}-0", events.event_type(e).value, events.encode(e)
        )
        for e in recorded
    ]
    (directory / f"{bucket}.jsonl").write_bytes(b"".join(lines))


def book(venue: str, ts: int, seq: int, ask: float) -> BookEvent:
    """Return a two-level book around an ask."""
    return BookEvent(
        ts_recv=ts,
        venue=venue,
        symbol=SYMBOL,
        seq=seq,
        ts_exch=None,
        bids=[[round(ask - 0.01, 2), 500.0], [round(ask - 0.02, 2), 500.0]],
        asks=[[ask, 500.0], [round(ask + 0.01, 2), 500.0]],
    )


def balance(venue: str, ts: int) -> BalanceEvent:
    """Return a well funded balance."""
    return BalanceEvent(
        ts_recv=ts,
        venue=venue,
        seq=1,
        ts_exch=None,
        balances={
            "BASE": AssetBalance(free=1000.0, used=0.0, total=1000.0),
            "QUOTE": AssetBalance(free=1000.0, used=0.0, total=1000.0),
        },
    )


def recording(root: Path) -> None:
    """Five seconds of books on both venues and one sweep through the quote on A."""
    books_a = [book(A, T0 + i * 100 * MS, i + 1, 0.35) for i in range(50)]
    books_b = [book(B, T0 + i * 100 * MS + 30 * MS, i + 1, 0.34) for i in range(50)]
    # The strategy quotes 0.36 off the first book; the order rests from about
    # T0+0.3s. A print at 0.40 two seconds in takes it, and carries the whole
    # quote, since a fill is capped by what printed. Nothing afterwards fills
    # the requote, so the wind-down cancels it.
    trades_a = [
        TradeEvent(
            ts_recv=T0 + 2 * S + 50 * MS,
            venue=A,
            symbol=SYMBOL,
            seq=1,
            ts_exch=None,
            trade_id="t1",
            side=Side.BUY,
            price=0.40,
            amount=3.0,
        )
    ]
    write(root, f"md:book:{A}:{SYMBOL}", "2026-09-06T14", books_a)
    write(root, f"md:book:{B}:{SYMBOL}", "2026-09-06T14", books_b)
    write(root, f"md:trade:{A}:{SYMBOL}", "2026-09-06T14", trades_a)
    write(root, f"acct:balance:{A}", "2026-09-06T09", [balance(A, T0 - 5 * 3600 * S)])
    write(root, f"acct:balance:{B}", "2026-09-06T09", [balance(B, T0 - 5 * 3600 * S)])


@pytest.mark.asyncio
async def test_a_quote_is_filled_hedged_and_the_run_winds_down(tmp_path: Path):
    """Replayer, strategy, simulated broker and matcher complete one round trip of the design."""
    recording(tmp_path)
    cfg = config()
    redis = fakeredis.FakeRedis()
    latency = LatencyModel()
    latency.assume(A, 300)
    latency.assume(B, 400)
    probe = Probe()
    strategy_cfg = cfg.strategies[0]

    replayer = rp.Replayer(
        tmp_path,
        rp.replay_streams(cfg, True, include_oms=False),
        PREFIX,
        start_ns=T0,
        follow=["probe", sb.PROCESS_NAME],
        lookahead_s=0.5,
        balances=rp.BALANCES_PRIME,
    )
    broker = sb.SimulatedBroker(
        redis, cfg, PREFIX, latency, follow=["probe"], block_ms=10
    )
    runtime = Runtime(
        redis,
        cfg,
        strategy_cfg,
        probe,
        clock=Clock.replay(),
        prefix=PREFIX,
        block_ms=10,
    )
    matcher = consume_order_events(
        redis,
        StreamPublisher(maxlen=10_000, prefix=PREFIX),
        matching_venues(cfg),
        10,
        100,
        prefix=PREFIX,
        clock=Clock.replay(),
    )

    results = await asyncio.wait_for(
        asyncio.gather(replayer.run(redis), broker.run(), runtime.run(), matcher),
        timeout=30,
    )
    replay_report, sim_report = results[0], results[1]

    # The market data went through, primed balances first.
    assert replay_report.published[f"md:book:{A}:{SYMBOL}"] == 50
    assert sorted(replay_report.primed) == [f"acct:balance:{A}", f"acct:balance:{B}"]
    assert set(broker.balances.adopted) == {A, B}

    # The strategy quoted, the sweep filled the quote, the matcher hedged it on B.
    order_events = [
        from_stream_fields(f)
        for _, f in await redis.xrange(prefixed(PREFIX, ORDER_EVENTS_STREAM))
    ]
    fills = [
        e
        for e in order_events
        if isinstance(e, OrderEvent) and e.state is OrderState.FILLED
    ]
    assert [e.venue for e in fills] == [A, B]
    maker_fill, hedge_fill = fills
    assert maker_fill.side is Side.SELL and maker_fill.avg_price == Decimal("0.36")
    assert maker_fill.filled == 10
    assert hedge_fill.side is Side.BUY and hedge_fill.strategy == "matching"
    assert hedge_fill.intent_id == maker_fill.intent_id  # the hedge reuses the id
    assert hedge_fill.tags["hedge_of"] == maker_fill.intent_id
    # The hedge was created when the matcher learned of the fill, in event time,
    # and reached B after the assumed latency, against B's book of that moment.
    assert hedge_fill.ts_recv > maker_fill.ts_recv
    assert hedge_fill.avg_price == Decimal("0.34")

    # The requote after the fill rested until the wind-down cancelled it.
    cancelled = [
        e
        for e in order_events
        if isinstance(e, OrderEvent) and e.state is OrderState.CANCELLED
    ]
    assert cancelled and cancelled[-1].reason == "shutting down"
    # The strategy was still there to see its fill and to requote.
    assert [e.state for e in probe.terminal][:1] == [OrderState.FILLED]
    assert probe.resting is None or probe.resting.intent_id == cancelled[-1].intent_id

    # Every placement left a latency record; the report adds up.
    records = await redis.xrange(prefixed(PREFIX, LATENCY_STREAM))
    assert len(records) == sim_report.placed
    assert sim_report.placed == 3  # the quote, the hedge, the requote
    assert sim_report.fills == 2
    assert sim_report.cancelled == 1
    assert sim_report.volume == {A: Decimal(10), B: Decimal(10)}
    assert Decimal(sim_report.closing[A]["BASE"]) == Decimal("990.0")
    assert Decimal(sim_report.closing[A]["QUOTE"]) == Decimal("1003.6")
    assert Decimal(sim_report.closing[B]["QUOTE"]) == Decimal("1000.0") - Decimal("3.4")
    assert Decimal(sim_report.closing[B]["BASE"]) == Decimal("1000") + Decimal("10") * (
        1 - Decimal("0.001")
    )
    assert sim_report.latency[A]["source"] == "assumed"

    # Everybody stopped on its own, in order.
    assert await redis.get(replay_closed_key(PREFIX)) is not None
    assert not broker.buffer and not broker.timeline

    # Nothing touched the live streams.
    assert await redis.exists(ORDER_EVENTS_STREAM) == 0
    assert await redis.exists(f"md:book:{A}:{SYMBOL}") == 0
