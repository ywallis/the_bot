"""Tests for the notifier."""

import time
from decimal import Decimal

import pytest

from apps.maker.src.notifier import Notifier, read_streams, watched_streams
from apps.shared.src.config import NotifierConfig, parse_app_config
from apps.shared.src.events import (
    AlertEvent,
    AlertLevel,
    DeltaEvent,
    FundingEvent,
    OrderEvent,
    OrderState,
    Side,
)

PERP = "BASE/USDT:USDT"


class Outbox:
    """A delivery channel that keeps what it is handed."""

    def __init__(self):
        """Start empty."""
        self.messages: list[str] = []

    async def __call__(self, text: str) -> None:
        """Keep the message."""
        self.messages.append(text)


def notifier(**overrides) -> tuple[Notifier, Outbox]:
    """Return a notifier with a recording outbox."""
    outbox = Outbox()
    return Notifier(NotifierConfig(**overrides), outbox), outbox


def funding(rate: float) -> FundingEvent:
    """Return a funding event at a rate."""
    return FundingEvent(
        ts_recv=1, venue="venueperp", symbol=PERP, seq=1, funding_rate=rate
    )


@pytest.mark.asyncio
async def test_a_strategy_alert_is_delivered_once_per_cooldown():
    """The same key twice within the cooldown is one message."""
    note, outbox = notifier()
    alert = AlertEvent(
        ts_recv=1, source="rec", key="delta", level=AlertLevel.WARNING, message="m"
    )
    assert await note.handle("alerts", alert) is not None
    assert await note.handle("alerts", alert) is None
    assert outbox.messages == ["[warning] rec: m"]


@pytest.mark.asyncio
async def test_funding_alerts_on_crossing_the_floor_and_on_recovery():
    """Negative funding alerts once; recovering alerts once more."""
    note, outbox = notifier()
    assert await note.handle("f", funding(0.0001)) is None
    assert await note.handle("f", funding(-0.0002)) is not None
    assert await note.handle("f", funding(-0.0003)) is None
    recovered = await note.handle("f", funding(0.0001))
    assert recovered is not None and recovered.level is AlertLevel.INFO
    assert len(outbox.messages) == 2


@pytest.mark.asyncio
async def test_a_rejected_hedge_is_critical():
    """A hedge the venue refused leaves a fill open and says so."""
    note, _ = notifier()
    base = dict(
        ts_recv=1,
        intent_id="t-1_inv_q",
        venue="venueperp",
        symbol=PERP,
        side=Side.SELL,
    )
    rejected = OrderEvent(
        strategy="matching", state=OrderState.REJECTED, reason="margin", **base
    )
    alert = await note.handle("oms:events", rejected)
    assert alert is not None and alert.level is AlertLevel.CRITICAL
    quote = OrderEvent(strategy="inv_q", state=OrderState.REJECTED, **base)
    assert await note.handle("oms:events", quote) is None


@pytest.mark.asyncio
async def test_an_unhealthy_delta_alerts():
    """A reconciler reporting unhealthy reaches the operator."""
    note, _ = notifier()
    delta = DeltaEvent(
        ts_recv=1,
        asset="BASE",
        spot_base={},
        perp_base=Decimal(0),
        delta=Decimal(0),
        in_flight_base=Decimal(0),
        deadband=Decimal(1),
        healthy=False,
        reason="position stale",
    )
    alert = await note.handle("acct:delta:BASE", delta)
    assert alert is not None and "position stale" in alert.message


@pytest.mark.asyncio
async def test_a_silent_stream_is_stale_until_it_delivers_again():
    """A watched stream alerts once when silent and once when it is back."""
    note, outbox = notifier(stale_after_s=10)
    note.watch(["acct:position:venueperp"])
    note.watched["acct:position:venueperp"] = time.monotonic() - 20
    assert len(await note.check_stale()) == 1
    assert await note.check_stale() == []
    back = await note.handle("acct:position:venueperp", funding(0.0001))
    assert back is not None and back.level is AlertLevel.INFO
    assert len(outbox.messages) == 2


@pytest.mark.asyncio
async def test_a_failing_channel_does_not_stop_the_notifier():
    """An alert that cannot be sent is logged and counted as handled."""

    async def broken(text: str) -> None:
        raise RuntimeError("down")

    note = Notifier(NotifierConfig(), broken)
    alert = AlertEvent(
        ts_recv=1, source="rec", key="k", level=AlertLevel.INFO, message="m"
    )
    assert await note.handle("alerts", alert) is not None


def test_the_streams_read_follow_the_subscriptions():
    """Positions and funding are held to a deadline; deltas are read too."""
    config = parse_app_config(
        {
            "venues": [
                {"id": "venuea", "name": "A"},
                {"id": "venueperp", "name": "P", "market_type": "swap"},
            ],
            "strategies": [
                {
                    "identifier": "inv",
                    "type": "inventory",
                    "production": True,
                    "subscriptions": [
                        {"venue": "venuea", "symbol": "BASE/USDT", "feeds": ["delta"]},
                        {
                            "venue": "venueperp",
                            "symbol": PERP,
                            "feeds": ["position", "funding"],
                        },
                    ],
                }
            ],
        }
    )
    assert watched_streams(config, True) == [
        "acct:position:venueperp",
        f"md:funding:venueperp:{PERP}",
    ]
    assert read_streams(config, True) == [
        "alerts",
        "oms:events",
        "acct:delta:BASE",
        "acct:position:venueperp",
        f"md:funding:venueperp:{PERP}",
    ]
