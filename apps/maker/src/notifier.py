"""Deliver alerts to the operator.

The only process that talks to anything outside the trading host and Redis.
It reads ``alerts``, where strategies publish ``AlertEvent``s, and derives
alerts of its own from the streams it watches:

- funding under ``[notifier] min_funding_bps`` per interval, and again when
  it recovers;
- a hedge the order manager rejected, which leaves a fill unhedged;
- a delta its reconciler reports unhealthy;
- a position or funding stream silent for ``stale_after_s``.

Every alert is delivered at most once per key per ``cooldown_s``. Delivery
is a Telegram bot when ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID`` are
set in the environment, and the log otherwise, so a host without the bot
still records every alert. See ``docs/design/inventory-hedging.md``
section 7.5.
"""

import asyncio
import logging
import os
import time
from typing import Any, cast

import aiohttp
from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.config import (
    DELTA_FEED,
    FUNDING_FEED,
    POSITION_FEED,
    AppConfig,
    NotifierConfig,
    load_app_config,
)
from apps.shared.src.events import (
    ALERTS_STREAM,
    ORDER_EVENTS_STREAM,
    AlertEvent,
    AlertLevel,
    AnyEvent,
    DeltaEvent,
    FundingEvent,
    OrderEvent,
    OrderState,
    from_stream_fields,
    now_ns,
)
from apps.shared.src.streams import entry_id_str, feed_stream, stream_tail
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"

# Strategy key of the hedges the matcher places.
MATCHING_STRATEGY = "matching"

# How often stale feeds are checked for, in seconds.
STALE_CHECK_S = 30.0


class Notifier:
    """
    Turn events into alerts and alerts into messages, with a cooldown.

    Attributes
    ----------
    config : NotifierConfig
        Thresholds and cooldown.
    watched : dict[str, float]
        Monotonic time each watched position or funding stream last
        delivered, keyed by stream name.
    sent : list[AlertEvent]
        Every alert delivered, oldest first; tests read it.
    """

    def __init__(self, config: NotifierConfig, send: Any) -> None:
        """
        Initialize the notifier.

        Parameters
        ----------
        config : NotifierConfig
            Thresholds and cooldown.
        send : Any
            Coroutine function taking the message text, the delivery
            channel.
        """
        self.config = config
        self.send = send
        self.watched: dict[str, float] = {}
        self.sent: list[AlertEvent] = []
        self._last_sent: dict[str, float] = {}
        self._funding_low: dict[str, bool] = {}
        self._stale: set[str] = set()

    def watch(self, streams: list[str]) -> None:
        """
        Start the staleness clock of the streams that must keep delivering.

        Parameters
        ----------
        streams : list[str]
            Position and funding stream names.
        """
        now = time.monotonic()
        for stream in streams:
            self.watched.setdefault(stream, now)

    async def deliver(self, alert: AlertEvent) -> bool:
        """
        Send an alert unless one with its key went out within the cooldown.

        Parameters
        ----------
        alert : AlertEvent
            The alert.

        Returns
        -------
        bool
            Whether it was sent.
        """
        now = time.monotonic()
        last = self._last_sent.get(alert.key)
        if last is not None and now - last < self.config.cooldown_s:
            logger.debug(f"Alert {alert.key} within its cooldown, dropped")
            return False
        self._last_sent[alert.key] = now
        text = f"[{alert.level.value}] {alert.source}: {alert.message}"
        logger.warning(f"Alert {alert.key}: {text}")
        try:
            await self.send(text)
        except Exception as error:  # noqa: BLE001, the log still has it
            logger.error(f"Could not deliver alert {alert.key}: {error!r}")
        self.sent.append(alert)
        return True

    def _alert(self, key: str, level: AlertLevel, message: str) -> AlertEvent:
        return AlertEvent(
            ts_recv=now_ns(), source="notifier", key=key, level=level, message=message
        )

    async def handle(self, stream: str, event: AnyEvent) -> AlertEvent | None:
        """
        Derive and deliver the alert one event calls for, if any.

        Parameters
        ----------
        stream : str
            The stream the event was read from.
        event : AnyEvent
            The event.

        Returns
        -------
        AlertEvent | None
            The alert delivered, None when the event called for none or the
            cooldown dropped it.
        """
        if stream in self.watched:
            self.watched[stream] = time.monotonic()
            if stream in self._stale:
                self._stale.discard(stream)
                return await self._send_if(
                    self._alert(
                        f"stale:{stream}", AlertLevel.INFO, f"{stream} delivers again"
                    ),
                    bypass=True,
                )
        alert: AlertEvent | None = None
        match event:
            case AlertEvent():
                alert = event
            case FundingEvent():
                alert = self.funding_alert(event)
            case OrderEvent():
                alert = self.hedge_alert(event)
            case DeltaEvent():
                if not event.healthy:
                    alert = self._alert(
                        f"delta:{event.asset}",
                        AlertLevel.WARNING,
                        f"{event.asset} delta unhealthy: {event.reason}",
                    )
        if alert is None:
            return None
        return await self._send_if(alert)

    async def _send_if(
        self, alert: AlertEvent, bypass: bool = False
    ) -> AlertEvent | None:
        if bypass:
            self._last_sent.pop(alert.key, None)
        return alert if await self.deliver(alert) else None

    def funding_alert(self, event: FundingEvent) -> AlertEvent | None:
        """
        Alert when funding crosses the floor, either way.

        Parameters
        ----------
        event : FundingEvent
            The event.

        Returns
        -------
        AlertEvent | None
            An alert on the crossing, None while the side is unchanged.
        """
        key = f"{event.venue}:{event.symbol}"
        bps = event.funding_rate * 10_000
        low = bps < self.config.min_funding_bps
        was_low = self._funding_low.get(key, False)
        self._funding_low[key] = low
        if low and not was_low:
            return self._alert(
                f"funding:{key}",
                AlertLevel.WARNING,
                f"funding on {key} is {bps:.2f} bps per interval, under "
                f"{self.config.min_funding_bps}: the short pays",
            )
        if was_low and not low:
            self._last_sent.pop(f"funding:{key}", None)
            return self._alert(
                f"funding:{key}",
                AlertLevel.INFO,
                f"funding on {key} recovered to {bps:.2f} bps per interval",
            )
        return None

    def hedge_alert(self, event: OrderEvent) -> AlertEvent | None:
        """
        Alert when a hedge is rejected.

        Parameters
        ----------
        event : OrderEvent
            The event.

        Returns
        -------
        AlertEvent | None
            A critical alert for a rejected hedge, None otherwise.
        """
        if (
            event.strategy != MATCHING_STRATEGY
            or event.state is not OrderState.REJECTED
        ):
            return None
        return self._alert(
            f"hedge:{event.venue}:{event.symbol}",
            AlertLevel.CRITICAL,
            f"hedge {event.intent_id} on {event.venue} {event.symbol} rejected: "
            f"{event.reason}; the fill it covers is unhedged",
        )

    async def check_stale(self) -> list[AlertEvent]:
        """
        Alert on every watched stream silent for longer than allowed.

        Returns
        -------
        list[AlertEvent]
            The alerts delivered.
        """
        now = time.monotonic()
        delivered: list[AlertEvent] = []
        for stream, last in self.watched.items():
            if now - last < self.config.stale_after_s or stream in self._stale:
                continue
            self._stale.add(stream)
            alert = self._alert(
                f"stale:{stream}",
                AlertLevel.WARNING,
                f"nothing on {stream} for {self.config.stale_after_s:.0f}s",
            )
            if await self.deliver(alert):
                delivered.append(alert)
        return delivered


def watched_streams(config: AppConfig, production_mode: bool | None) -> list[str]:
    """
    Return the position and funding streams the notifier holds to a deadline.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    production_mode : bool | None
        Which strategies' subscriptions to serve.

    Returns
    -------
    list[str]
        Stream names, sorted.
    """
    streams: set[str] = set()
    for feed in (POSITION_FEED, FUNDING_FEED):
        for venue, symbol in config.feed_pairs(feed, production_mode):
            streams.add(feed_stream(feed, venue, symbol))
    return sorted(streams)


def read_streams(config: AppConfig, production_mode: bool | None) -> list[str]:
    """
    Return every stream the notifier reads.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    production_mode : bool | None
        Which strategies' subscriptions to serve.

    Returns
    -------
    list[str]
        The alerts, the order events, the watched streams and the deltas.
    """
    deltas = {
        feed_stream(DELTA_FEED, venue, symbol)
        for venue, symbol in config.feed_pairs(DELTA_FEED, production_mode)
    }
    return [ALERTS_STREAM, ORDER_EVENTS_STREAM] + sorted(
        set(watched_streams(config, production_mode)) | deltas
    )


def telegram_sender(session: aiohttp.ClientSession, token: str, chat_id: str) -> Any:
    """
    Return a coroutine function sending a message through a Telegram bot.

    Parameters
    ----------
    session : aiohttp.ClientSession
        The HTTP session.
    token : str
        Bot token.
    chat_id : str
        Chat to post to.

    Returns
    -------
    Any
        ``async def send(text) -> None``, raising on a refused message.
    """

    async def send(text: str) -> None:
        async with session.post(
            TELEGRAM_URL.format(token=token),
            json={"chat_id": chat_id, "text": text},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"Telegram answered {response.status}")

    return send


async def log_only(text: str) -> None:
    """
    Deliver nowhere: the notifier has logged the alert already.

    Parameters
    ----------
    text : str
        The message.
    """


async def run(
    redis: Any, notifier: Notifier, cursors: dict[str, str], block_ms: int
) -> None:
    """
    Read the streams and check staleness until cancelled.

    Parameters
    ----------
    redis : Any
        The Redis client.
    notifier : Notifier
        The notifier.
    cursors : dict[str, str]
        Entry id to read after per stream.
    block_ms : int
        How long a read blocks when nothing arrives.
    """
    positions: dict[Any, Any] = dict(cursors)
    next_check = time.monotonic() + STALE_CHECK_S
    while True:
        response = await redis.xread(positions, count=100, block=block_ms)
        for stream, entries in cast(list[Any], response or []):
            name = entry_id_str(stream)
            for entry_id, fields in entries:
                positions[name] = entry_id_str(entry_id)
                try:
                    event = from_stream_fields(fields)
                except Exception as error:  # noqa: BLE001, keep reading
                    logger.error(f"Undecodable entry {entry_id!r} on {name}: {error}")
                    continue
                await notifier.handle(name, event)
        if time.monotonic() >= next_check:
            await notifier.check_stale()
            next_check = time.monotonic() + STALE_CHECK_S


async def main(config: AppConfig) -> None:
    """
    Run the notifier until cancelled.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    """
    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=5
    )
    redis = Redis(connection_pool=pool)
    streams = read_streams(config, production)
    cursors = {stream: await stream_tail(redis, stream) for stream in streams}
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    async with aiohttp.ClientSession() as session:
        if token and chat_id:
            send = telegram_sender(session, token, chat_id)
            logger.info("Alerts go to Telegram")
        else:
            send = log_only
            logger.warning(
                "No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID; alerts are logged only"
            )
        notifier = Notifier(config.notifier, send)
        notifier.watch(watched_streams(config, production))
        try:
            await run(redis, notifier, cursors, config.oms.block_ms)
        finally:
            await redis.aclose()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    asyncio.run(main(load_app_config()))
