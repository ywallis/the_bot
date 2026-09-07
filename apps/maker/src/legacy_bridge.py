"""Compatibility shim: legacy pubsub messages become order intents.

Legacy strategies publish ``OrderMessage``, ``OrderBatchMessage`` and
``CancellationMessage`` dicts on the ``messageprocessor`` pubsub channel.
Phase 3 moved the order manager to a consumer group on ``oms:intents``, so
this module subscribes to that channel and re-publishes each message as a
typed intent.

The order manager therefore has exactly one input path. Everything about the
legacy wire format is confined here. Since phase 4 the only legacy strategy
is ``take_take``; the whole module is deleted once it is ported or retired.
See ``docs/design/event-driven-framework.md`` sections 6.1 and 11.

Two pieces of legacy vocabulary have no field on ``OrderIntent`` and are
carried as tags instead:

- ``replace`` versus ``unique`` versus ``market``. Only ``replace`` orders
  rest and get superseded by the next order of the same strategy, and the
  legacy sender does not know the intent id it supersedes, so it cannot fill
  ``replace_of``. The tag lets the order manager apply the same
  cancel-then-place behaviour it applies to an explicit ``replace_of``.
- A cancellation with an empty ``id``, which strategies send to mean "cancel
  whatever I have resting" rather than naming an order. It becomes a
  ``CancelIntent`` with an empty ``target_intent_id``, which the order
  manager resolves against its own record of the strategy's resting order.
"""

import asyncio
import logging
from decimal import Decimal

from redis.asyncio import Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OrderType
from apps.maker.src.structs import (
    CancellationMessage,
    OrderBatchMessage,
    OrderMessage,
)
from apps.maker.src.utils import parse_message
from apps.shared.src.events import (
    CancelIntent,
    OrderIntent,
    OrderKind,
    Side,
    now_ns,
)
from apps.shared.src.streams import StreamPublisher

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# Tag carrying the legacy order type of an intent that came through the shim.
LEGACY_ORDER_TYPE_TAG = "legacy_order_type"
# Tag marking an intent as one the shim produced, so the recording shows
# which orders were still going through the compatibility path.
LEGACY_SOURCE_TAG = "source"
LEGACY_SOURCE = "legacy_bridge"


def order_kind_of(order_type: OrderType) -> OrderKind:
    """
    Map a legacy order type to the venue order type.

    Parameters
    ----------
    order_type : OrderType
        Legacy order type.

    Returns
    -------
    OrderKind
        ``MARKET`` for a legacy market order, ``LIMIT`` otherwise. The
        difference between ``replace`` and ``unique`` is about whether the
        order rests, not about what is sent to the venue, so both are limit
        orders here and are told apart by ``LEGACY_ORDER_TYPE_TAG``.
    """
    if order_type == OrderType.MARKET:
        return OrderKind.MARKET
    return OrderKind.LIMIT


def intent_from_order_message(msg: OrderMessage, ts_recv: int) -> OrderIntent:
    """
    Convert a legacy order message into an ``OrderIntent``.

    Parameters
    ----------
    msg : OrderMessage
        The legacy message.
    ts_recv : int
        Time the shim received the message, nanoseconds. This stands in for
        the strategy's creation time, so latency measured from it excludes
        the pubsub hop the shim adds.

    Returns
    -------
    OrderIntent
        The intent.
    """
    order_type = msg["order_type"]
    return OrderIntent(
        ts_recv=ts_recv,
        intent_id=msg["id"],
        strategy=msg["strategy"],
        venue=msg["exchange"],
        symbol=msg["pair"],
        side=Side(msg["side"].value),
        order_type=order_kind_of(order_type),
        amount=Decimal(msg["amount"]),
        price=Decimal(msg["price"]),
        tags={
            LEGACY_ORDER_TYPE_TAG: order_type.value,
            LEGACY_SOURCE_TAG: LEGACY_SOURCE,
        },
    )


def cancel_intent_from_message(msg: CancellationMessage, ts_recv: int) -> CancelIntent:
    """
    Convert a legacy cancellation into a ``CancelIntent``.

    Parameters
    ----------
    msg : CancellationMessage
        The legacy message. Its ``id`` is empty when the strategy means
        "cancel whatever I have resting".
    ts_recv : int
        Time the shim received the message, nanoseconds.

    Returns
    -------
    CancelIntent
        The intent.
    """
    return CancelIntent(
        ts_recv=ts_recv,
        intent_id=f"cancel-{msg['strategy']}-{ts_recv}",
        strategy=msg["strategy"],
        venue=msg["exchange"],
        symbol=msg["pair"],
        target_intent_id=msg["id"],
    )


def intents_from_message(
    msg: OrderMessage | CancellationMessage | OrderBatchMessage,
    ts_recv: int,
) -> list[OrderIntent | CancelIntent]:
    """
    Convert any legacy message into the intents it stands for.

    A batch becomes one intent per order, published in the order the batch
    listed them.

    Parameters
    ----------
    msg : OrderMessage | CancellationMessage | OrderBatchMessage
        The parsed legacy message.
    ts_recv : int
        Time the shim received the message, nanoseconds.

    Returns
    -------
    list[OrderIntent | CancelIntent]
        The intents, empty for a message the shim does not translate.
    """
    match msg.get("kind"):
        case MessageType.ORDER:
            return [intent_from_order_message(msg, ts_recv)]  # type: ignore[arg-type]
        case MessageType.CANCELLATION:
            return [cancel_intent_from_message(msg, ts_recv)]  # type: ignore[arg-type]
        case MessageType.ORDERBATCH:
            batch: OrderBatchMessage = msg  # type: ignore[assignment]
            return [
                intent_from_order_message(order, ts_recv) for order in batch["orders"]
            ]
    logger.error(f"Legacy message of unknown kind, dropping: {msg}")
    return []


async def publish_legacy_message(
    redis: Redis, publisher: StreamPublisher, raw: str | bytes
) -> int:
    """
    Parse one legacy message and publish the intents it stands for.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``oms:intents``.
    raw : str | bytes
        The pubsub payload.

    Returns
    -------
    int
        Number of intents published.
    """
    ts_recv = now_ns()
    try:
        parsed = parse_message(raw.decode() if isinstance(raw, bytes) else raw)
    except (ValueError, KeyError, TypeError) as error:
        logger.error(f"Unparseable legacy message {raw!r}: {error}")
        return 0
    if parsed is None:
        logger.error(f"Unparseable legacy message {raw!r}")
        return 0

    intents = intents_from_message(parsed, ts_recv)
    for intent in intents:
        await publisher.publish(redis, intent)
        logger.debug(f"Bridged {intent.intent_id} to oms:intents")
    return len(intents)


async def run(
    redis: Redis,
    publisher: StreamPublisher,
    channel: str = MESSAGE_PROCESSOR_CHANNEL,
) -> None:
    """
    Subscribe to the legacy channel and bridge every message it carries.

    A message that cannot be parsed or translated is logged and dropped: the
    bridge must never take down the order manager it runs inside.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    publisher : StreamPublisher
        Publisher writing to ``oms:intents``.
    channel : str
        Pubsub channel to subscribe to.
    """
    async with redis.pubsub() as pubsub:
        await pubsub.subscribe(channel)
        logger.info(f"Legacy bridge listening on {channel}")
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                await publish_legacy_message(redis, publisher, message["data"])
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001, the bridge must survive
                logger.exception(
                    f"Legacy bridge failed on {message['data']!r}: {error}"
                )
