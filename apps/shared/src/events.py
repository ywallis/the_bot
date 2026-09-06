"""Event schemas and stream naming for the Redis Streams bus.

These structs are the cross-language contract. A non-Python component only
needs to produce or consume the JSON shape they encode to. Every stream entry
has two fields: ``type`` (the tag string) and ``data`` (the JSON payload).

See ``docs/design/event-driven-framework.md`` sections 3 and 4.
"""

import time
from decimal import Decimal
from enum import Enum
from typing import Union

import msgspec

SCHEMA_VERSION = 1

# Stream names --------------------------------------------------------------

INTENTS_STREAM = "oms:intents"
ORDER_EVENTS_STREAM = "oms:events"
LATENCY_STREAM = "oms:latency"
OMS_CONSUMER_GROUP = "oms"


def book_stream(venue: str, symbol: str) -> str:
    """
    Return the order book stream name for a venue and symbol.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.

    Returns
    -------
    str
        Stream name.
    """
    return f"md:book:{venue}:{symbol}"


def trade_stream(venue: str, symbol: str) -> str:
    """
    Return the trade stream name for a venue and symbol.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.

    Returns
    -------
    str
        Stream name.
    """
    return f"md:trade:{venue}:{symbol}"


def balance_stream(venue: str) -> str:
    """
    Return the balance stream name for a venue.

    Parameters
    ----------
    venue : str
        CCXT short id.

    Returns
    -------
    str
        Stream name.
    """
    return f"acct:balance:{venue}"


def snapshot_key(venue: str, symbol: str) -> str:
    """
    Return the legacy latest-snapshot key for an order book.

    Kept during migration so polling strategies continue to work.

    Parameters
    ----------
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.

    Returns
    -------
    str
        Redis key.
    """
    return f"{symbol}-{venue}"


def prefixed(prefix: str, stream: str) -> str:
    """
    Prefix a stream name, e.g. for a backtest namespace.

    Parameters
    ----------
    prefix : str
        Namespace prefix without trailing colon, e.g. ``bt:run1``.
    stream : str
        Stream name.

    Returns
    -------
    str
        Prefixed stream name, or the stream unchanged if prefix is empty.
    """
    return f"{prefix}:{stream}" if prefix else stream


def now_ns() -> int:
    """
    Return the current wall clock time in nanoseconds since the epoch.

    Returns
    -------
    int
        Nanoseconds.
    """
    return time.time_ns()


# Enums ---------------------------------------------------------------------


class EventType(str, Enum):
    """Tag values used in the ``type`` field of every event."""

    BOOK = "book"
    TRADE = "trade"
    BALANCE = "balance"
    ORDER_INTENT = "order_intent"
    CANCEL_INTENT = "cancel_intent"
    ORDER_EVENT = "order_event"
    LATENCY = "latency"


class Side(str, Enum):
    """Order or trade side."""

    BUY = "buy"
    SELL = "sell"


class OrderKind(str, Enum):
    """Venue order type."""

    LIMIT = "limit"
    MARKET = "market"


class TimeInForce(str, Enum):
    """Time in force for limit orders."""

    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    POST_ONLY = "post_only"


class OrderState(str, Enum):
    """Lifecycle states of an order as seen by the order manager."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class Liquidity(str, Enum):
    """Whether a fill added or removed liquidity."""

    MAKER = "maker"
    TAKER = "taker"


# Structs -------------------------------------------------------------------

Level = tuple[float, float]
"""A price level as ``(price, size)``."""


class Event(msgspec.Struct, tag_field="type", kw_only=True):
    """
    Common fields of every event.

    Attributes
    ----------
    ts_recv : int
        Local time the event was observed, nanoseconds since the epoch.
        This is the clock strategies reason in.
    v : int
        Schema version.
    """

    ts_recv: int
    v: int = SCHEMA_VERSION


class BookEvent(Event, tag=EventType.BOOK.value):
    """
    Top-N order book snapshot.

    Attributes
    ----------
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    seq : int
        Per-stream producer sequence number.
    ts_exch : int | None
        Exchange timestamp in milliseconds, if provided.
    bids : list[Level]
        Bid levels, best first.
    asks : list[Level]
        Ask levels, best first.
    """

    venue: str
    symbol: str
    seq: int
    ts_exch: int | None
    bids: list[Level]
    asks: list[Level]


class TradeEvent(Event, tag=EventType.TRADE.value):
    """
    A public trade.

    Attributes
    ----------
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    seq : int
        Per-stream producer sequence number.
    ts_exch : int | None
        Exchange timestamp in milliseconds, if provided.
    trade_id : str | None
        Venue trade id, if provided.
    side : Side | None
        Aggressor side, if provided.
    price : float
        Trade price.
    amount : float
        Trade size in base asset.
    """

    venue: str
    symbol: str
    seq: int
    ts_exch: int | None
    trade_id: str | None
    side: Side | None
    price: float
    amount: float


class AssetBalance(msgspec.Struct):
    """
    Balance of a single asset.

    Attributes
    ----------
    free : float
        Available balance.
    used : float
        Balance locked in orders.
    total : float
        Free plus used.
    """

    free: float
    used: float
    total: float


class BalanceEvent(Event, tag=EventType.BALANCE.value):
    """
    Full balance snapshot for a venue.

    Attributes
    ----------
    venue : str
        CCXT short id.
    seq : int
        Per-stream producer sequence number.
    ts_exch : int | None
        Exchange timestamp in milliseconds, if provided.
    balances : dict[str, AssetBalance]
        Balances keyed by asset.
    """

    venue: str
    seq: int
    ts_exch: int | None
    balances: dict[str, AssetBalance]


class OrderIntent(Event, tag=EventType.ORDER_INTENT.value):
    """
    A request from a strategy to place an order.

    Attributes
    ----------
    intent_id : str
        Unique id chosen by the strategy, used as venue client order id.
    strategy : str
        Strategy identifier.
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    side : Side
        Buy or sell.
    order_type : OrderKind
        Limit or market.
    amount : Decimal
        Size in base asset.
    price : Decimal | None
        Limit price, None for market orders.
    time_in_force : TimeInForce
        Time in force, defaults to good till cancelled.
    replace_of : str | None
        Intent id this intent supersedes. The order manager cancels that
        order first and coalesces pending replacements per strategy.
    tags : dict[str, str]
        Free-form labels carried through to order events.
    """

    intent_id: str
    strategy: str
    venue: str
    symbol: str
    side: Side
    order_type: OrderKind
    amount: Decimal
    price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    replace_of: str | None = None
    tags: dict[str, str] = {}


class CancelIntent(Event, tag=EventType.CANCEL_INTENT.value):
    """
    A request from a strategy to cancel an order.

    Attributes
    ----------
    intent_id : str
        Unique id of this cancel request.
    strategy : str
        Strategy identifier.
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    target_intent_id : str
        Intent id of the order to cancel.
    """

    intent_id: str
    strategy: str
    venue: str
    symbol: str
    target_intent_id: str


class Fill(msgspec.Struct):
    """
    A single execution against an order.

    Attributes
    ----------
    price : Decimal
        Fill price.
    amount : Decimal
        Fill size in base asset.
    fee : Decimal | None
        Fee charged, if known.
    fee_currency : str | None
        Fee currency, if known.
    liquidity : Liquidity | None
        Maker or taker, if known.
    venue_trade_id : str | None
        Venue trade id, if known.
    """

    price: Decimal
    amount: Decimal
    fee: Decimal | None = None
    fee_currency: str | None = None
    liquidity: Liquidity | None = None
    venue_trade_id: str | None = None


class OrderEvent(Event, tag=EventType.ORDER_EVENT.value):
    """
    A state transition of an order.

    Attributes
    ----------
    intent_id : str
        Intent id of the order.
    strategy : str
        Strategy identifier.
    venue : str
        CCXT short id.
    symbol : str
        CCXT symbol.
    state : OrderState
        New state.
    side : Side | None
        Buy or sell. Every producer sets it; the type stays optional so a
        decode of an event from a producer that predates the field still
        succeeds rather than failing the whole read.
    ts_exch : int | None
        Exchange timestamp in milliseconds, if provided.
    venue_order_id : str | None
        Venue-assigned order id, once known.
    filled : Decimal
        Cumulative filled size.
    remaining : Decimal
        Remaining size.
    avg_price : Decimal | None
        Average fill price, if any fills.
    last_fill : Fill | None
        The fill that caused this transition, if any.
    reason : str | None
        Rejection or cancellation reason, if any.
    tags : dict[str, str]
        Tags copied from the intent.
    """

    intent_id: str
    strategy: str
    venue: str
    symbol: str
    state: OrderState
    side: Side | None = None
    ts_exch: int | None = None
    venue_order_id: str | None = None
    filled: Decimal = Decimal(0)
    remaining: Decimal = Decimal(0)
    avg_price: Decimal | None = None
    last_fill: Fill | None = None
    reason: str | None = None
    tags: dict[str, str] = {}


class LatencyRecord(Event, tag=EventType.LATENCY.value):
    """
    Timing of one intent through the system.

    All timestamps are nanoseconds since the epoch, local clock, except
    ``ts_venue_ack`` which is whatever the venue reports converted to ns.

    Attributes
    ----------
    intent_id : str
        Intent id.
    venue : str
        CCXT short id.
    ts_created : int
        When the strategy created the intent.
    ts_oms_recv : int | None
        When the order manager read it from the stream.
    ts_broker_send : int | None
        When the broker sent the request to the venue.
    ts_broker_ack : int | None
        When the broker received the venue's response.
    ts_venue_ack : int | None
        Venue-reported acceptance time, if any.
    """

    intent_id: str
    venue: str
    ts_created: int
    ts_oms_recv: int | None = None
    ts_broker_send: int | None = None
    ts_broker_ack: int | None = None
    ts_venue_ack: int | None = None


AnyEvent = Union[
    BookEvent,
    TradeEvent,
    BalanceEvent,
    OrderIntent,
    CancelIntent,
    OrderEvent,
    LatencyRecord,
]

EVENT_TYPES: dict[type, EventType] = {
    BookEvent: EventType.BOOK,
    TradeEvent: EventType.TRADE,
    BalanceEvent: EventType.BALANCE,
    OrderIntent: EventType.ORDER_INTENT,
    CancelIntent: EventType.CANCEL_INTENT,
    OrderEvent: EventType.ORDER_EVENT,
    LatencyRecord: EventType.LATENCY,
}

# Codec ---------------------------------------------------------------------

_encoder = msgspec.json.Encoder()
_decoder = msgspec.json.Decoder(AnyEvent)

TYPE_FIELD = "type"
DATA_FIELD = "data"


def event_type(event: AnyEvent) -> EventType:
    """
    Return the tag of an event.

    Parameters
    ----------
    event : AnyEvent
        The event.

    Returns
    -------
    EventType
        Its tag.
    """
    return EVENT_TYPES[type(event)]


def encode(event: AnyEvent) -> bytes:
    """
    Encode an event as JSON.

    Parameters
    ----------
    event : AnyEvent
        The event.

    Returns
    -------
    bytes
        JSON bytes including the ``type`` tag.
    """
    return _encoder.encode(event)


def decode(data: bytes | str) -> AnyEvent:
    """
    Decode JSON into the matching event struct.

    Parameters
    ----------
    data : bytes | str
        JSON produced by ``encode`` or by another language.

    Returns
    -------
    AnyEvent
        The event, dispatched on its ``type`` tag.

    Raises
    ------
    msgspec.ValidationError
        If the payload does not match any schema.
    """
    return _decoder.decode(data)


def to_stream_fields(event: AnyEvent) -> dict[str, bytes | str]:
    """
    Build the field map for ``XADD``.

    Parameters
    ----------
    event : AnyEvent
        The event.

    Returns
    -------
    dict[str, bytes | str]
        ``{"type": tag, "data": json}``.
    """
    return {TYPE_FIELD: event_type(event).value, DATA_FIELD: encode(event)}


def from_stream_fields(fields: dict[bytes | str, bytes | str]) -> AnyEvent:
    """
    Decode the field map of a stream entry.

    Works whether the Redis client returns bytes or decoded strings.

    Parameters
    ----------
    fields : dict[bytes | str, bytes | str]
        Field map from ``XREAD``.

    Returns
    -------
    AnyEvent
        The event.

    Raises
    ------
    KeyError
        If the ``data`` field is missing.
    """
    data = fields.get(DATA_FIELD)
    if data is None:
        data = fields[DATA_FIELD.encode()]
    return decode(data)


def stream_for(event: AnyEvent) -> str:
    """
    Return the stream an event belongs on.

    Parameters
    ----------
    event : AnyEvent
        The event.

    Returns
    -------
    str
        Stream name.
    """
    match event:
        case BookEvent():
            return book_stream(event.venue, event.symbol)
        case TradeEvent():
            return trade_stream(event.venue, event.symbol)
        case BalanceEvent():
            return balance_stream(event.venue)
        case OrderIntent() | CancelIntent():
            return INTENTS_STREAM
        case OrderEvent():
            return ORDER_EVENTS_STREAM
        case LatencyRecord():
            return LATENCY_STREAM
    raise TypeError(f"Unknown event type: {type(event)!r}")
