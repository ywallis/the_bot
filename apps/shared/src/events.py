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
ALERTS_STREAM = "alerts"
OMS_CONSUMER_GROUP = "oms"

# ``replace_of`` value meaning "whatever this strategy key has resting". A
# quote's first intent has no predecessor to name but must still rest under
# its strategy key, so that a later quote supersedes it and shutdown cancels
# it. Mirrors the empty ``target_intent_id`` of a ``CancelIntent``.
REPLACE_RESTING = ""

# Tag a strategy puts on an intent whose hedge it places itself, as one leg
# of a pair sent together. The matcher skips a fill carrying it; without
# the tag the taken basis would be hedged twice. Any value counts, ``"1"``
# by convention. See ``docs/design/inventory-hedging.md`` section 7.
SELF_HEDGED_TAG = "self_hedged"


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


def fees_stream(venue: str) -> str:
    """
    Return the fee schedule stream name for a venue.

    Parameters
    ----------
    venue : str
        CCXT short id.

    Returns
    -------
    str
        Stream name.
    """
    return f"acct:fees:{venue}"


def position_stream(venue: str) -> str:
    """
    Return the position stream name for a derivatives venue.

    Parameters
    ----------
    venue : str
        Venue id.

    Returns
    -------
    str
        Stream name.
    """
    return f"acct:position:{venue}"


def funding_stream(venue: str, symbol: str) -> str:
    """
    Return the funding rate stream name for a perpetual.

    Parameters
    ----------
    venue : str
        Venue id.
    symbol : str
        CCXT contract symbol.

    Returns
    -------
    str
        Stream name.
    """
    return f"md:funding:{venue}:{symbol}"


def delta_stream(asset: str) -> str:
    """
    Return the delta stream name for an asset.

    Parameters
    ----------
    asset : str
        Base asset code, e.g. ``BTC``.

    Returns
    -------
    str
        Stream name.
    """
    return f"acct:delta:{asset}"


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
    FEE_SCHEDULE = "fee_schedule"
    ORDER_INTENT = "order_intent"
    CANCEL_INTENT = "cancel_intent"
    ORDER_EVENT = "order_event"
    LATENCY = "latency"
    POSITION = "position"
    FUNDING = "funding"
    DELTA = "delta"
    ALERT = "alert"


class FeeSource(str, Enum):
    """Where a fee schedule's rates came from."""

    TRADING_FEES = "trading_fees"
    MARKETS = "markets"
    CONFIG = "config"


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


class PositionSide(str, Enum):
    """Direction of a derivatives position."""

    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class AlertLevel(str, Enum):
    """How urgently an alert wants the operator."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


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


class SymbolFees(msgspec.Struct):
    """
    Trading terms for one symbol on one venue.

    Attributes
    ----------
    symbol : str
        CCXT symbol.
    maker : Decimal | None
        Maker fee rate as a fraction of the traded value, None if unknown.
    taker : Decimal | None
        Taker fee rate as a fraction of the traded value, None if unknown.
    min_cost : float | None
        Smallest order notional the venue accepts, None if unreported.
    amount_precision : int | None
        Decimal places an order amount may carry, None if unreported.
    contract_size : Decimal | None
        Base units one contract stands for, on a derivative. None on a
        spot market, where an order amount is already in base units. An
        order on a contract market is sized in contracts, so a hedge for
        a fill in base divides by this and quantizes to whole contracts.
    """

    symbol: str
    maker: Decimal | None
    taker: Decimal | None
    min_cost: float | None
    amount_precision: int | None
    contract_size: Decimal | None = None


class FeeScheduleEvent(Event, tag=EventType.FEE_SCHEDULE.value):
    """
    The fee schedule of one venue.

    Published periodically rather than once: fee levels follow the account's
    rolling volume and balance, so consumers size against a fresh schedule
    instead of rates frozen at startup. Which currency a fee is charged in
    is the policy declared in configuration (``fee_currency``), resolved
    against the base and quote assets of the symbol being traded; the
    ScheduleEvent's rates say how much, not in what.

    Attributes
    ----------
    venue : str
        CCXT short id.
    fee_currency : str
        Fee policy: ``quote`` (always the quote asset), ``base`` (always
        the base asset), ``received`` (base on buys, quote on sells) or an
        explicit asset code for venues that charge their own token.
    symbols : list[SymbolFees]
        One entry per traded symbol the venue lists.
    source : FeeSource
        Where the rates came from.
    ts_fetched : int | None
        When the schedule was fetched, milliseconds since the epoch.
    """

    venue: str
    fee_currency: str
    symbols: list[SymbolFees]
    source: FeeSource
    ts_fetched: int | None = None


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
        Size in the unit the market is traded in: base asset on spot,
        contracts on a derivative (see ``SymbolFees.contract_size``).
    price : Decimal | None
        Limit price, None for market orders.
    time_in_force : TimeInForce
        Time in force, defaults to good till cancelled.
    reduce_only : bool
        Whether the venue must only shrink an existing position with this
        order and never open one the other way. Meaningful on a contract
        symbol only; the order manager rejects it on a spot symbol. This is
        how an unwinding hedge is kept from flipping the short long when
        our view of the position and the venue's disagree.
    replace_of : str | None
        Intent id this intent supersedes. The order manager cancels that
        order first and coalesces pending replacements per strategy. None
        marks an independent order that is placed as is and left alone at
        shutdown. ``REPLACE_RESTING`` (the empty string) marks a quote with
        nothing to name yet: it rests under its strategy key like any other
        replacement and supersedes whatever that key holds.
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
    reduce_only: bool = False
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


class PositionEvent(Event, tag=EventType.POSITION.value):
    """
    The position held in one contract symbol on one venue.

    A snapshot by contract, like ``BalanceEvent``: the latest event per
    venue and symbol is the whole truth about that position. A symbol with
    no position is published as ``flat`` so that a consumer can tell "no
    position" from "no feed yet". See ``docs/design/inventory-hedging.md``
    section 5.

    Attributes
    ----------
    venue : str
        Venue id.
    symbol : str
        CCXT contract symbol.
    seq : int
        Per-stream producer sequence number.
    ts_exch : int | None
        Exchange timestamp in milliseconds, if provided.
    side : PositionSide
        Long, short or flat.
    contracts : Decimal
        Size in contracts, never negative; ``side`` carries the sign.
    contract_size : Decimal
        Base units one contract stands for.
    base_amount : Decimal
        Contracts times contract size, negative for a short. This is the
        figure delta is computed from.
    entry_price : Decimal | None
        Average entry price, if reported.
    mark_price : Decimal | None
        Mark price, if reported.
    liquidation_price : Decimal | None
        Liquidation price, if reported. A unified account often reports
        none for a hedged short, which is what it should be.
    unrealized_pnl : Decimal | None
        Unrealized profit in the settlement currency, if reported.
    margin_mode : str | None
        ``cross`` or ``isolated``, if reported.
    leverage : Decimal | None
        Leverage, if reported.
    collateral : Decimal | None
        Margin backing the position, if reported.
    """

    venue: str
    symbol: str
    seq: int
    ts_exch: int | None
    side: PositionSide
    contracts: Decimal
    contract_size: Decimal
    base_amount: Decimal
    entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    liquidation_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    margin_mode: str | None = None
    leverage: Decimal | None = None
    collateral: Decimal | None = None


class FundingEvent(Event, tag=EventType.FUNDING.value):
    """
    The current funding rate of one perpetual.

    Attributes
    ----------
    venue : str
        Venue id.
    symbol : str
        CCXT contract symbol.
    seq : int
        Per-stream producer sequence number.
    funding_rate : float
        Rate for the coming interval as a fraction of notional; positive
        pays the short.
    next_funding_ts : int | None
        When the next payment is due, milliseconds since the epoch.
    interval_s : int | None
        Seconds between payments, None when the venue does not say.
    mark_price : float | None
        Mark price, if reported.
    index_price : float | None
        Index price, if reported.
    """

    venue: str
    symbol: str
    seq: int
    funding_rate: float
    next_funding_ts: int | None = None
    interval_s: int | None = None
    mark_price: float | None = None
    index_price: float | None = None


class DeltaEvent(Event, tag=EventType.DELTA.value):
    """
    How far one asset's holdings are from flat, as the reconciler sees them.

    A snapshot. The inventory strategy reads it as its kill switch and the
    notifier for the out-of-band alert.

    Attributes
    ----------
    asset : str
        Base asset code.
    spot_base : dict[str, Decimal]
        Total base held per spot venue.
    perp_base : Decimal
        Perpetual position in base units, negative for a short.
    delta : Decimal
        Total spot base plus ``perp_base``.
    in_flight_base : Decimal
        Signed base of corrections sent and not yet reported terminal.
    deadband : Decimal
        Delta the reconciler tolerates without acting.
    healthy : bool
        False when a feed the delta depends on is missing or stale, or the
        margin guard has tripped. A builder stops on False.
    reason : str | None
        Why the delta is not healthy, if it is not.
    """

    asset: str
    spot_base: dict[str, Decimal]
    perp_base: Decimal
    delta: Decimal
    in_flight_base: Decimal
    deadband: Decimal
    healthy: bool = True
    reason: str | None = None


class AlertEvent(Event, tag=EventType.ALERT.value):
    """
    Something the operator should hear about.

    Attributes
    ----------
    source : str
        What raised it, e.g. a strategy identifier or ``notifier``.
    key : str
        Stable name of the condition, used for the cooldown: two alerts
        with one key within the cooldown are delivered once.
    level : AlertLevel
        Urgency.
    message : str
        Human readable text.
    """

    source: str
    key: str
    level: AlertLevel
    message: str


AnyEvent = Union[
    BookEvent,
    TradeEvent,
    BalanceEvent,
    FeeScheduleEvent,
    OrderIntent,
    CancelIntent,
    OrderEvent,
    LatencyRecord,
    PositionEvent,
    FundingEvent,
    DeltaEvent,
    AlertEvent,
]

EVENT_TYPES: dict[type, EventType] = {
    BookEvent: EventType.BOOK,
    TradeEvent: EventType.TRADE,
    BalanceEvent: EventType.BALANCE,
    FeeScheduleEvent: EventType.FEE_SCHEDULE,
    OrderIntent: EventType.ORDER_INTENT,
    CancelIntent: EventType.CANCEL_INTENT,
    OrderEvent: EventType.ORDER_EVENT,
    LatencyRecord: EventType.LATENCY,
    PositionEvent: EventType.POSITION,
    FundingEvent: EventType.FUNDING,
    DeltaEvent: EventType.DELTA,
    AlertEvent: EventType.ALERT,
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
        case FeeScheduleEvent():
            return fees_stream(event.venue)
        case OrderIntent() | CancelIntent():
            return INTENTS_STREAM
        case OrderEvent():
            return ORDER_EVENTS_STREAM
        case LatencyRecord():
            return LATENCY_STREAM
        case PositionEvent():
            return position_stream(event.venue)
        case FundingEvent():
            return funding_stream(event.venue, event.symbol)
        case DeltaEvent():
            return delta_stream(event.asset)
        case AlertEvent():
            return ALERTS_STREAM
    raise TypeError(f"Unknown event type: {type(event)!r}")
