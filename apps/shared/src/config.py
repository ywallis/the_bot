"""Typed application configuration.

This module is the single place where ``config.toml`` is read and validated.
Every strategy declares its market data ``subscriptions`` explicitly and keeps
its free-form parameters under ``[strategies.params]``.

See ``docs/design/event-driven-framework.md`` section 5.
"""

import logging
import os
import tomllib
from pathlib import Path
from typing import Any

import msgspec
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = (
    Path(__file__).parents[3] / "apps" / "strategies" / "config" / "config.toml"
)

STRATEGY_KEYS: frozenset[str] = frozenset(
    {"identifier", "type", "production", "subscriptions", "params"}
)

BOOK_FEED = "book"
TRADE_FEED = "trade"
KNOWN_FEEDS: frozenset[str] = frozenset({BOOK_FEED, TRADE_FEED})

# Fee currency policies a venue can declare: always the quote asset, always
# the base asset, or the asset received (base on buys, quote on sells). Any
# other uppercase asset code is accepted and means the venue charges its own
# token regardless of side.
FEE_POLICY_KEYWORDS: frozenset[str] = frozenset({"quote", "received", "base"})

# What a venue entry trades. ``spot`` is the default and what every venue
# was until derivatives arrived. ``swap`` is a linear perpetual account:
# CCXT's ``defaultType`` is set accordingly and every symbol on the venue
# must carry a settle suffix (``BASE/QUOTE:QUOTE``). See
# ``docs/design/inventory-hedging.md`` section 3.
SPOT_MARKET = "spot"
SWAP_MARKET = "swap"
MARKET_TYPES: frozenset[str] = frozenset({SPOT_MARKET, SWAP_MARKET})

# How a venue's wallets are arranged. ``classic`` keeps spot and derivatives
# in separate wallets, so a perpetual is a second venue entry with
# ``market_type = "swap"``. ``unified`` is one margin pool where spot
# holdings collateralise a short, so one entry serves both symbol kinds.
CLASSIC_ACCOUNT = "classic"
UNIFIED_ACCOUNT = "unified"
ACCOUNT_TYPES: frozenset[str] = frozenset({CLASSIC_ACCOUNT, UNIFIED_ACCOUNT})

MARGIN_MODES: frozenset[str] = frozenset({"cross", "isolated"})


def is_contract(symbol: str) -> bool:
    """
    Return whether a CCXT symbol names a derivative rather than a spot market.

    CCXT spells a linear perpetual ``BASE/QUOTE:SETTLE``; the colon and the
    settlement currency after it are what distinguish it from ``BASE/QUOTE``.

    Parameters
    ----------
    symbol : str
        CCXT symbol.

    Returns
    -------
    bool
        True for a contract symbol.
    """
    return ":" in symbol


class ConfigError(Exception):
    """Raised when the configuration is missing or inconsistent."""


class RedisConfig(msgspec.Struct, frozen=True):
    """
    Connection details for the Redis bus.

    Attributes
    ----------
    host : str
        Redis hostname.
    port : int
        Redis port.
    """

    host: str = "localhost"
    port: int = 6379


class MarketDataConfig(msgspec.Struct, frozen=True):
    """
    Settings for market data publication.

    Attributes
    ----------
    book_depth : int
        Number of levels per side published in book events.
    stream_maxlen : int
        Approximate maximum length of each market data stream.
    """

    book_depth: int = 20
    stream_maxlen: int = 10_000


class RecorderConfig(msgspec.Struct, frozen=True):
    """
    Settings for the stream recorder.

    Attributes
    ----------
    root : str
        Directory under which recorded streams are written. Relative paths
        are resolved against the current working directory.
    flush_interval_s : float
        Seconds between forced flushes of open files.
    seal_grace_s : float
        How long past the end of an hour bucket the recorder keeps its file
        open. Bounds how long a quiet stream's data waits on the trading
        host before the shipper may take it.
    block_ms : int
        How long a blocking ``XREAD`` waits when no entry is available.
    batch : int
        Maximum entries fetched per stream per ``XREAD``.
    """

    root: str = "data"
    flush_interval_s: float = 1.0
    seal_grace_s: float = 300.0
    block_ms: int = 1000
    batch: int = 1000


class OmsConfig(msgspec.Struct, frozen=True):
    """
    Settings for the order manager.

    Attributes
    ----------
    consumer : str
        Consumer name inside the ``oms`` consumer group. It is deliberately
        stable across restarts: a restarted order manager then reclaims the
        entries its previous incarnation read but never acknowledged, which
        a per-process name would strand as another consumer's pending list.
    block_ms : int
        How long a blocking ``XREADGROUP`` waits when no intent is available.
    batch : int
        Maximum intents fetched per read.
    stream_maxlen : int
        Approximate maximum length of ``oms:events`` and ``oms:latency``.
    max_intent_age_s : float
        Intents older than this are rejected instead of sent to a venue.
        Bounds the damage of a replay after a crash and of a strategy that
        stalled between building an intent and publishing it.
    """

    consumer: str = "oms"
    block_ms: int = 1000
    batch: int = 100
    stream_maxlen: int = 10_000
    max_intent_age_s: float = 5.0


class FeesConfig(msgspec.Struct, frozen=True):
    """
    Settings for the fee schedule feed.

    Attributes
    ----------
    refresh_s : int
        Seconds between fee schedule fetches. Fee levels follow the
        account's rolling volume and balance, so rates are refreshed rather
        than fetched once at startup.
    """

    refresh_s: int = 3600


class VenueConfig(msgspec.Struct, frozen=True):
    """
    A trading venue.

    Attributes
    ----------
    id : str
        Venue id, unique in the config. It keys every stream, client and
        order, and prefixes the credential variables in the environment.
        It is the CCXT short id (e.g. ``gate``) unless ``ccxt_id`` says
        otherwise.
    name : str
        Human readable name.
    options : dict[str, Any]
        CCXT ``options`` passed to the client constructor, e.g.
        ``{"watchOrderBook": {"checksum": False}}``.
    fee_currency : str
        Which currency the venue charges fees in: one of
        ``FEE_POLICY_KEYWORDS`` or an explicit asset code. Verified against
        the fee currencies venues actually report on fills.
    maker_fee : float | None
        Static maker fee rate as a fraction of traded value, overriding
        what the venue reports. For venues whose API does not expose their
        fee schedule.
    taker_fee : float | None
        Static taker fee rate as a fraction of traded value, see
        ``maker_fee``.
    ccxt_id : str | None
        CCXT exchange class to build the client from, when it differs from
        ``id``. This is how one exchange appears twice in the config: once
        as its spot wallet and once as its futures wallet.
    market_type : str
        One of ``MARKET_TYPES``. ``swap`` sets CCXT's ``defaultType`` and
        requires every symbol subscribed on the venue to be a contract.
    account : str
        One of ``ACCOUNT_TYPES``. ``unified`` declares one margin pool for
        spot and derivatives, on which both symbol kinds may be subscribed.
        CCXT's own switch for the account type, where the venue has one,
        goes in ``options`` like any other client option.
    margin_mode : str | None
        One of ``MARGIN_MODES``, set on every contract symbol at broker
        start. Only meaningful on a derivatives venue.
    leverage : int | None
        Leverage set on every contract symbol at broker start. Only
        meaningful on a derivatives venue.
    leverage_params : tuple[dict[str, Any], ...]
        Extra CCXT parameters for the leverage call, one call per entry.
        Some venues set leverage per margin type and position side and
        refuse a call without them; declaring the venue's own parameter
        names here keeps that out of the code. Empty means one call with
        no parameters.
    credentials : str | None
        Prefix of the ``_KEY``, ``_SECRET`` and ``_PASSWORD`` environment
        variables, uppercased. Defaults to ``id``, so two entries for one
        exchange each read their own key unless told to share one.
    """

    id: str
    name: str
    options: dict[str, Any] = {}
    fee_currency: str = "quote"
    maker_fee: float | None = None
    taker_fee: float | None = None
    ccxt_id: str | None = None
    market_type: str = SPOT_MARKET
    account: str = CLASSIC_ACCOUNT
    margin_mode: str | None = None
    leverage: int | None = None
    leverage_params: tuple[dict[str, Any], ...] = ()
    credentials: str | None = None

    @property
    def exchange(self) -> str:
        """
        Return the CCXT exchange class name the client is built from.

        Returns
        -------
        str
            ``ccxt_id`` if set, else ``id``.
        """
        return self.ccxt_id or self.id

    @property
    def env_prefix(self) -> str:
        """
        Return the uppercased prefix of this venue's credential variables.

        Returns
        -------
        str
            ``credentials`` if set, else ``id``, uppercased.
        """
        return (self.credentials or self.id).upper()

    @property
    def derivatives(self) -> bool:
        """
        Return whether contract symbols may be traded on this venue.

        Returns
        -------
        bool
            True for a ``swap`` venue or a ``unified`` account.
        """
        return self.market_type == SWAP_MARKET or self.account == UNIFIED_ACCOUNT

    def accepts(self, symbol: str) -> bool:
        """
        Return whether a symbol belongs on this venue.

        Parameters
        ----------
        symbol : str
            CCXT symbol.

        Returns
        -------
        bool
            A unified account takes both kinds; a spot venue takes spot
            symbols only and a swap venue contract symbols only.
        """
        if self.account == UNIFIED_ACCOUNT:
            return True
        return is_contract(symbol) == (self.market_type == SWAP_MARKET)


class Subscription(msgspec.Struct, frozen=True):
    """
    A market data subscription: one venue, one symbol, one or more feeds.

    Attributes
    ----------
    venue : str
        CCXT short id of the venue.
    symbol : str
        CCXT symbol, e.g. ``BTC/USDT``.
    feeds : tuple[str, ...]
        Feed names, e.g. ``("book", "trade")``.
    """

    venue: str
    symbol: str
    feeds: tuple[str, ...] = ("book",)


class StrategyConfig(msgspec.Struct, frozen=True):
    """
    A strategy instance.

    Attributes
    ----------
    identifier : str
        Short unique identifier used in order ids.
    type : str
        Name of the strategy module and entry function.
    production : bool
        Whether the strategy runs in production mode.
    subscriptions : tuple[Subscription, ...]
        Market data the strategy needs.
    params : dict[str, Any]
        Free-form strategy parameters, passed through untouched.
    """

    identifier: str
    type: str
    production: bool
    subscriptions: tuple[Subscription, ...]
    params: dict[str, Any]

    @property
    def symbols(self) -> set[str]:
        """
        Return the set of symbols this strategy subscribes to.

        Returns
        -------
        set[str]
            Symbols.
        """
        return {sub.symbol for sub in self.subscriptions}

    @property
    def venue_symbol_pairs(self) -> set[tuple[str, str]]:
        """
        Return the set of (venue, symbol) tuples this strategy subscribes to.

        Returns
        -------
        set[tuple[str, str]]
            Venue and symbol tuples.
        """
        return {(sub.venue, sub.symbol) for sub in self.subscriptions}

    def to_strategy_dict(self, refresh_speed: float | None) -> dict[str, Any]:
        """
        Flatten the strategy into the dict passed to ``strategy(redis, config)``.

        Parameters
        ----------
        refresh_speed : float | None
            Polling interval injected as ``refresh_speed``, if set.

        Returns
        -------
        dict[str, Any]
            Flat dict with identifier, type, production, all params and the
            subscriptions as a list of dicts.
        """
        flat: dict[str, Any] = {
            "identifier": self.identifier,
            "type": self.type,
            "production": self.production,
        }
        flat.update(self.params)
        flat["subscriptions"] = [msgspec.to_builtins(s) for s in self.subscriptions]
        if refresh_speed is not None:
            flat["refresh_speed"] = refresh_speed
        return flat


class AppConfig(msgspec.Struct, frozen=True):
    """
    The full validated configuration.

    Attributes
    ----------
    redis : RedisConfig
        Redis connection details.
    market_data : MarketDataConfig
        Market data publication settings.
    venues : tuple[VenueConfig, ...]
        Declared venues.
    strategies : tuple[StrategyConfig, ...]
        All strategies, production and test alike.
    refresh_speed : float | None
        Polling interval for strategies that still read snapshot keys.
    recorder : RecorderConfig
        Stream recorder settings.
    oms : OmsConfig
        Order manager settings.
    fees : FeesConfig
        Fee schedule feed settings.
    """

    redis: RedisConfig
    market_data: MarketDataConfig
    venues: tuple[VenueConfig, ...]
    strategies: tuple[StrategyConfig, ...]
    refresh_speed: float | None = None
    recorder: RecorderConfig = RecorderConfig()
    oms: OmsConfig = OmsConfig()
    fees: FeesConfig = FeesConfig()

    @property
    def venue_ids(self) -> set[str]:
        """
        Return the ids of all declared venues.

        Returns
        -------
        set[str]
            Venue ids.
        """
        return {v.id for v in self.venues}

    def venue(self, venue_id: str) -> VenueConfig:
        """
        Return a declared venue by id.

        Parameters
        ----------
        venue_id : str
            The venue id.

        Returns
        -------
        VenueConfig
            The venue.

        Raises
        ------
        KeyError
            If no venue has that id.
        """
        for venue in self.venues:
            if venue.id == venue_id:
                return venue
        raise KeyError(venue_id)

    def active_strategies(self, production: bool | None = None) -> list[StrategyConfig]:
        """
        Return strategies filtered by production flag.

        Parameters
        ----------
        production : bool | None
            Keep only strategies with this production flag. None keeps all.

        Returns
        -------
        list[StrategyConfig]
            Strategies in config order.
        """
        if production is None:
            return list(self.strategies)
        return [s for s in self.strategies if s.production == production]

    def subscriptions(self, production: bool | None = None) -> set[Subscription]:
        """
        Return the union of subscriptions across selected strategies.

        Parameters
        ----------
        production : bool | None
            Production filter, see ``active_strategies``.

        Returns
        -------
        set[Subscription]
            Distinct subscriptions.
        """
        subs: set[Subscription] = set()
        for strategy in self.active_strategies(production):
            subs.update(strategy.subscriptions)
        return subs

    def feed_pairs(
        self, feed: str, production: bool | None = None
    ) -> set[tuple[str, str]]:
        """
        Return distinct (venue, symbol) tuples that subscribe to a feed.

        Parameters
        ----------
        feed : str
            Feed name, e.g. ``"book"`` or ``"trade"``.
        production : bool | None
            Production filter, see ``active_strategies``.

        Returns
        -------
        set[tuple[str, str]]
            Venue and symbol tuples whose subscription lists ``feed``.
        """
        return {
            (s.venue, s.symbol)
            for s in self.subscriptions(production)
            if feed in s.feeds
        }

    def venue_symbol_pairs(
        self, production: bool | None = None
    ) -> set[tuple[str, str]]:
        """
        Return distinct (venue, symbol) tuples across selected strategies.

        Parameters
        ----------
        production : bool | None
            Production filter, see ``active_strategies``.

        Returns
        -------
        set[tuple[str, str]]
            Venue and symbol tuples.
        """
        return {(s.venue, s.symbol) for s in self.subscriptions(production)}

    def symbols(self, production: bool | None = None) -> set[str]:
        """
        Return distinct symbols across selected strategies.

        Parameters
        ----------
        production : bool | None
            Production filter, see ``active_strategies``.

        Returns
        -------
        set[str]
            Symbols.
        """
        return {s.symbol for s in self.subscriptions(production)}


def production_mode() -> bool:
    """
    Return whether the process runs in production mode.

    Production is the default. Setting ``TESTING=True`` in the environment
    selects test strategies instead.

    Returns
    -------
    bool
        True in production mode.
    """
    return os.getenv("TESTING", "False") != "True"


def load_raw_config(path: Path | None = None) -> dict[str, Any]:
    """
    Read the TOML configuration file without validation.

    Parameters
    ----------
    path : Path | None
        Path to the TOML file. Defaults to the strategies submodule config.

    Returns
    -------
    dict[str, Any]
        The parsed TOML document.

    Raises
    ------
    ConfigError
        If the file does not exist.
    """
    config_path = path or DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def _parse_strategy(raw: dict[str, Any]) -> StrategyConfig:
    """
    Convert a raw strategy table into a ``StrategyConfig``.

    Parameters
    ----------
    raw : dict[str, Any]
        Raw strategy table from TOML.

    Returns
    -------
    StrategyConfig
        The validated strategy.

    Raises
    ------
    ConfigError
        If required keys are missing, unknown keys are present or values are
        malformed. Strategy parameters must live under ``params``; a stray
        top-level key is an error rather than a silently dropped parameter.
    """
    for key in ("identifier", "type", "production", "subscriptions"):
        if key not in raw:
            raise ConfigError(
                f"Strategy {raw.get('identifier', raw)!r} is missing required "
                f"key {key!r}"
            )
    unknown = set(raw) - STRATEGY_KEYS
    if unknown:
        raise ConfigError(
            f"Strategy {raw['identifier']!r} has unknown keys {sorted(unknown)}; "
            "strategy parameters belong under [strategies.params]"
        )

    try:
        subscriptions = msgspec.convert(raw["subscriptions"], tuple[Subscription, ...])
    except msgspec.ValidationError as e:
        raise ConfigError(
            f"Strategy {raw['identifier']!r} has invalid subscriptions: {e}"
        ) from e

    params = raw.get("params", {})
    if not isinstance(params, dict):
        raise ConfigError(f"Strategy {raw['identifier']!r} params must be a table")

    try:
        return StrategyConfig(
            identifier=str(raw["identifier"]),
            type=str(raw["type"]),
            production=bool(raw["production"]),
            subscriptions=subscriptions,
            params=dict(params),
        )
    except msgspec.ValidationError as e:
        raise ConfigError(f"Strategy {raw['identifier']!r} is invalid: {e}") from e


def _validate_fee_policy(venue: VenueConfig) -> None:
    """
    Check that a venue's fee currency policy is a keyword or asset code.

    Parameters
    ----------
    venue : VenueConfig
        The venue whose policy is checked.

    Raises
    ------
    ConfigError
        If the policy is neither a keyword nor an asset code, or a static
        fee override is not a fraction of traded value.
    """
    policy = venue.fee_currency
    if policy not in FEE_POLICY_KEYWORDS and not (
        policy.isalnum() and policy == policy.upper()
    ):
        raise ConfigError(
            f"Venue {venue.id!r} has invalid fee_currency {policy!r}; use one "
            f"of {sorted(FEE_POLICY_KEYWORDS)} or an explicit asset code"
        )
    for name, rate in (("maker_fee", venue.maker_fee), ("taker_fee", venue.taker_fee)):
        if rate is not None and not 0 <= rate < 1:
            raise ConfigError(
                f"Venue {venue.id!r} has invalid {name} {rate!r}; a fee rate "
                "is a fraction of traded value, e.g. 0.001 for 0.1%"
            )


def _validate_markets(venue: VenueConfig) -> None:
    """
    Check a venue's market type, account type and derivatives settings.

    Parameters
    ----------
    venue : VenueConfig
        The venue.

    Raises
    ------
    ConfigError
        If a keyword is unknown, or leverage or a margin mode is declared
        on a venue that cannot trade contracts.
    """
    if venue.market_type not in MARKET_TYPES:
        raise ConfigError(
            f"Venue {venue.id!r} has invalid market_type {venue.market_type!r}; "
            f"use one of {sorted(MARKET_TYPES)}"
        )
    if venue.account not in ACCOUNT_TYPES:
        raise ConfigError(
            f"Venue {venue.id!r} has invalid account {venue.account!r}; "
            f"use one of {sorted(ACCOUNT_TYPES)}"
        )
    if venue.margin_mode is not None and venue.margin_mode not in MARGIN_MODES:
        raise ConfigError(
            f"Venue {venue.id!r} has invalid margin_mode {venue.margin_mode!r}; "
            f"use one of {sorted(MARGIN_MODES)}"
        )
    if venue.leverage is not None and venue.leverage < 1:
        raise ConfigError(
            f"Venue {venue.id!r} has invalid leverage {venue.leverage!r}; "
            "it is a whole multiple of 1"
        )
    if venue.leverage_params and venue.leverage is None:
        raise ConfigError(
            f"Venue {venue.id!r} declares leverage_params without a leverage"
        )
    if not venue.derivatives and (
        venue.leverage is not None or venue.margin_mode is not None
    ):
        raise ConfigError(
            f"Venue {venue.id!r} declares leverage or margin_mode but trades "
            "spot only; set market_type = 'swap' or account = 'unified'"
        )


def _validate(config: AppConfig) -> None:
    """
    Check cross-field invariants.

    Parameters
    ----------
    config : AppConfig
        The assembled configuration.

    Raises
    ------
    ConfigError
        On duplicate venue or strategy identifiers, references to
        undeclared venues, unknown feed names, or a symbol subscribed on a
        venue whose market type cannot trade it.
    """
    venue_ids = config.venue_ids
    if len(venue_ids) != len(config.venues):
        raise ConfigError("Duplicate venue id")
    for venue in config.venues:
        _validate_fee_policy(venue)
        _validate_markets(venue)
    for production in (True, False):
        seen: set[str] = set()
        for strategy in config.active_strategies(production):
            if strategy.identifier in seen:
                raise ConfigError(
                    f"Duplicate strategy identifier {strategy.identifier!r} "
                    f"(production={production})"
                )
            seen.add(strategy.identifier)

    for strategy in config.strategies:
        for sub in strategy.subscriptions:
            if sub.venue not in venue_ids:
                raise ConfigError(
                    f"Strategy {strategy.identifier!r} subscribes to undeclared "
                    f"venue {sub.venue!r}"
                )
            venue = config.venue(sub.venue)
            if not venue.accepts(sub.symbol):
                kind = "a contract" if is_contract(sub.symbol) else "a spot"
                raise ConfigError(
                    f"Strategy {strategy.identifier!r} subscribes to {kind} symbol "
                    f"{sub.symbol!r} on venue {sub.venue!r}, whose market_type is "
                    f"{venue.market_type!r}; contract symbols carry a settle suffix "
                    "(BASE/QUOTE:QUOTE) and belong on a swap venue or a unified "
                    "account"
                )
            unknown = set(sub.feeds) - KNOWN_FEEDS
            if unknown:
                raise ConfigError(
                    f"Strategy {strategy.identifier!r} subscribes to unknown "
                    f"feeds {sorted(unknown)} on {sub.venue}:{sub.symbol}; "
                    f"known feeds are {sorted(KNOWN_FEEDS)}"
                )


def parse_app_config(raw: dict[str, Any]) -> AppConfig:
    """
    Build and validate an ``AppConfig`` from a parsed TOML document.

    Parameters
    ----------
    raw : dict[str, Any]
        The TOML document as a dict.

    Returns
    -------
    AppConfig
        The validated configuration.

    Raises
    ------
    ConfigError
        If the document is malformed or inconsistent.
    """
    try:
        redis = msgspec.convert(raw.get("redis", {}), RedisConfig)
        market_data = msgspec.convert(raw.get("market_data", {}), MarketDataConfig)
        recorder = msgspec.convert(raw.get("recorder", {}), RecorderConfig)
        oms = msgspec.convert(raw.get("oms", {}), OmsConfig)
        fees = msgspec.convert(raw.get("fees", {}), FeesConfig)
        venues = msgspec.convert(raw.get("venues", []), tuple[VenueConfig, ...])
    except msgspec.ValidationError as e:
        raise ConfigError(str(e)) from e

    raw_strategies = raw.get("strategies")
    if not raw_strategies:
        raise ConfigError("No strategy found")

    refresh_speed_raw = raw.get("refresh_speed")
    refresh_speed = float(refresh_speed_raw) if refresh_speed_raw is not None else None

    config = AppConfig(
        redis=redis,
        market_data=market_data,
        venues=venues,
        strategies=tuple(_parse_strategy(s) for s in raw_strategies),
        refresh_speed=refresh_speed,
        recorder=recorder,
        oms=oms,
        fees=fees,
    )
    _validate(config)
    return config


def load_app_config(path: Path | None = None) -> AppConfig:
    """
    Read and validate the configuration file.

    Parameters
    ----------
    path : Path | None
        Path to the TOML file. Defaults to the strategies submodule config.

    Returns
    -------
    AppConfig
        The validated configuration.
    """
    return parse_app_config(load_raw_config(path))
