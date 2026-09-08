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


class FeeConfig(msgspec.Struct, frozen=True):
    """
    Trading fees of one venue, as fractions of the traded notional.

    The simulated broker charges the fee in the asset received: quote on a
    sell, base on a buy, which is how the venues traded so far bill a spot
    fill. A venue that bills differently needs a field here, not a guess.

    Attributes
    ----------
    maker : float
        Fee on a fill of a resting order.
    taker : float
        Fee on a fill that crossed the book.
    """

    maker: float = 0.0
    taker: float = 0.0


class BacktestConfig(msgspec.Struct, frozen=True):
    """
    Settings for the simulated broker.

    Attributes
    ----------
    fees : dict[str, FeeConfig]
        Fees per venue id. A venue without an entry trades free, and the
        broker says so at startup.
    history_s : float
        Recorded seconds of books and trades the broker keeps per feed, so
        an order can be matched against the market as it stood when the
        order reached the venue rather than when the broker heard of it.
    idle_s : float
        Wall seconds without a new intent, once the replay is done and every
        followed strategy has finished, before the broker winds down.
    """

    fees: dict[str, FeeConfig] = {}
    history_s: float = 60.0
    idle_s: float = 3.0


class VenueConfig(msgspec.Struct, frozen=True):
    """
    A trading venue.

    Attributes
    ----------
    id : str
        CCXT short id, e.g. ``gate``.
    name : str
        Human readable name.
    options : dict[str, Any]
        CCXT ``options`` passed to the client constructor, e.g.
        ``{"watchOrderBook": {"checksum": False}}``.
    """

    id: str
    name: str
    options: dict[str, Any] = {}


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
    backtest : BacktestConfig
        Simulated broker settings.
    """

    redis: RedisConfig
    market_data: MarketDataConfig
    venues: tuple[VenueConfig, ...]
    strategies: tuple[StrategyConfig, ...]
    refresh_speed: float | None = None
    recorder: RecorderConfig = RecorderConfig()
    oms: OmsConfig = OmsConfig()
    backtest: BacktestConfig = BacktestConfig()

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
        On duplicate identifiers within a production group, references to
        undeclared venues or unknown feed names.
    """
    venue_ids = config.venue_ids
    unknown_fee_venues = set(config.backtest.fees) - venue_ids
    if unknown_fee_venues:
        raise ConfigError(
            f"backtest.fees names undeclared venues {sorted(unknown_fee_venues)}"
        )
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
        backtest = msgspec.convert(raw.get("backtest", {}), BacktestConfig)
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
        backtest=backtest,
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
