"""Typed application configuration.

This module is the single place where ``config.toml`` is read and validated.
It supports two strategy shapes:

- The target shape, where a strategy declares ``subscriptions`` explicitly.
- The legacy shape, where a strategy carries ``exchange_1``, ``exchange_2``,
  ``maker_exchange``, ``taker_exchange`` and ``symbol``. Subscriptions are
  derived from those keys so existing strategies keep working unchanged.

See ``docs/design/event-driven-framework.md`` section 5.
"""

import logging
import os
import tomllib
from pathlib import Path
from typing import Any

import msgspec

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = (
    Path(__file__).parents[3] / "apps" / "strategies" / "config" / "config.toml"
)

LEGACY_VENUE_KEYS: tuple[str, ...] = (
    "exchange_1",
    "exchange_2",
    "maker_exchange",
    "taker_exchange",
)

RESERVED_STRATEGY_KEYS: frozenset[str] = frozenset(
    {"identifier", "type", "production", "subscriptions"}
)


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


class VenueConfig(msgspec.Struct, frozen=True):
    """
    A trading venue.

    Attributes
    ----------
    id : str
        CCXT short id, e.g. ``gate``.
    name : str
        Human readable name.
    """

    id: str
    name: str


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
    legacy : bool
        True if subscriptions were derived from legacy keys.
    """

    identifier: str
    type: str
    production: bool
    subscriptions: tuple[Subscription, ...]
    params: dict[str, Any]
    legacy: bool = False

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

    def to_legacy_dict(self, refresh_speed: float | None) -> dict[str, Any]:
        """
        Flatten the strategy into the dict shape existing strategies expect.

        Parameters
        ----------
        refresh_speed : float | None
            Polling interval injected as ``refresh_speed``, if set.

        Returns
        -------
        dict[str, Any]
            Flat dict with identifier, type, production, all params and,
            unless legacy, the subscriptions as a list of dicts.
        """
        flat: dict[str, Any] = {
            "identifier": self.identifier,
            "type": self.type,
            "production": self.production,
        }
        flat.update(self.params)
        if not self.legacy:
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
        Polling interval for legacy strategies.
    """

    redis: RedisConfig
    market_data: MarketDataConfig
    venues: tuple[VenueConfig, ...]
    strategies: tuple[StrategyConfig, ...]
    refresh_speed: float | None = None

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

    def venue_symbol_pairs(self, production: bool | None = None) -> set[tuple[str, str]]:
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


def _derive_legacy_subscriptions(raw: dict[str, Any]) -> tuple[Subscription, ...]:
    """
    Build subscriptions from legacy two-venue strategy keys.

    Parameters
    ----------
    raw : dict[str, Any]
        Raw strategy table.

    Returns
    -------
    tuple[Subscription, ...]
        One book subscription per distinct venue, in key order.

    Raises
    ------
    ConfigError
        If no symbol or no venue key is present.
    """
    symbol = raw.get("symbol")
    if not isinstance(symbol, str):
        raise ConfigError(
            f"Strategy {raw.get('identifier')!r} has neither 'subscriptions' "
            "nor a legacy 'symbol'"
        )
    venues: list[str] = []
    for key in LEGACY_VENUE_KEYS:
        venue = raw.get(key)
        if isinstance(venue, str) and venue not in venues:
            venues.append(venue)
    if not venues:
        raise ConfigError(
            f"Strategy {raw.get('identifier')!r} has neither 'subscriptions' "
            f"nor any of {LEGACY_VENUE_KEYS}"
        )
    return tuple(Subscription(venue=v, symbol=symbol) for v in venues)


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
        If required keys are missing or malformed.
    """
    for key in ("identifier", "type", "production"):
        if key not in raw:
            raise ConfigError(f"Strategy {raw!r} is missing required key {key!r}")

    legacy = "subscriptions" not in raw
    if legacy:
        subscriptions = _derive_legacy_subscriptions(raw)
        logger.info(
            "Strategy %s uses legacy venue keys, derived subscriptions %s",
            raw["identifier"],
            subscriptions,
        )
    else:
        try:
            subscriptions = msgspec.convert(
                raw["subscriptions"], tuple[Subscription, ...]
            )
        except msgspec.ValidationError as e:
            raise ConfigError(
                f"Strategy {raw['identifier']!r} has invalid subscriptions: {e}"
            ) from e

    # Explicit [strategies.params] table wins; otherwise every non-reserved
    # key is a parameter, which is what legacy strategies rely on.
    if "params" in raw and isinstance(raw["params"], dict):
        params = dict(raw["params"])
    else:
        params = {k: v for k, v in raw.items() if k not in RESERVED_STRATEGY_KEYS}

    try:
        return StrategyConfig(
            identifier=str(raw["identifier"]),
            type=str(raw["type"]),
            production=bool(raw["production"]),
            subscriptions=subscriptions,
            params=params,
            legacy=legacy,
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
        undeclared venues, or legacy strategies without a refresh speed.
    """
    venue_ids = config.venue_ids
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

    if config.refresh_speed is None and any(s.legacy for s in config.strategies):
        raise ConfigError("refresh_speed is required while legacy strategies exist")


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
        venues = msgspec.convert(
            raw.get("venues", raw.get("exchanges", [])), tuple[VenueConfig, ...]
        )
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
