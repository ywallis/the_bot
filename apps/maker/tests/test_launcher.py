"""Tests for the strategy launcher's module resolution."""

from types import ModuleType
from typing import Any

import pytest

from apps.maker.src.launcher import (
    STRATEGY_ATTRIBUTE,
    legacy_entry_point,
    run,
    runtime_strategy_class,
)
from apps.shared.src.config import (
    AppConfig,
    MarketDataConfig,
    RedisConfig,
    StrategyConfig,
    Subscription,
)
from apps.shared.src.runtime import Strategy


def strategy_config(type_: str) -> StrategyConfig:
    """Return a strategy config of a given type."""
    return StrategyConfig(
        identifier="fmb",
        type=type_,
        production=False,
        subscriptions=(Subscription(venue="mexc", symbol="ALPH/USDT"),),
        params={"a": 1},
    )


def config() -> AppConfig:
    """Return a minimal application config."""
    return AppConfig(
        redis=RedisConfig(),
        market_data=MarketDataConfig(),
        venues=(),
        strategies=(),
        refresh_speed=0.5,
    )


class Native(Strategy):
    """A runtime strategy that remembers its config."""

    def __init__(self, strategy: StrategyConfig) -> None:
        """Keep the config."""
        self.strategy = strategy


def module(name: str, **attributes: Any) -> ModuleType:
    """Build a module with the given attributes."""
    built = ModuleType(name)
    for key, value in attributes.items():
        setattr(built, key, value)
    return built


def test_a_module_exporting_strategy_is_native():
    """``STRATEGY`` selects the runtime path."""
    assert runtime_strategy_class(module("m", STRATEGY=Native)) is Native


def test_a_module_without_strategy_is_legacy():
    """No ``STRATEGY`` means the legacy coroutine is looked up by type."""
    async def fake_maker(redis: Any, strategy: dict[str, Any]) -> None:
        pass

    built = module("m", fake_maker=fake_maker)
    assert runtime_strategy_class(built) is None
    assert legacy_entry_point(built, strategy_config("fake_maker")) is fake_maker


def test_a_strategy_that_is_not_a_strategy_is_refused():
    """Exporting something else under ``STRATEGY`` is a configuration error."""
    with pytest.raises(TypeError):
        runtime_strategy_class(module("m", STRATEGY=object))
    with pytest.raises(TypeError):
        runtime_strategy_class(module("m", **{STRATEGY_ATTRIBUTE: Native(strategy_config("x"))}))


def test_a_module_with_neither_shape_is_an_error():
    """A module that exports nothing usable fails loudly."""
    with pytest.raises(AttributeError):
        legacy_entry_point(module("m"), strategy_config("fake_maker"))


@pytest.mark.asyncio
async def test_run_hands_a_native_strategy_its_config(monkeypatch: pytest.MonkeyPatch):
    """The runtime path constructs the class with the ``StrategyConfig``."""
    seen: dict[str, Any] = {}

    async def fake_run_strategy(redis: Any, config: AppConfig, strategy: StrategyConfig, handler: Strategy) -> None:
        seen.update(redis=redis, strategy=strategy, handler=handler)

    monkeypatch.setattr("apps.maker.src.launcher.run_strategy", fake_run_strategy)
    monkeypatch.setattr(
        "apps.maker.src.launcher.load_strategy_module",
        lambda strategy: module("m", STRATEGY=Native),
    )
    await run("redis", config(), strategy_config("fake_maker"))
    assert isinstance(seen["handler"], Native)
    assert seen["handler"].strategy == strategy_config("fake_maker")
    assert seen["redis"] == "redis"


@pytest.mark.asyncio
async def test_run_hands_a_legacy_strategy_the_flat_dict(monkeypatch: pytest.MonkeyPatch):
    """The legacy path passes the flattened dict with ``refresh_speed``."""
    seen: dict[str, Any] = {}

    async def fake_maker(redis: Any, strategy: dict[str, Any]) -> None:
        seen.update(redis=redis, strategy=strategy)

    monkeypatch.setattr(
        "apps.maker.src.launcher.load_strategy_module",
        lambda strategy: module("m", fake_maker=fake_maker),
    )
    await run("redis", config(), strategy_config("fake_maker"))
    assert seen["strategy"]["identifier"] == "fmb"
    assert seen["strategy"]["a"] == 1
    assert seen["strategy"]["refresh_speed"] == 0.5
