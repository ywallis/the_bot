"""Tests for the typed configuration loader."""

import pytest

from apps.shared.src.config import (
    ConfigError,
    Subscription,
    load_app_config,
    parse_app_config,
)

VENUES = [{"id": "gate", "name": "Gate.io"}, {"id": "mexc", "name": "Mexc"}]


def legacy_strategy(**overrides):
    """Return a raw legacy-shaped strategy table."""
    raw = {
        "symbol": "ALPH/USDT",
        "type": "single_edge_liquidity",
        "identifier": "lam",
        "exchange_1": "gate",
        "exchange_2": "mexc",
        "taker_exchange": "gate",
        "maker_exchange": "mexc",
        "should_match": True,
        "spread": 1.0025,
        "min_size_usdt": 11,
        "production": True,
    }
    raw.update(overrides)
    return raw


def new_strategy(**overrides):
    """Return a raw strategy table in the target shape."""
    raw = {
        "identifier": "lat",
        "type": "latency_arb",
        "production": True,
        "subscriptions": [
            {"venue": "gate", "symbol": "BTC/USDT", "feeds": ["book", "trade"]},
            {"venue": "mexc", "symbol": "SOL/USDT"},
        ],
        "params": {"lag_ms": 400, "threshold": 0.002},
    }
    raw.update(overrides)
    return raw


def test_legacy_strategy_derives_subscriptions():
    """Legacy venue keys yield one book subscription per distinct venue."""
    config = parse_app_config(
        {"refresh_speed": 0.01, "exchanges": VENUES, "strategies": [legacy_strategy()]}
    )
    strategy = config.strategies[0]
    assert strategy.legacy is True
    assert strategy.subscriptions == (
        Subscription(venue="gate", symbol="ALPH/USDT"),
        Subscription(venue="mexc", symbol="ALPH/USDT"),
    )
    assert config.venue_symbol_pairs() == {("gate", "ALPH/USDT"), ("mexc", "ALPH/USDT")}
    assert config.symbols() == {"ALPH/USDT"}


def test_legacy_dict_roundtrip_preserves_raw_keys():
    """The flat dict handed to legacy strategies matches the original table."""
    raw = legacy_strategy()
    config = parse_app_config(
        {"refresh_speed": 0.01, "exchanges": VENUES, "strategies": [raw]}
    )
    flat = config.strategies[0].to_legacy_dict(config.refresh_speed)
    expected = dict(raw, refresh_speed=0.01)
    assert flat == expected


def test_new_strategy_shape():
    """Explicit subscriptions and params are parsed and exposed."""
    config = parse_app_config(
        {"venues": VENUES, "strategies": [new_strategy()]}
    )
    strategy = config.strategies[0]
    assert strategy.legacy is False
    assert strategy.params == {"lag_ms": 400, "threshold": 0.002}
    assert strategy.subscriptions[0].feeds == ("book", "trade")
    assert strategy.subscriptions[1].feeds == ("book",)
    assert config.symbols() == {"BTC/USDT", "SOL/USDT"}
    flat = strategy.to_legacy_dict(None)
    assert flat["lag_ms"] == 400
    assert flat["subscriptions"][0]["venue"] == "gate"
    assert "refresh_speed" not in flat


def test_defaults_for_redis_and_market_data():
    """Missing tables fall back to defaults; explicit values override."""
    config = parse_app_config({"venues": VENUES, "strategies": [new_strategy()]})
    assert config.redis.host == "localhost"
    assert config.redis.port == 6379
    assert config.market_data.book_depth == 20

    config = parse_app_config(
        {
            "redis": {"host": "redis.internal", "port": 6380},
            "market_data": {"book_depth": 5},
            "venues": VENUES,
            "strategies": [new_strategy()],
        }
    )
    assert config.redis.host == "redis.internal"
    assert config.redis.port == 6380
    assert config.market_data.book_depth == 5
    assert config.market_data.stream_maxlen == 10_000


def test_production_filter():
    """Filtering by production flag selects the right strategies."""
    config = parse_app_config(
        {
            "venues": VENUES,
            "strategies": [
                new_strategy(identifier="a", production=True),
                new_strategy(identifier="b", production=False),
            ],
        }
    )
    assert [s.identifier for s in config.active_strategies(True)] == ["a"]
    assert [s.identifier for s in config.active_strategies(False)] == ["b"]
    assert len(config.active_strategies()) == 2


def test_duplicate_identifier_within_group_rejected():
    """Two strategies with the same identifier and production flag fail."""
    with pytest.raises(ConfigError, match="Duplicate"):
        parse_app_config(
            {
                "venues": VENUES,
                "strategies": [new_strategy(), new_strategy()],
            }
        )


def test_duplicate_identifier_across_groups_allowed():
    """The same identifier may be reused between production and test."""
    config = parse_app_config(
        {
            "venues": VENUES,
            "strategies": [new_strategy(production=True), new_strategy(production=False)],
        }
    )
    assert len(config.strategies) == 2


def test_undeclared_venue_rejected():
    """Subscribing to a venue that is not declared fails."""
    with pytest.raises(ConfigError, match="undeclared venue"):
        parse_app_config(
            {
                "venues": VENUES,
                "strategies": [
                    new_strategy(subscriptions=[{"venue": "binance", "symbol": "X/Y"}])
                ],
            }
        )


def test_legacy_requires_refresh_speed():
    """Legacy strategies need a polling interval."""
    with pytest.raises(ConfigError, match="refresh_speed"):
        parse_app_config({"exchanges": VENUES, "strategies": [legacy_strategy()]})


def test_missing_required_key_rejected():
    """A strategy without a type fails loudly."""
    raw = new_strategy()
    del raw["type"]
    with pytest.raises(ConfigError, match="type"):
        parse_app_config({"venues": VENUES, "strategies": [raw]})


def test_no_strategies_rejected():
    """An empty strategies list fails."""
    with pytest.raises(ConfigError, match="No strategy"):
        parse_app_config({"venues": VENUES, "strategies": []})


def test_load_from_file(tmp_path):
    """A TOML file in the target shape loads end to end."""
    toml = """
refresh_speed = 0.01

[redis]
host = "127.0.0.1"

[[venues]]
id = "gate"
name = "Gate.io"

[[strategies]]
identifier = "x"
type = "demo"
production = false
subscriptions = [{ venue = "gate", symbol = "BTC/USDT" }]

[strategies.params]
threshold = 0.5
"""
    path = tmp_path / "config.toml"
    path.write_text(toml)
    config = load_app_config(path)
    assert config.redis.host == "127.0.0.1"
    assert config.strategies[0].params == {"threshold": 0.5}


def test_missing_file(tmp_path):
    """A missing config file raises ConfigError."""
    with pytest.raises(ConfigError, match="not found"):
        load_app_config(tmp_path / "nope.toml")


def test_real_config_loads():
    """The checked-in config in the strategies submodule parses."""
    config = load_app_config()
    assert config.venue_ids >= {"mexc", "bitget"}
    assert config.strategies
    assert not any(s.legacy for s in config.strategies)
