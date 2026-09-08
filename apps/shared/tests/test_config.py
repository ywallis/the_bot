"""Tests for the typed configuration loader."""

import pytest

from apps.shared.src.config import (
    ConfigError,
    Subscription,
    load_app_config,
    parse_app_config,
)

VENUES = [{"id": "gate", "name": "Gate.io"}, {"id": "mexc", "name": "Mexc"}]


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


def test_new_strategy_shape():
    """Explicit subscriptions and params are parsed and exposed."""
    config = parse_app_config({"venues": VENUES, "strategies": [new_strategy()]})
    strategy = config.strategies[0]
    assert strategy.params == {"lag_ms": 400, "threshold": 0.002}
    assert strategy.subscriptions[0].feeds == ("book", "trade")
    assert strategy.subscriptions[1].feeds == ("book",)
    assert config.symbols() == {"BTC/USDT", "SOL/USDT"}
    flat = strategy.to_strategy_dict(None)
    assert flat["lag_ms"] == 400
    assert flat["subscriptions"][0]["venue"] == "gate"
    assert "refresh_speed" not in flat
    assert strategy.to_strategy_dict(0.01)["refresh_speed"] == 0.01


def test_strategy_dict_matches_what_strategies_read():
    """Params are flattened to the top level, as strategies index them."""
    raw = new_strategy(params={"maker_exchange": "mexc", "spread": 1.0025})
    config = parse_app_config({"venues": VENUES, "strategies": [raw]})
    flat = config.strategies[0].to_strategy_dict(0.01)
    assert flat["maker_exchange"] == "mexc"
    assert flat["spread"] == 1.0025
    assert flat["identifier"] == "lat"
    assert "params" not in flat


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
            "strategies": [
                new_strategy(production=True),
                new_strategy(production=False),
            ],
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


def test_missing_subscriptions_rejected():
    """Subscriptions are mandatory; nothing is inferred from parameters."""
    raw = new_strategy()
    del raw["subscriptions"]
    with pytest.raises(ConfigError, match="subscriptions"):
        parse_app_config({"venues": VENUES, "strategies": [raw]})


def test_stray_top_level_key_rejected():
    """A parameter outside [strategies.params] is an error, not dropped."""
    raw = new_strategy(spread=1.0025)
    with pytest.raises(ConfigError, match="unknown keys.*spread"):
        parse_app_config({"venues": VENUES, "strategies": [raw]})


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
    assert all(s.subscriptions for s in config.strategies)


def test_recorder_config_defaults_and_override():
    """The recorder section is optional and fully defaulted."""
    config = parse_app_config({"venues": VENUES, "strategies": [new_strategy()]})
    assert config.recorder.root == "data"
    assert config.recorder.block_ms == 1000
    config = parse_app_config(
        {
            "venues": VENUES,
            "recorder": {"root": "/var/bot/data", "batch": 50},
            "strategies": [new_strategy()],
        }
    )
    assert config.recorder.root == "/var/bot/data"
    assert config.recorder.batch == 50
    assert config.recorder.flush_interval_s == 1.0


def test_unknown_feed_is_rejected():
    """A typo in a feed name fails loudly instead of silently disabling it."""
    bad = new_strategy(
        subscriptions=[{"venue": "gate", "symbol": "BTC/USDT", "feeds": ["books"]}]
    )
    with pytest.raises(ConfigError, match="unknown feeds"):
        parse_app_config({"venues": VENUES, "strategies": [bad]})


def test_feed_pairs_filters_by_feed():
    """feed_pairs returns only the venue/symbol tuples subscribed to a feed."""
    config = parse_app_config({"venues": VENUES, "strategies": [new_strategy()]})
    assert config.feed_pairs("book") == {("gate", "BTC/USDT"), ("mexc", "SOL/USDT")}
    assert config.feed_pairs("trade") == {("gate", "BTC/USDT")}


def test_venue_options_default_and_parsed():
    """Per-venue CCXT options are optional and passed through untouched."""
    venues = [
        {"id": "gate", "name": "Gate.io"},
        {
            "id": "mexc",
            "name": "Mexc",
            "options": {"watchOrderBook": {"checksum": False}},
        },
    ]
    config = parse_app_config({"venues": venues, "strategies": [new_strategy()]})
    by_id = {v.id: v for v in config.venues}
    assert by_id["gate"].options == {}
    assert by_id["mexc"].options == {"watchOrderBook": {"checksum": False}}


def test_oms_settings_default():
    """The order manager runs on defaults unless the config says otherwise."""
    config = parse_app_config({"venues": VENUES, "strategies": [new_strategy()]})
    assert config.oms.consumer == "oms"
    assert config.oms.max_intent_age_s == 5.0
    assert config.oms.stream_maxlen == 10_000


def test_oms_settings_are_overridable():
    """Each order manager setting can be tuned from the config file."""
    config = parse_app_config(
        {
            "venues": VENUES,
            "strategies": [new_strategy()],
            "oms": {"consumer": "oms-b", "max_intent_age_s": 0.5, "batch": 5},
        }
    )
    assert config.oms.consumer == "oms-b"
    assert config.oms.max_intent_age_s == 0.5
    assert config.oms.batch == 5
    assert config.oms.block_ms == 1000


def test_backtest_settings_default_and_override():
    """Fees are per venue and must name declared venues; the rest has defaults."""
    config = parse_app_config({"venues": VENUES, "strategies": [new_strategy()]})
    assert config.backtest.fees == {}
    assert config.backtest.history_s == 60.0
    config = parse_app_config(
        {
            "venues": VENUES,
            "strategies": [new_strategy()],
            "backtest": {"fees": {"gate": {"maker": 0.0, "taker": 0.001}}, "idle_s": 1},
        }
    )
    assert config.backtest.fees["gate"].taker == 0.001
    assert config.backtest.fees["gate"].maker == 0.0
    assert config.backtest.idle_s == 1.0
    with pytest.raises(ConfigError):
        parse_app_config(
            {
                "venues": VENUES,
                "strategies": [new_strategy()],
                "backtest": {"fees": {"nowhere": {"taker": 0.001}}},
            }
        )
