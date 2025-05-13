import tomllib
from pathlib import Path

# TODO:
# - Separate config for dev and prod

def load_config():
    CONFIG_PATH = Path(__file__).parents[3] / "config" / "config.toml"
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
        return config


exchange_and_pair: set[tuple[str, str]] = set()

strategies = load_config().get("strategies")
if strategies is None:
    raise Exception("No strategy found")
for strategy in strategies:
    exchange_and_pair.add((strategy["exchange_1"], strategy["symbol"]))
    exchange_and_pair.add((strategy["exchange_2"], strategy["symbol"]))
