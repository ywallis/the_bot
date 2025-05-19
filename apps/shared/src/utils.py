import os
import tomllib
from pathlib import Path
from dotenv import load_dotenv

# TODO:
# - Separate config for dev and prod


def load_config():
    CONFIG_PATH = Path(__file__).parents[3] / "config" / "config.toml"
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
        return config


# Load environment variables
load_dotenv()
production = os.getenv("TESTING", False) != "True"
if production:
    print("WARNING: USING PROD STRATEGIES")

exchange_and_pair: set[tuple[str, str]] = set()
pairs: set[str] = set()

strategies: list[dict[str, str]] | None = load_config().get("strategies")
if strategies is None:
    raise Exception("No strategy found")
strategies = list(filter(lambda x: x["production"] == production, strategies))
for strategy in strategies:
    exchange_and_pair.add((strategy["exchange_1"], strategy["symbol"]))
    exchange_and_pair.add((strategy["exchange_2"], strategy["symbol"]))
    pairs.add(strategy["symbol"])
