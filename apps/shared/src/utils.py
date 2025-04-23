import tomllib
from pathlib import Path


def load_config():
    CONFIG_PATH = Path(__file__).parents[3] / "config" / "config.toml"
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
        return config
