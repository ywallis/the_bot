"""Backwards-compatible views over the typed configuration.

Everything here is derived from ``apps.shared.src.config``. New code should
import ``load_app_config`` from there instead of these module globals.
"""

from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from apps.shared.src.config import (
    AppConfig,
    load_app_config,
    load_raw_config,
    production_mode,
)


def load_config(path: Path | None = None) -> dict[str, Any]:
    """
    Load the configuration from the TOML file without validation.

    Parameters
    ----------
    path : Path | None
        Path to the TOML file. Defaults to the strategies submodule config.

    Returns
    -------
    dict
        The configuration dictionary.
    """
    return load_raw_config(path)


# Load environment variables
load_dotenv()
production: bool = production_mode()
if production:
    print("WARNING: USING PROD STRATEGIES")

app_config: AppConfig = load_app_config()

refresh_speed: float | None = app_config.refresh_speed

strategies: list[dict[str, Any]] = [
    s.to_strategy_dict(refresh_speed) for s in app_config.active_strategies(production)
]
exchange_and_pair: set[tuple[str, str]] = app_config.venue_symbol_pairs(production)
pairs: set[str] = app_config.symbols(production)
