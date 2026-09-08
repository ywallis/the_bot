"""Launcher for executing trading strategies.

A strategy module is named by its ``type`` in the config and lives at
``apps.strategies.src.<type>``. It is one of two shapes:

- a module-level ``STRATEGY`` class deriving from ``runtime.Strategy``,
  constructed with the strategy's ``StrategyConfig`` and run on a
  ``Runtime`` that reads its subscribed streams and submits its intents;
- the legacy ``async def <type>(redis, strategy_dict)`` coroutine, which
  polls snapshot keys and speaks the pubsub protocol the legacy bridge
  translates.

Both shapes stay valid; the runtime is an additional way to write a
strategy, not a replacement.

``--replay <run_id>`` runs a runtime strategy against a replayed recording
instead of the live bus: every stream is read and written under
``bt:<run_id>`` and the clock is event time (see ``replayer.py``). A legacy
strategy polls snapshot keys the replayer does not write, so it cannot be
replayed.
"""

import argparse
import asyncio
import importlib
import inspect
import logging
from collections.abc import Callable
from types import ModuleType
from typing import Any, cast

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.config import AppConfig, StrategyConfig, load_app_config
from apps.shared.src.events import backtest_prefix
from apps.shared.src.runtime import Clock, Strategy, run_strategy
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# Name of the class a runtime strategy module exports.
STRATEGY_ATTRIBUTE = "STRATEGY"


def load_strategy_module(strategy: StrategyConfig) -> ModuleType:
    """
    Import the module a strategy's ``type`` names.

    Parameters
    ----------
    strategy : StrategyConfig
        The strategy.

    Returns
    -------
    ModuleType
        The module ``apps.strategies.src.<type>``.
    """
    return importlib.import_module(f"apps.strategies.src.{strategy.type}")


def runtime_strategy_class(module: ModuleType) -> type[Strategy] | None:
    """
    Return the runtime strategy class a module exports, if any.

    Parameters
    ----------
    module : ModuleType
        The strategy module.

    Returns
    -------
    type[Strategy] | None
        The ``STRATEGY`` class, or None for a legacy module.

    Raises
    ------
    TypeError
        If the module exports ``STRATEGY`` but it is not a ``Strategy``
        subclass.
    """
    candidate = getattr(module, STRATEGY_ATTRIBUTE, None)
    if candidate is None:
        return None
    if not (inspect.isclass(candidate) and issubclass(candidate, Strategy)):
        raise TypeError(
            f"{module.__name__}.{STRATEGY_ATTRIBUTE} must be a subclass of "
            f"apps.shared.src.runtime.Strategy, got {candidate!r}"
        )
    return candidate


def legacy_entry_point(module: ModuleType, strategy: StrategyConfig) -> Any:
    """
    Return the legacy coroutine function a module exports.

    Parameters
    ----------
    module : ModuleType
        The strategy module.
    strategy : StrategyConfig
        The strategy, whose ``type`` names the function.

    Returns
    -------
    Any
        The coroutine function ``<type>(redis, strategy_dict)``.

    Raises
    ------
    AttributeError
        If the module exports neither ``STRATEGY`` nor the function.
    """
    if not hasattr(module, strategy.type):
        raise AttributeError(
            f"Module {module.__name__!r} exports neither {STRATEGY_ATTRIBUTE!r} "
            f"nor a function {strategy.type!r}"
        )
    return getattr(module, strategy.type)


def replay_options(run_id: str | None) -> dict[str, Any]:
    """
    Return the ``Runtime`` options for a replay, or none for live trading.

    Parameters
    ----------
    run_id : str | None
        The backtest run id, or None to run against the live bus.

    Returns
    -------
    dict[str, Any]
        Empty live; ``prefix`` and a replay ``clock`` under replay.
    """
    if run_id is None:
        return {}
    return {"prefix": backtest_prefix(run_id), "clock": Clock.replay()}


async def run(
    redis: Redis,
    config: AppConfig,
    strategy: StrategyConfig,
    replay: str | None = None,
) -> None:
    """
    Run one strategy in whichever shape its module has.

    Parameters
    ----------
    redis : Redis
        The Redis client.
    config : AppConfig
        The application configuration.
    strategy : StrategyConfig
        The strategy to run.
    replay : str | None
        A backtest run id to run against instead of the live bus.

    Raises
    ------
    RuntimeError
        If a legacy strategy is asked to run under replay.
    """
    module = load_strategy_module(strategy)
    strategy_class = runtime_strategy_class(module)
    if strategy_class is not None:
        where = f"under {backtest_prefix(replay)}" if replay else "on the runtime"
        logger.info(f"Running {strategy.identifier} ({strategy.type}) {where}")
        construct = cast(Callable[[StrategyConfig], Strategy], strategy_class)
        await run_strategy(
            redis, config, strategy, construct(strategy), **replay_options(replay)
        )
        return
    if replay is not None:
        raise RuntimeError(
            f"{strategy.identifier} ({strategy.type}) is a legacy strategy and "
            "cannot be replayed: it polls snapshot keys the replayer does not write"
        )
    logger.info(f"Running {strategy.identifier} ({strategy.type}) as a legacy strategy")
    entry_point = legacy_entry_point(module, strategy)
    await entry_point(redis, strategy.to_strategy_dict(config.refresh_speed))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parse the command line.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, ``sys.argv[1:]`` if omitted.

    Returns
    -------
    argparse.Namespace
        ``strategy_index`` and ``replay`` (a run id or None).
    """
    parser = argparse.ArgumentParser(description="Run one configured strategy.")
    parser.add_argument(
        "strategy_index",
        type=int,
        help="index among the strategies matching the production flag, in config order",
    )
    parser.add_argument(
        "--replay",
        metavar="RUN_ID",
        default=None,
        help="run against the recording replayed under bt:<RUN_ID> with a replay clock",
    )
    return parser.parse_args(argv)


async def main(strategy_index: int, replay: str | None = None) -> None:
    """
    Execute the strategy at an index in the active strategy list.

    Parameters
    ----------
    strategy_index : int
        The index of the strategy among those matching the production flag,
        in config order, which is the order the orchestrator uses.
    replay : str | None
        A backtest run id to run against instead of the live bus.

    Raises
    ------
    Exception
        If no strategies are configured.
    """
    config = load_app_config()
    strategies = config.active_strategies(production)
    if not strategies:
        raise Exception("Could not find any valid strategy")

    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)
    try:
        await run(redis, config, strategies[strategy_index], replay)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.strategy_index, args.replay))
