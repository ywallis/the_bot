import asyncio
import importlib
import logging
import sys

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.maker.src.constants import REDIS_HOSTNAME, REDIS_PORT
from apps.shared.src.utils import strategies

logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def main(strategy_index: int):
    pool = ConnectionPool(
        host=REDIS_HOSTNAME, port=REDIS_PORT, db=0, max_connections=20
    )
    redis = Redis(decode_responses=True, connection_pool=pool)

    if not strategies:
        raise Exception("Could not find any valid strategy")

    strategy = strategies[strategy_index]  # Get specific strategy config

    function_name = strategy["type"]
    # Dynamically import the module
    module = importlib.import_module(f"apps.maker.src.strategies.{function_name}")

    # Ensure the function exists in the module
    if not hasattr(module, function_name):
        raise AttributeError(
            f"Function '{function_name}' not found in module '{module.__name__}'"
        )

    # Get function reference and execute it
    strategy_function = getattr(module, function_name)
    await strategy_function(redis, strategy)  # Pass the selected strategy


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m launcher <strategy_index>")
        sys.exit(1)

    strategy_index = int(sys.argv[1])

    asyncio.run(main(strategy_index))
