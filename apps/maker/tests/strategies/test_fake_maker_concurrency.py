import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from apps.maker.src.strategies.fake_maker import fake_maker

@pytest.mark.asyncio
async def test_fake_maker_is_non_blocking():
    """
    Verifies that fake_maker does not block the event loop while waiting for refresh_speed.
    """
    # Setup mocks
    mock_redis = AsyncMock()
    mock_redis.publish = AsyncMock()

    async def retrieve_ob_redis_side_effect(_redis, symbol):
        # Return dummy data to keep the loop running
        return {"bids": [[50000, 1]], "asks": [[50001, 1]]}

    retrieve_ob_redis_mock = AsyncMock(side_effect=retrieve_ob_redis_side_effect)

    async def retrieve_balance_redis_side_effect(_redis, _symbol):
        return {"BTC": {"free": 50000}, "USDT": {"free": 500000}}

    retrieve_balance_redis_mock = AsyncMock(
        side_effect=retrieve_balance_redis_side_effect
    )

    def order_time_mock():
        return "0"

    # Strategy with 1 second sleep
    strategy = {
        "refresh_speed": "1.0",
        "symbol": "BTC/USDT",
        "maker_exchange": "maker",
        "taker_exchange": "taker",
        "spread": "1.01",
        "min_spread": "1.005",
        "min_size_usdt": "10",
        "max_size_usdt": "1000000",
        "liquidity_utilization": "1",
    }

    # Patch functions
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "apps.maker.src.strategies.fake_maker.retrieve_ob_redis",
            retrieve_ob_redis_mock,
        )
        mp.setattr(
            "apps.maker.src.strategies.utils.retrieve_balances_redis",
            retrieve_balance_redis_mock,
        )
        mp.setattr(
            "apps.maker.src.strategies.utils.order_time",
            order_time_mock,
        )
        mp.setattr(
            "apps.maker.src.strategies.fake_maker.send_processor_cancellation",
            AsyncMock(),
        )
        mp.setattr(
            "apps.maker.src.strategies.utils.generate_order_replace",
            AsyncMock(),
        )

        start_time = time.time()

        async def monitor():
            await asyncio.sleep(0.1)
            end_time = time.time()
            return end_time - start_time

        # Start fake_maker
        task = asyncio.create_task(fake_maker(mock_redis, strategy))

        # Start monitor
        elapsed = await monitor()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # If the event loop was blocked by fake_maker (sleeping for 1s),
        # monitor would not have been able to run until that sleep finished.
        # So elapsed would be >= 1.0s.
        # If non-blocking, it should be around 0.1s.
        assert elapsed < 0.5, f"Event loop was blocked! Elapsed: {elapsed:.4f}s"
