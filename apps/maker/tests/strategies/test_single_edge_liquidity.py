import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL

from apps.maker.src.strategies.single_edge_liquidity import single_edge_liquidity

# TODO 
# - Define test objects on actual CCXT values
# - Check if redis publish is called with the right values
# - Does min/max converter really need to be mocked?

@pytest.mark.asyncio
async def test_single_edge_liquidity_sell_order():
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish 

    strategy = {
        "symbol": "BTC/USDT",
        "identifier": "test_strategy",
        "maker_exchange": "maker",
        "taker_exchange": "taker",
        "spread": "1.005",
        "min_size_usdt": "10",
        "max_size_usdt": "1000",
    }

    async def retrieve_ob_redis_side_effect(_redis, symbol):
        if "maker" in symbol:
            return {"bids": [[40000, 1], [39999, 1]], "asks": [[60000, 1], [60001, 1]]}
        if "taker" in symbol:
            return {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]}
        return None

    async def check_if_solvent(*_args, **_kwargs):
        return True


    retrieve_ob_redis_mock = AsyncMock(side_effect=retrieve_ob_redis_side_effect)
    check_if_solvent_mock = AsyncMock(side_effect=check_if_solvent)

    # Patch functions
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "apps.maker.src.strategies.single_edge_liquidity.retrieve_ob_redis",
            retrieve_ob_redis_mock,
        )
        mp.setattr(
            "apps.maker.src.strategies.single_edge_liquidity.check_if_solvent",
            check_if_solvent_mock,
        )
        mp.setattr(
            "apps.maker.src.strategies.single_edge_liquidity.maker_order_sizer",
            MagicMock(return_value=0.01),
        )

        # Run function in a background task
        task = asyncio.create_task(single_edge_liquidity(mock_redis, strategy))
        await asyncio.sleep(1)  # Allow enough time for the function to run
        task.cancel()

        
        # Ensure the function was actually called
        assert retrieve_ob_redis_mock.call_count > 0, (
            "retrieve_ob_redis should be called"
        )
        assert check_if_solvent_mock.call_count > 0, "check_if_solvent should be called"
        # assert mock_redis.publish.call_count > 2, "publish should be called"
        # mock_redis.publish.l
        mocked_publish.assert_awaited_with(MESSAGE_PROCESSOR_CHANNEL, '{"kind": "cancellation", "strategy": "test_strategy", "exchange": "maker", "id": "", "pair": "BTC/USDT"}')
