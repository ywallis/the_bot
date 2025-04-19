import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, call

import pytest

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.strategies.take_take import take_take
from apps.maker.src.structs import OrderBatchMessage, OrderMessage

# TODO


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gate_side_effects, coinbase_side_effects, expected_messages",
    [
        # Test case 1: Two cancellations, one sell order at price 60000
        (
            [
                {"bids": [[59900, 1], [59899, 1]], "asks": [[60000, 1], [60001, 1]]}
            ],  # gate OB
            [
                {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]}
            ],  # coinbase OB
            [
                OrderBatchMessage(
                    type=MessageType.ORDERBATCH,
                    strategy="test_strategy_tt",
                    id="t-0_test_strategy_tt",
                    orders=[
                        OrderMessage(
                            kind=MessageType.ORDER,
                            strategy="test_strategy_tt",
                            exchange="gate",
                            id="t-0_test_strategy_tt",
                            exchange_id="_",
                            pair="BTC/USDT",
                            side=OrderSide.SELL,
                            order_type=OrderType.UNIQUE,
                            price=Decimal(60000),
                            amount=Decimal(2),
                        ),
                        OrderMessage(
                            kind=MessageType.ORDER,
                            strategy="test_strategy_tt",
                            exchange="coinbase",
                            id="t-0_test_strategy_eb",
                            exchange_id="_",
                            pair="BTC/USDT",
                            side=OrderSide.BUY,
                            order_type=OrderType.UNIQUE,
                            price=Decimal(39900),
                            amount=Decimal(2),
                        ),
                    ],
                ),
            ],
        ),
    ],
)
async def test_single_edge_liquidity_order(
    base_strategy_tt: dict[str, str],
    gate_side_effects,
    coinbase_side_effects,
    expected_messages,
):
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    async def retrieve_ob_redis_side_effect(_redis, symbol):
        if "gate" in symbol:
            return gate_side_effects.pop(0)
        if "coinbase" in symbol:
            return coinbase_side_effects.pop(0)
        return None

    retrieve_ob_redis_mock = AsyncMock(side_effect=retrieve_ob_redis_side_effect)

    async def retrieve_balance_redis_side_effect(_redis, _symbol):
        return {"BTC": {"free": 50000}, "USDT": {"free": 500000}}

    retrieve_balance_redis_mock = AsyncMock(
        side_effect=retrieve_balance_redis_side_effect
    )

    def order_time_mock():
        return "0"

    # Patch functions
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "apps.maker.src.strategies.single_edge_liquidity.retrieve_ob_redis",
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

        # Run function in a background task
        task = asyncio.create_task(take_take(mock_redis, base_strategy_tt))
        await asyncio.sleep(0.1)
        task.cancel()

        assert retrieve_ob_redis_mock.call_count > 0

        # Validate all expected messages
        mocked_publish.assert_has_awaits(
            [
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(message), default=str),
                )
                for message in expected_messages
            ]
        )
