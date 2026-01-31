import asyncio
from datetime import datetime, timezone
import json
from decimal import Decimal
from unittest.mock import AsyncMock, call

import pytest

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.strategies.take_take import take_take

# TODO
now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gate_side_effects, coinbase_side_effects, expected_messages",
    [
        # Test case 1: Basic, no inventory compensation
        (
            [
                {
                    "bids": [[59900, 1], [59899, 1]],
                    "asks": [[60000, 1], [60001, 1]],
                    "timestamp": now_ms,
                }
            ],  # gate OB
            [
                {
                    "bids": [[49900, 1], [49899, 1]],
                    "asks": [[50000, 1], [50001, 1]],
                    "timestamp": now_ms,
                }
            ],  # coinbase OB
            [
                {
                    "kind": MessageType.ORDERBATCH,
                    "strategy": "test_strategy_tt",
                    "id": "t-0_test_strategy_tt",
                    "orders": [
                        {
                            "kind": MessageType.ORDER,
                            "strategy": "test_strategy_tt",
                            "exchange": "coinbase",
                            "id": "t-0_test_strategy_tt",
                            "exchange_id": "_",
                            "pair": "BTC/USDT",
                            "side": OrderSide.BUY,
                            "order_type": OrderType.UNIQUE,
                            "price": Decimal(50000),
                            "amount": Decimal(0.8),
                        },
                        {
                            "kind": MessageType.ORDER,
                            "strategy": "test_strategy_tt",
                            "exchange": "gate",
                            "id": "t-0_test_strategy_tt",
                            "exchange_id": "_",
                            "pair": "BTC/USDT",
                            "side": OrderSide.SELL,
                            "order_type": OrderType.UNIQUE,
                            "price": Decimal(59900),
                            "amount": Decimal(0.8),
                        },
                    ],
                },
            ],
        ),
        # Test case 2: Basic, no inventory compensation, e2 driven
        (
            [
                {
                    "bids": [[49900, 1], [49899, 1]],
                    "asks": [[50000, 1], [50001, 1]],
                    "timestamp": now_ms,
                }
            ],  # gate OB
            [
                {
                    "bids": [[59900, 1], [59899, 1]],
                    "asks": [[60000, 1], [60001, 1]],
                    "timestamp": now_ms,
                }
            ],  # coinbase OB
            [
                {
                    "kind": MessageType.ORDERBATCH,
                    "strategy": "test_strategy_tt",
                    "id": "t-0_test_strategy_tt",
                    "orders": [
                        {
                            "kind": MessageType.ORDER,
                            "strategy": "test_strategy_tt",
                            "exchange": "gate",
                            "id": "t-0_test_strategy_tt",
                            "exchange_id": "_",
                            "pair": "BTC/USDT",
                            "side": OrderSide.BUY,
                            "order_type": OrderType.UNIQUE,
                            "price": Decimal(50000),
                            "amount": Decimal(0.8008),
                        },
                        {
                            "kind": MessageType.ORDER,
                            "strategy": "test_strategy_tt",
                            "exchange": "coinbase",
                            "id": "t-0_test_strategy_tt",
                            "exchange_id": "_",
                            "pair": "BTC/USDT",
                            "side": OrderSide.SELL,
                            "order_type": OrderType.UNIQUE,
                            "price": Decimal(59900),
                            "amount": Decimal(0.8),
                        },
                    ],
                },
            ],
        ),
        # Test case 3: No arb
        (
            [
                {
                    "bids": [[49900, 1], [49899, 1]],
                    "asks": [[50000, 1], [50001, 1]],
                    "timestamp": now_ms,
                }
            ],  # gate OB
            [
                {
                    "bids": [[49900, 1], [49899, 1]],
                    "asks": [[50000, 1], [50001, 1]],
                    "timestamp": now_ms,
                }
            ],  # coinbase OB
            [],
        ),
    ],
)
async def test_take_take(
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
        return {"BTC": {"free": 50000}, "USDT": {"free": 500000}, "timestamp": now_ms}

    retrieve_balance_redis_mock = AsyncMock(
        side_effect=retrieve_balance_redis_side_effect
    )

    def order_time_mock():
        return "0"

    # Patch functions
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "apps.maker.src.strategies.take_take.retrieve_ob_redis",
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


@pytest.mark.asyncio
async def test_take_take_throttle(
    base_strategy_tt: dict[str, str],
):
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    gate_side_effects = [
        {
            "bids": [[49900, 1], [49899, 1]],
            "asks": [[50000, 1], [50001, 1]],
            "timestamp": now_ms,
        },
        {
            "bids": [[49900, 1], [49899, 1]],
            "asks": [[50000, 1], [50001, 1]],
            "timestamp": now_ms,
        }
    ]
    # gate OB
    coinbase_side_effects = [
        {
            "bids": [[59900, 1], [59899, 1]],
            "asks": [[60000, 1], [60001, 1]],
            "timestamp": now_ms,
        },
        {
            "bids": [[59900, 1], [59899, 1]],
            "asks": [[60000, 1], [60001, 1]],
            "timestamp": now_ms,
        }
    ]

    async def retrieve_ob_redis_side_effect(_redis, symbol):
        if "gate" in symbol:
            return gate_side_effects.pop(0)
        if "coinbase" in symbol:
            return coinbase_side_effects.pop(0)
        return None

    retrieve_ob_redis_mock = AsyncMock(side_effect=retrieve_ob_redis_side_effect)

    async def retrieve_balance_redis_side_effect(_redis, _symbol):
        return {"BTC": {"free": 50000}, "USDT": {"free": 500000}, "timestamp": now_ms}

    retrieve_balance_redis_mock = AsyncMock(
        side_effect=retrieve_balance_redis_side_effect
    )

    def order_time_mock():
        return "0"

    # Patch functions
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "apps.maker.src.strategies.take_take.retrieve_ob_redis",
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
        mocked_publish.assert_awaited_once()
