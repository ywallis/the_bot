import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, call

import pytest

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.strategies.fake_maker import fake_maker
from apps.maker.src.structs import CancellationMessage, OrderMessage

# TODO


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "maker_side_effects, taker_side_effects, expected_messages",
    [
        # Test case 1: Two cancellations, one sell order one buy order
        (
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]}
            ],  # maker OB
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]}
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="t-0_test_strategy_es",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(50500),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="t-0_test_strategy_eb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(49500),
                    amount=Decimal(2),
                ),
            ],
        ),
        # Test case 2: Two cancellations, two sets of orders at different prices because of large price change
        (
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[60000, 1], [59999, 1]], "asks": [[60000, 1], [60001, 1]]},
            ],  # maker OB
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[60000, 1], [59999, 1]], "asks": [[60000, 1], [60001, 1]]},
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="t-0_test_strategy_es",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(50500),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="t-0_test_strategy_eb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(49500),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="t-0_test_strategy_es",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(60600),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="t-0_test_strategy_eb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(59400),
                    amount=Decimal(2),
                ),
            ],
        ),
        # Test case 3: Two cancellations, one set of orders and ignore price change
        (
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[50001, 1], [50000, 1]], "asks": [[50001, 1], [50002, 1]]},
            ],  # maker OB
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[50001, 1], [50000, 1]], "asks": [[50001, 1], [50002, 1]]},
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="t-0_test_strategy_es",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(50500),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="t-0_test_strategy_eb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(49500),
                    amount=Decimal(2),
                ),
            ],
        ),
        # Test case 4: Two cancellations, one set of orders and ignore liquidity change
        (
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
            ],  # maker OB
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {
                    "bids": [[50000, 1.01], [49999, 1]],
                    "asks": [[50000, 1.01], [50001, 1]],
                },
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="t-0_test_strategy_es",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(50500),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="t-0_test_strategy_eb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(49500),
                    amount=Decimal(2),
                ),
            ],
        ),
        # Test case 5: Two cancellations, 2 sets of orders because of liquidity change
        (
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
            ],  # maker OB
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[50000, 2], [49999, 1]], "asks": [[50000, 2], [50001, 1]]},
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="t-0_test_strategy_es",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(50500),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="t-0_test_strategy_eb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(49500),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="t-0_test_strategy_es",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(50500),
                    amount=Decimal(3),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="t-0_test_strategy_eb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(49500),
                    amount=Decimal(3),
                ),
            ],
        ),
        # Test case 6: Two cancellations, a set of orders and 2 cancellations
        (
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[40000, 1], [39999, 1]], "asks": [[60000, 1], [60001, 1]]},
            ],  # maker OB
            [
                {"bids": [[50000, 1], [49999, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[50000, 2], [49999, 1]], "asks": [[50000, 2], [50001, 1]]},
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="t-0_test_strategy_es",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(50500),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="t-0_test_strategy_eb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(49500),
                    amount=Decimal(2),
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
            ],
        ),
        # Test case 7: Two cancellations, no orders because natural spread wider
        (
            [
                {"bids": [[40000, 1], [39999, 1]], "asks": [[60000, 1], [60001, 1]]},
            ],  # maker OB
            [
                {"bids": [[50000, 2], [49999, 1]], "asks": [[50000, 2], [50001, 1]]},
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_es",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategy_eb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
            ],
        ),
    ],
)
async def test_fake_maker(
    base_strategy_fm: dict[str, str],
    maker_side_effects,
    taker_side_effects,
    expected_messages,
):
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    async def retrieve_ob_redis_side_effect(_redis, symbol):
        if "maker" in symbol:
            return maker_side_effects.pop(0)
        if "taker" in symbol:
            return taker_side_effects.pop(0)
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

        # Run function in a background task
        task = asyncio.create_task(fake_maker(mock_redis, base_strategy_fm))
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
        assert mocked_publish.call_count == len(expected_messages)
