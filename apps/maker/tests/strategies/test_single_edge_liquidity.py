import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, call

import pytest

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.strategies.single_edge_liquidity import single_edge_liquidity
from apps.maker.src.structs import CancellationMessage, OrderMessage

# TODO
# - Clean this shit, make fixtures you can re-use
# - Make tests for all util functions
# - Make a constant for timestamp and make sure it is generated properly with a test


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "maker_side_effects, taker_side_effects, expected_messages",
    [
        # Test case 1: One cancellation, one order at price 60000
        (
            [
                {"bids": [[49900, 1], [49899, 1]], "asks": [[60000, 1], [60001, 1]]}
            ],  # maker OB
            [
                {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]}
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategyes",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategyeb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategyes",
                    exchange="maker",
                    id="t-0_test_strategyes",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(60000),
                    amount=Decimal(2),
                ),
            ],
        ),
        # Test case 2: Two cancellations, two orders at different prices
        (
            [
                {"bids": [[49900, 1], [49899, 1]], "asks": [[60001, 1], [60002, 1]]},
                {"bids": [[49900, 1], [49899, 1]], "asks": [[60002, 1], [60002, 1]]}
            ],  # maker OB
            [
                {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategyes",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategyeb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategyes",
                    exchange="maker",
                    id="t-0_test_strategyes",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(60001),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategyes",
                    exchange="maker",
                    id="t-0_test_strategyes",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.SELL,
                    order_type=OrderType.REPLACE,
                    price=Decimal(60002),
                    amount=Decimal(2),
                ),
            ],
        ),
        # Test case 3: Two cancellations, two buy orders at different prices
        (
            [
                {"bids": [[39900, 1], [39899, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[39899, 1], [39898, 1]], "asks": [[50000, 1], [50001, 1]]}
            ],  # maker OB
            [
                {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
                {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
            ],  # taker OB
            [
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategyes",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                CancellationMessage(
                    kind=MessageType.CANCELLATION,
                    strategy="test_strategyeb",
                    exchange="maker",
                    id="",
                    pair="BTC/USDT",
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategyeb",
                    exchange="maker",
                    id="t-0_test_strategyeb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(39900),
                    amount=Decimal(2),
                ),
                OrderMessage(
                    kind=MessageType.ORDER,
                    strategy="test_strategyeb",
                    exchange="maker",
                    id="t-0_test_strategyeb",
                    exchange_id="_",
                    pair="BTC/USDT",
                    side=OrderSide.BUY,
                    order_type=OrderType.REPLACE,
                    price=Decimal(39899),
                    amount=Decimal(2),
                ),
            ],
        ),
    ],
)
async def test_single_edge_liquidity_order(
    base_strategy: dict[str, str],
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
        task = asyncio.create_task(single_edge_liquidity(mock_redis, base_strategy))
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
async def test_single_edge_liquidity_order_no_replace(base_strategy: dict[str, str]):
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    expected_cancellation_1 = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=f"{base_strategy['identifier']}es",
        exchange=base_strategy["maker_exchange"],
        id="",
        pair=base_strategy["symbol"],
    )

    expected_cancellation_2 = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=f"{base_strategy['identifier']}eb",
        exchange=base_strategy["maker_exchange"],
        id="",
        pair=base_strategy["symbol"],
    )

    expected_sell_order = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{base_strategy['identifier']}es",
        exchange=base_strategy["maker_exchange"],
        id=f"t-0_{base_strategy['identifier']}es",
        exchange_id="_",
        pair=base_strategy["symbol"],
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal(60000),
        amount=Decimal(2),
    )

    expected_buy_order = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{base_strategy['identifier']}eb",
        exchange=base_strategy["maker_exchange"],
        id=f"t-0_{base_strategy['identifier']}eb",
        exchange_id="_",
        pair=base_strategy["symbol"],
        side=OrderSide.BUY,
        order_type=OrderType.REPLACE,
        price=Decimal(40000),
        amount=Decimal(2),
    )

    async def retrieve_balance_redis_side_effect(_redis, symbol):
        if "maker" in symbol:
            return {"BTC": {"free": 50000}, "USDT": {"free": 500000}}
        if "taker" in symbol:
            return {"BTC": {"free": 50000}, "USDT": {"free": 500000}}
        return None

    async def retrieve_ob_redis_side_effect(_redis, symbol):
        if "maker" in symbol:
            return {"bids": [[40000, 1], [39999, 1]], "asks": [[60000, 1], [60001, 1]]}
        if "taker" in symbol:
            return {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]}
        return None

    def order_time_mock():
        return "0"

    retrieve_ob_redis_mock = AsyncMock(side_effect=retrieve_ob_redis_side_effect)
    retrieve_balance_redis_mock = AsyncMock(
        side_effect=retrieve_balance_redis_side_effect
    )

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
        task = asyncio.create_task(single_edge_liquidity(mock_redis, base_strategy))
        await asyncio.sleep(1)  # Allow enough time for the function to run
        task.cancel()

        # Ensure the function was actually called
        assert retrieve_ob_redis_mock.call_count > 0, (
            "retrieve_ob_redis should be called"
        )

        mocked_publish.assert_has_awaits(
            [
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_cancellation_1), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_cancellation_2), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_sell_order), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_buy_order), default=str),
                ),
            ]
        )


@pytest.mark.asyncio
async def test_single_edge_liquidity_order_replace_sell(base_strategy: dict[str, str]):
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    expected_cancellation_1 = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=f"{base_strategy['identifier']}es",
        exchange=base_strategy["maker_exchange"],
        id="",
        pair=base_strategy["symbol"],
    )

    expected_cancellation_2 = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=f"{base_strategy['identifier']}eb",
        exchange=base_strategy["maker_exchange"],
        id="",
        pair=base_strategy["symbol"],
    )

    expected_sell_order = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{base_strategy['identifier']}es",
        exchange=base_strategy["maker_exchange"],
        id=f"t-0_{base_strategy['identifier']}es",
        exchange_id="_",
        pair=base_strategy["symbol"],
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal(60000),
        amount=Decimal(2),
    )

    expected_sell_order_2 = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{base_strategy['identifier']}es",
        exchange=base_strategy["maker_exchange"],
        id=f"t-0_{base_strategy['identifier']}es",
        exchange_id="_",
        pair=base_strategy["symbol"],
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal(60001),
        amount=Decimal(2),
    )

    maker_side_effects = [
        {"bids": [[49900, 1], [49899, 1]], "asks": [[60000, 1], [60001, 1]]},
        {"bids": [[49900, 1], [49899, 1]], "asks": [[60001, 1], [60002, 1]]},
    ]
    taker_side_effects = [
        {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
        {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
    ]

    async def retrieve_balance_redis_side_effect(_redis, symbol):
        if "maker" in symbol:
            return {"BTC": {"free": 50000}, "USDT": {"free": 500000}}
        if "taker" in symbol:
            return {"BTC": {"free": 50000}, "USDT": {"free": 500000}}
        return None

    async def retrieve_ob_redis_side_effect(_redis, symbol):
        if "maker" in symbol:
            return maker_side_effects.pop(0)  # Pop for maker symbol calls
        if "taker" in symbol:
            return taker_side_effects.pop(0)  # Pop for taker symbol calls
        return None

    def order_time_mock():
        return "0"

    retrieve_ob_redis_mock = AsyncMock(side_effect=retrieve_ob_redis_side_effect)
    retrieve_balance_redis_mock = AsyncMock(
        side_effect=retrieve_balance_redis_side_effect
    )

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
        task = asyncio.create_task(single_edge_liquidity(mock_redis, base_strategy))
        await asyncio.sleep(1)  # Allow enough time for the function to run
        task.cancel()

        # Ensure the function was actually called
        assert retrieve_ob_redis_mock.call_count > 0, (
            "retrieve_ob_redis should be called"
        )

        mocked_publish.assert_has_awaits(
            [
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_cancellation_1), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_cancellation_2), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_sell_order), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_sell_order_2), default=str),
                ),
            ]
        )


@pytest.mark.asyncio
async def test_single_edge_liquidity_order_replace_buy(base_strategy: dict[str, str]):
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    expected_cancellation_1 = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=f"{base_strategy['identifier']}es",
        exchange=base_strategy["maker_exchange"],
        id="",
        pair=base_strategy["symbol"],
    )

    expected_cancellation_2 = CancellationMessage(
        kind=MessageType.CANCELLATION,
        strategy=f"{base_strategy['identifier']}eb",
        exchange=base_strategy["maker_exchange"],
        id="",
        pair=base_strategy["symbol"],
    )

    expected_buy_order = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{base_strategy['identifier']}eb",
        exchange=base_strategy["maker_exchange"],
        id=f"t-0_{base_strategy['identifier']}eb",
        exchange_id="_",
        pair=base_strategy["symbol"],
        side=OrderSide.BUY,
        order_type=OrderType.REPLACE,
        price=Decimal(40000),
        amount=Decimal(2),
    )

    expected_buy_order_2 = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{base_strategy['identifier']}eb",
        exchange=base_strategy["maker_exchange"],
        id=f"t-0_{base_strategy['identifier']}eb",
        exchange_id="_",
        pair=base_strategy["symbol"],
        side=OrderSide.BUY,
        order_type=OrderType.REPLACE,
        price=Decimal(40001),
        amount=Decimal(2),
    )

    maker_side_effects = [
        {"bids": [[40000, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
        {"bids": [[40001, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
    ]
    taker_side_effects = [
        {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
        {"bids": [[49900, 1], [49899, 1]], "asks": [[50000, 1], [50001, 1]]},
    ]

    async def retrieve_balance_redis_side_effect(_redis, symbol):
        if "maker" in symbol:
            return {"BTC": {"free": 50000}, "USDT": {"free": 500000}}
        if "taker" in symbol:
            return {"BTC": {"free": 50000}, "USDT": {"free": 500000}}
        return None

    async def retrieve_ob_redis_side_effect(_redis, symbol):
        if "maker" in symbol:
            return maker_side_effects.pop(0)  # Pop for maker symbol calls
        if "taker" in symbol:
            return taker_side_effects.pop(0)  # Pop for taker symbol calls
        return None

    def order_time_mock():
        return "0"

    retrieve_ob_redis_mock = AsyncMock(side_effect=retrieve_ob_redis_side_effect)
    retrieve_balance_redis_mock = AsyncMock(
        side_effect=retrieve_balance_redis_side_effect
    )

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
        task = asyncio.create_task(single_edge_liquidity(mock_redis, base_strategy))
        await asyncio.sleep(1)  # Allow enough time for the function to run
        task.cancel()

        # Ensure the function was actually called
        assert retrieve_ob_redis_mock.call_count > 0, (
            "retrieve_ob_redis should be called"
        )

        mocked_publish.assert_has_awaits(
            [
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_cancellation_1), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_cancellation_2), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_buy_order), default=str),
                ),
                call(
                    MESSAGE_PROCESSOR_CHANNEL,
                    json.dumps(dict(expected_buy_order_2), default=str),
                ),
            ]
        )
