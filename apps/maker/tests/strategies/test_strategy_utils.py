import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest
from redis.asyncio import Redis

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OidComponent, OrderSide, OrderType
from apps.maker.src.strategies.utils import (
    check_if_solvent,
    generate_oid,
    generate_take_take_order,
    maker_order_sizer,
    min_max_usd_converter,
    ob_matcher,
    order_time,
    send_processor_cancellation,
    send_processor_order,
)
from apps.maker.src.structs import CancellationMessage, OrderBatchMessage, OrderMessage
from apps.maker.src.utils import info_from_oid


def test_info_from_oid():
    oid = generate_oid("la", "es")

    assert info_from_oid(oid, OidComponent.TIME).startswith("2")
    assert info_from_oid(oid, OidComponent.STRATEGY) == "la"
    assert info_from_oid(oid, OidComponent.ORDER) == "es"


@pytest.mark.asyncio
async def test_send_processor_init_cancellation():
    redis_mock = AsyncMock(spec=Redis)
    redis_mock.publish = AsyncMock()
    strategy = {
        "identifier": "test_strategy",
        "maker_exchange": "binance",
        "symbol": "BTC/USDT",
    }
    order_identifier = "es"

    await send_processor_cancellation(redis_mock, strategy, order_identifier)
    expected_message = json.dumps(
        dict(
            CancellationMessage(
                kind=MessageType.CANCELLATION,
                strategy=f"{strategy['identifier']}_{order_identifier}",
                exchange=strategy["maker_exchange"],
                id="",
                pair=strategy["symbol"],
            )
        ),
        default=str,
    )

    redis_mock.publish.assert_called_once_with(
        MESSAGE_PROCESSOR_CHANNEL, expected_message
    )


@pytest.mark.asyncio
async def test_send_processor_order():
    redis_mock = AsyncMock(spec=Redis)
    redis_mock.publish = AsyncMock()
    order = OrderMessage(
        kind=MessageType.ORDER,
        strategy="es",
        exchange="maker_exchange",
        id="es",
        exchange_id="_",
        pair="symbol",
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal(60000),
        amount=Decimal(2),
    )

    await send_processor_order(redis_mock, order)
    expected_message = json.dumps(dict(order), default=str)
    redis_mock.publish.assert_called_once_with(
        MESSAGE_PROCESSOR_CHANNEL, expected_message
    )


def test_min_max_usd_converter():
    min_size, max_size = min_max_usd_converter(50000, 10, 1000)
    assert min_size == 0.0002  # 10 / 50000
    assert max_size == 0.02  # 1000 / 50000


def test_order_time():
    timestamp = order_time()
    assert isinstance(timestamp, str)
    assert len(timestamp) > 10  # Basic format check


@pytest.mark.asyncio
async def test_check_if_solvent_true():
    redis_mock = AsyncMock()

    buy_client_id = "client1"
    sell_client_id = "client2"
    price = 100
    quantity = 2
    pair = "BTC/USDT"

    # Mock balances
    buy_balance = {"USDT": {"free": 1000}}
    sell_balance = {"BTC": {"free": 10}}

    # Mock `redis.get()` correctly
    redis_mock.get = AsyncMock(
        side_effect=[
            json.dumps(buy_balance),  # First call (balance-client1)
            json.dumps(sell_balance),  # Second call (balance-client2)
        ]
    )

    is_solvent = await check_if_solvent(
        redis_mock, buy_client_id, sell_client_id, price, quantity, pair
    )

    assert is_solvent is True  # Expect True because balances are sufficient


@pytest.mark.asyncio
async def test_check_if_solvent_false():
    redis_mock = AsyncMock()

    buy_client_id = "client1"
    sell_client_id = "client2"
    price = 100
    quantity = 2
    pair = "BTC/USDT"

    # Mock balances
    buy_balance = {"USDT": {"free": 0}}
    sell_balance = {"BTC": {"free": 0}}

    # Mock `redis.get()` correctly
    redis_mock.get = AsyncMock(
        side_effect=[
            json.dumps(buy_balance),  # First call (balance-client1)
            json.dumps(sell_balance),  # Second call (balance-client2)
        ]
    )

    is_solvent = await check_if_solvent(
        redis_mock, buy_client_id, sell_client_id, price, quantity, pair
    )

    assert is_solvent is False  # Expect True because balances are sufficient

@pytest.mark.asyncio
async def test_check_if_solvent_false_old_timestamp():
    redis_mock = AsyncMock()

    buy_client_id = "client1"
    sell_client_id = "client2"
    price = 100
    quantity = 2
    pair = "BTC/USDT"
    order_timestamp = 2

    # Mock balances
    buy_balance = {"USDT": {"free": 1000}, "timestamp": 1}
    sell_balance = {"BTC": {"free": 1000}, "timestamp": 1}

    # Mock `redis.get()` correctly
    redis_mock.get = AsyncMock(
        side_effect=[
            json.dumps(buy_balance),  # First call (balance-client1)
            json.dumps(sell_balance),  # Second call (balance-client2)
        ]
    )

    is_solvent = await check_if_solvent(
        redis_mock, buy_client_id, sell_client_id, price, quantity, pair, order_timestamp
    )

    assert is_solvent is False  # Expect True because balances are sufficient


def test_maker_order_sizer():
    taker_book = [[50001, 1], [50002, 2], [50003, 3]]

    assert maker_order_sizer(50000, taker_book, OrderSide.BUY, 1.0001, 5, 0.5) == 0.5
    assert maker_order_sizer(50000, taker_book, OrderSide.BUY, 1.01, 5, 0.5) == 0.5


@pytest.mark.parametrize(
    "bids, asks, spread, sizing, max_size, min_size, extend_spread, expected",
    [
        # Happy path: matching depth 0
        ([[100, 1], [99, 2]], [[90, 1], [91, 2]], 1.1, 1.0, 10, 0.5, 0, (90, 100, 1.0)),
        # Matching at depth 1
        ([[100, 1], [99, 4]], [[95, 1], [90, 5]], 1.05, 1.0, 10, 1.5, 0, (90, 99, 5.0)),
        # Not enough spread (should not match)
        ([[95, 1], [94, 1]], [[94, 1], [93, 1]], 1.2, 1.0, 10, 0.5, 0, None),
        # Sizing too small to meet min_order_size
        (
            [[101, 0.2], [100, 0.3]],
            [[90, 0.2], [89, 0.3]],
            1.1,
            1.0,
            10,
            1.0,
            0,
            None,
        ),
        # Capped by max_order_size
        ([[100, 10]], [[90, 10]], 1.05, 1.0, 5.0, 1.0, 0, (90, 100, 5.0)),
        # extend_spread shifts target price
        (
            [[101, 1], [100, 1]],
            [[90, 1], [89, 1]],
            1.1,
            1.0,
            10,
            0.5,
            1,
            (89, 100, 1.0),
        ),
        # extend_spread too large, out of bounds
        ([[100, 1]], [[90, 1]], 1.1, 1.0, 10, 0.5, 1, None),
        # Books too thin to satisfy min
        ([[100, 0.1]], [[90, 0.1]], 1.1, 1.0, 10, 0.5, 0, None),
    ],
)
def test_ob_matcher(
    bids, asks, spread, sizing, max_size, min_size, extend_spread, expected
):
    result = ob_matcher(bids, asks, spread, sizing, max_size, min_size, extend_spread)
    assert result == expected


@pytest.mark.asyncio
async def test_generate_take_take_order():
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    strategy = {}
    strategy["identifier"] = "test"
    identifier = "tt"
    buy_exchange = "coinbase"
    sell_exchange = "binance"
    common_id = "t-202512-test-tt"
    pair = "BTC/USDT"
    buy_price = 100
    sell_price = 110
    amount = 1
    last_order_timestamp = 0

    expected_buy_order = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{strategy['identifier']}_{identifier}",
        exchange=buy_exchange,
        id=common_id,
        exchange_id="_",
        pair=pair,
        side=OrderSide.BUY,
        order_type=OrderType.UNIQUE,
        price=Decimal(buy_price),
        amount=Decimal(amount),
    )
    expected_sell_order = OrderMessage(
        kind=MessageType.ORDER,
        strategy=f"{strategy['identifier']}_{identifier}",
        exchange=sell_exchange,
        id=common_id,
        exchange_id="_",
        pair=pair,
        side=OrderSide.SELL,
        order_type=OrderType.UNIQUE,
        price=Decimal(sell_price),
        amount=Decimal(amount),
    )

    expected_order_batch = OrderBatchMessage(
        kind=MessageType.ORDERBATCH,
        strategy=f"{strategy['identifier']}_{identifier}",
        id=common_id,
        orders=[expected_buy_order, expected_sell_order],
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "apps.maker.src.strategies.utils.check_if_solvent",
            AsyncMock(return_value=True),
        )
        mp.setattr(
            "apps.maker.src.strategies.utils.generate_oid",
            Mock(return_value=common_id),
        )
        # mp.setattr(
        #     "apps.maker.src.strategies.utils.retrieve_balances_redis",
        #     retrieve_balance_redis_mock,
        # )
        task = asyncio.create_task(
            generate_take_take_order(
                mock_redis,
                buy_exchange,
                sell_exchange,
                buy_price,
                sell_price,
                amount,
                pair,
                strategy,
                identifier,
                last_order_timestamp
            )
        )
        await asyncio.sleep(0.1)
        task.cancel()

        assert mocked_publish.call_count > 0

        # Validate all expected messages
        mocked_publish.assert_awaited_with(
            MESSAGE_PROCESSOR_CHANNEL,
            json.dumps(dict(expected_order_batch), default=str),
        )
