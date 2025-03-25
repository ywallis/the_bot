import json
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from redis.asyncio import Redis

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.strategies.utils import (
    check_if_solvent,
    maker_order_sizer,
    min_max_usd_converter,
    order_time,
    send_processor_init_cancellation,
    send_processor_order,
)
from apps.maker.src.structs import CancellationMessage, OrderMessage


@pytest.mark.asyncio
async def test_send_processor_init_cancellation():
    redis_mock = AsyncMock(spec=Redis)
    redis_mock.publish = AsyncMock()
    strategy = {
        "identifier": "test_strategy",
        "maker_exchange": "binance",
        "symbol": "BTC/USDT",
    }

    await send_processor_init_cancellation(redis_mock, strategy)
    expected_message = json.dumps(
        dict(
            CancellationMessage(
                kind=MessageType.CANCELLATION,
                strategy=strategy["identifier"],
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


def test_maker_order_sizer():
    taker_book = [[50001, 1], [50002, 2], [50003, 3]]

    assert maker_order_sizer(50000, taker_book, OrderSide.BUY, 1.0001, 5, 0.5) == 0.5
    assert maker_order_sizer(50000, taker_book, OrderSide.BUY, 1.01, 5, 0.5) == 0.5
