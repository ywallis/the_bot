import asyncio
from decimal import Decimal
import json
from unittest.mock import AsyncMock

import pytest

from apps.maker.src.constants import MESSAGE_PROCESSOR_CHANNEL
from apps.maker.src.enums import MessageType, OrderSide, OrderType
from apps.maker.src.matcher import watch_orders
from apps.maker.src.structs import CustomExchange, OrderMessage


@pytest.mark.asyncio
async def test_watch_orders_various_order_states():
    # AsyncMock for redis
    redis = AsyncMock()

    # Create a mock CustomExchange client
    client = AsyncMock(spec=CustomExchange)
    client.name = "binance"

    expected_matching_order = OrderMessage(
        kind=MessageType.ORDER,
        strategy="matching",
        exchange="coinbase",
        id="t-prefix_strategy123_suffix",
        exchange_id="_",
        pair="BTC/USDT",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        price=Decimal(50000),
        amount=Decimal("0.01"),
    )
    # Configure the mock's watch_orders method to return a mix of orders
    client.watch_orders.side_effect = [
        [
            # ✅ Valid order
            {
                "id": "order-1",
                "clientOrderId": "t-prefix_strategy123_suffix",
                "symbol": "BTC/USDT",
                "price": "50000",
                "amount": "0.01",
                "filled": "0.01",
                "side": "buy",
                "status": "closed",
            },
            #  Unfilled order
            {
                "id": "order-2",
                "clientOrderId": "t-prefix_strategy123_suffix",
                "symbol": "BTC/USDT",
                "price": "50000",
                "amount": "0.01",
                "filled": "0.0",
                "side": "buy",
                "status": "closed",
            },
            #  Status is open
            {
                "id": "order-3",
                "clientOrderId": "t-prefix_strategy123_suffix",
                "symbol": "BTC/USDT",
                "price": "50000",
                "amount": "0.01",
                "filled": "0.01",
                "side": "buy",
                "status": "open",
            },
            #  Unknown strategy
            {
                "id": "order-4",
                "clientOrderId": "t-prefix_unknownStrategy_suffix",
                "symbol": "BTC/USDT",
                "price": "50000",
                "amount": "0.01",
                "filled": "0.01",
                "side": "buy",
                "status": "closed",
            },
            #  Duplicate valid order
            {
                "id": "order-1",
                "clientOrderId": "t-prefix_strategy123_suffix",
                "symbol": "BTC/USDT",
                "price": "50000",
                "amount": "0.01",
                "filled": "0.01",
                "side": "buy",
                "status": "closed",
            },
        ],
        asyncio.CancelledError("stop test loop"),  # force loop exit
    ]

    should_match = {
        "strategy123": "coinbase"
    }

    # Launch the watcher
    task = asyncio.create_task(watch_orders(redis, client, "BTC/USDT", should_match))

    # Let the watcher run briefly
    await asyncio.sleep(0.1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    # Check publish was only called for the valid, non-duplicate order
    redis.publish.assert_called_once()
    channel, message = redis.publish.call_args[0]
    assert channel == MESSAGE_PROCESSOR_CHANNEL
    assert '"exchange": "coinbase"' in message
    assert message == json.dumps(dict(expected_matching_order), default=str)
