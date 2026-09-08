"""Tests for CCXT abstraction functions."""

from decimal import Decimal
import pytest
from unittest.mock import AsyncMock
from apps.maker.src.enums import MessageType, OrderType, OrderSide
from apps.maker.src.structs import OrderMessage
from apps.maker.src.ccxt_abstractions import create_and_return_order


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "order_kind,expected_type",
    [
        (OrderType.MARKET, "market"),
        (OrderType.UNIQUE, "limit"),
        (OrderType.REPLACE, "limit"),
    ],
)
async def test_create_and_return_order_type(order_kind, expected_type):
    """Test creating an order with different order types."""
    # Mock order

    order = OrderMessage(
        kind=MessageType.ORDER,
        strategy="matching",
        exchange="gate",
        id="t-prefix_strategy123_suffix",
        exchange_id="_",
        pair="BTC/USDT",
        side=OrderSide.BUY,
        order_type=order_kind,
        price=Decimal(50000),
        # amount=Decimal("0.01"),
        amount=Decimal("2.0020"),
    )

    # Mock client
    mock_client = AsyncMock()
    mock_client.create_order.return_value = {"status": "success"}

    result = await create_and_return_order(order, mock_client)

    assert result == {"status": "success"}
    mock_client.create_order.assert_called_once()
    assert mock_client.create_order.call_args.kwargs["type"] == expected_type
