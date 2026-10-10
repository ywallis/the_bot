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


@pytest.mark.asyncio
async def test_create_and_return_order_passes_post_only_through():
    """A post-only order reaches CCXT with its unified flag, others without."""
    order = OrderMessage(
        kind=MessageType.ORDER,
        strategy="fmb_es",
        exchange="venue_a",
        id="t-prefix_fmb_es",
        exchange_id="_",
        pair="BASE/QUOTE",
        side=OrderSide.SELL,
        order_type=OrderType.REPLACE,
        price=Decimal("1.5"),
        amount=Decimal("10"),
        post_only=True,
    )
    mock_client = AsyncMock()
    mock_client.create_order.return_value = {"id": "1"}

    await create_and_return_order(order, mock_client)
    params = mock_client.create_order.call_args.kwargs["params"]
    assert params == {"clientOrderId": "t-prefix_fmb_es", "postOnly": True}

    del order["post_only"]
    await create_and_return_order(order, mock_client)
    params = mock_client.create_order.call_args.kwargs["params"]
    assert params == {"clientOrderId": "t-prefix_fmb_es"}


def market_order(side: OrderSide) -> OrderMessage:
    """Return a market hedge priced at the fill it offsets."""
    return OrderMessage(
        kind=MessageType.ORDER,
        strategy="matching",
        exchange="venue_a",
        id="t-prefix_matching",
        exchange_id="_",
        pair="BASE/QUOTE",
        side=side,
        order_type=OrderType.MARKET,
        price=Decimal("0.5"),
        amount=Decimal("10"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "options", "price"),
    [
        (OrderSide.SELL, {}, None),
        (OrderSide.BUY, {}, 0.5),
        (OrderSide.BUY, {"createMarketBuyOrderRequiresPrice": False}, None),
        (
            OrderSide.BUY,
            {"createOrder": {"createMarketBuyOrderRequiresPrice": False}},
            None,
        ),
    ],
)
async def test_market_orders_carry_a_price_only_where_required(side, options, price):
    """A price would turn the order into a cost order, so it goes only where needed."""
    mock_client = AsyncMock()
    mock_client.options = options
    mock_client.create_order.return_value = {"id": "1"}
    await create_and_return_order(market_order(side), mock_client)
    assert mock_client.create_order.call_args.kwargs["price"] == price


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "price"), [(OrderSide.BUY, 0.51), (OrderSide.SELL, 0.49)]
)
async def test_market_orders_go_as_ioc_limits_where_configured(side, price):
    """A venue with marketAsLimitBps gets an IOC limit that far through the price."""
    mock_client = AsyncMock()
    mock_client.options = {"marketAsLimitBps": 200}
    mock_client.create_order.return_value = {"id": "1"}
    await create_and_return_order(market_order(side), mock_client)
    kwargs = mock_client.create_order.call_args.kwargs
    assert kwargs["type"] == "limit"
    assert kwargs["price"] == pytest.approx(price)
    assert kwargs["amount"] == 10.0
    assert kwargs["params"]["timeInForce"] == "IOC"


@pytest.mark.asyncio
async def test_create_and_return_order_passes_reduce_only_through():
    """A reduce-only order reaches CCXT with its unified flag."""
    order = OrderMessage(
        kind=MessageType.ORDER,
        strategy="dh_h",
        exchange="venue_a_perp",
        id="t-prefix_dh_h",
        exchange_id="_",
        pair="BASE/QUOTE:QUOTE",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        price=Decimal("1.5"),
        amount=Decimal("10"),
        reduce_only=True,
    )
    mock_client = AsyncMock()
    mock_client.create_order.return_value = {"id": "1"}

    await create_and_return_order(order, mock_client)
    params = mock_client.create_order.call_args.kwargs["params"]
    assert params == {"clientOrderId": "t-prefix_dh_h", "reduceOnly": True}
