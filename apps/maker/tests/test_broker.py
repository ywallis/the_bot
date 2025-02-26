import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from apps.maker.src.errors import BrokerError
from apps.maker.src.structs import CustomExchange
from apps.maker.src.enums import MessageType
from apps.maker.src.broker import process_message
from apps.maker.tests.test_data import order_1, cancellation_1


# Need to create a ccxt response fixture
@pytest.mark.asyncio
async def test_process_message_order():
    results_queue = asyncio.Queue()
    ccxt_client = AsyncMock()  # Mock the exchange client

    # Mock `create_and_return_order` directly
    with patch(
        "apps.maker.src.broker.create_and_return_order",
        new=AsyncMock(return_value="order_created"),
    ):

        await process_message(order_1, results_queue, ccxt_client)
        result = await results_queue.get()

        assert result == (order_1["id"], MessageType.ORDER, "order_created")


@pytest.mark.asyncio
async def test_process_message_cancellation():
    results_queue = asyncio.Queue()
    ccxt_client = AsyncMock()  # Mock the exchange client

    # Mock `create_and_return_order` directly
    with patch(
        "apps.maker.src.broker.cancel_order_return_confirmation",
        new=AsyncMock(return_value="order_cancelled"),
    ):

        await process_message(cancellation_1, results_queue, ccxt_client)
        result = await results_queue.get()

        assert result == (
            cancellation_1["id"],
            MessageType.CANCELLATION,
            "order_cancelled",
        )


@pytest.mark.asyncio
async def test_process_message_unknown_type():
    message = {"id": "789", "kind": "INVALID"}
    results_queue = asyncio.Queue()
    ccxt_client = AsyncMock(spec=CustomExchange)

    await process_message(message, results_queue, ccxt_client)
    result = await results_queue.get()

    assert result[0] == "789"
    assert result[1] == MessageType.ERROR

    expected_error_msg = "[Error 400]: Message of unknown type was allowed through: {'id': '789', 'kind': 'INVALID'}"
    assert str(result[2]) == expected_error_msg
