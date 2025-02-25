import pytest
import asyncio
from unittest.mock import AsyncMock
from src.errors import BrokerError
from src.structs import CustomExchange
from src.enums import MessageType
from src.broker import process_message  

@pytest.mark.asyncio
async def test_process_message_order():

    message = {"id": "123", "kind": MessageType.ORDER}
    results_queue = asyncio.Queue()
    ccxt_client = AsyncMock(spec=CustomExchange)

    ccxt_client.create_and_return_order = AsyncMock(return_value="order_created")

    await process_message(message, results_queue, ccxt_client)
    result = await results_queue.get()

    assert result == ("123", MessageType.ORDER, "order_created")
