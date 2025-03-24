import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from apps.maker.src.structs import CustomExchange
from apps.maker.src.enums import MessageType
from apps.maker.src.broker import (
    process_message,
    worker,
    results_worker,
    redis_subscriber,
)


# Need to create a ccxt response fixture
@pytest.mark.asyncio
async def test_process_message_order(order_1):
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
async def test_process_message_cancellation(cancellation_1):
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


@pytest.mark.asyncio
async def test_worker(order_1):
    queue = asyncio.Queue()
    results_queue = asyncio.Queue()
    ccxt_client = AsyncMock(spec=CustomExchange)

    ccxt_client.name = "mock_exchange"

    await queue.put(order_1)
    await queue.put(None)  # To stop the worker

    worker_task = asyncio.create_task(worker(queue, results_queue, ccxt_client))

    await worker_task  # Ensure it completes

    assert queue.empty()


@pytest.mark.asyncio
async def test_results_worker_order():
    results_queue = asyncio.Queue()
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    await results_queue.put(("123", MessageType.ORDER, "success"))
    await results_queue.put((None, None, None))  # Stop signal

    await results_worker(mock_redis, results_queue)

    mocked_publish.assert_called_once_with("123", f"{MessageType.ORDER}|success")


@pytest.mark.asyncio
async def test_results_worker_cancellation():
    results_queue = asyncio.Queue()
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    await results_queue.put(("234", MessageType.CANCELLATION, "success"))
    await results_queue.put((None, None, None))  # Stop signal

    await results_worker(mock_redis, results_queue)

    mocked_publish.assert_called_once_with(
        "234", f"{MessageType.CANCELLATION}|success"
    )


@pytest.mark.asyncio
async def test_redis_subscriber_valid_message(order_1, order_raw_string):
    # Create a queue for the "binance" exchange.
    queue = asyncio.Queue()
    redis_mock = AsyncMock()
    queues = {order_1["exchange"]: queue}

    # Define a valid message payload.
    valid_message_data = order_raw_string
    valid_message = {"type": "message", "data": valid_message_data}

    # Fake async generator to simulate pubsub.listen().
    async def fake_listen():
        # Yield one valid message and then exit.
        yield valid_message

    # Create a fake pubsub object.
    pubsub = AsyncMock()
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    # Replace listen with our fake async generator.
    pubsub.listen = fake_listen

    # Patch the parse_message function to return a valid dictionary.
    with patch("apps.maker.src.broker.parse_message", return_value=order_1):
        await redis_subscriber(redis_mock, pubsub, queues)

    # Verify that the parsed message was put into the "binance" queue.
    result = await queue.get()
    assert result == order_1

    # Verify that subscribe and unsubscribe were called with the correct channel.
    pubsub.subscribe.assert_awaited_once_with("broker")
    pubsub.unsubscribe.assert_awaited_once_with("broker")
