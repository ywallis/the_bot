"""Tests for the broker module."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from ccxt.async_support import RateLimitExceeded

from apps.maker.src import broker
from apps.maker.src.broker import (
    fetch_all_open,
    process_message,
    redis_subscriber,
    results_worker,
    worker,
)
from apps.maker.src.enums import MessageType

from apps.shared.src.structs import CustomExchange


# Need to create a ccxt response fixture
@pytest.mark.asyncio
async def test_process_message_order(order_1):
    """Test processing an order message."""
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
    """Test processing a cancellation message."""
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
    """Test processing a message with an unknown type."""
    message = {"id": "789", "kind": "INVALID"}
    results_queue = asyncio.Queue()
    ccxt_client = AsyncMock(spec=CustomExchange)

    await process_message(message, results_queue, ccxt_client)  # pyright: ignore[]
    result = await results_queue.get()

    assert result[0] == "789"
    assert result[1] == MessageType.ERROR

    expected_error_msg = "[Error 400]: Message of unknown type was allowed through: {'id': '789', 'kind': 'INVALID'}"
    assert str(result[2]) == expected_error_msg


@pytest.mark.asyncio
async def test_process_message_answers_an_unexpected_error(order_1):
    """Any failure is still a reply, so the order manager is not left waiting."""
    results_queue = asyncio.Queue()

    with patch(
        "apps.maker.src.broker.create_and_return_order",
        new=AsyncMock(side_effect=KeyError("id")),
    ):
        await process_message(order_1, results_queue, AsyncMock())
        result = await results_queue.get()

    assert result[0] == order_1["id"]
    assert result[1] == MessageType.ERROR


@pytest.mark.asyncio
async def test_worker(order_1):
    """Test the worker function."""
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
    """Test the results worker with an order result."""
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
    """Test the results worker with a cancellation result."""
    results_queue = asyncio.Queue()
    mock_redis = AsyncMock()
    mocked_publish = AsyncMock()
    mock_redis.publish = mocked_publish

    await results_queue.put(("234", MessageType.CANCELLATION, "success"))
    await results_queue.put((None, None, None))  # Stop signal

    await results_worker(mock_redis, results_queue)

    mocked_publish.assert_called_once_with("234", f"{MessageType.CANCELLATION}|success")


@pytest.mark.asyncio
async def test_redis_subscriber_valid_message(order_1, order_raw_string):
    """Test the redis subscriber with a valid message."""
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


@pytest.mark.asyncio
async def test_wind_down_stops_every_task():
    """The subscriber is cancelled, the workers and results worker drain and exit."""
    from apps.maker.src.broker import results_worker, wind_down, worker

    class Never:
        async def close(self):
            pass

    async def listen_forever():
        await asyncio.Event().wait()

    published: list[tuple[str, str]] = []

    class FakeRedis:
        async def publish(self, channel, message):
            published.append((channel, message))

    worker_queues = {"venue_x": asyncio.Queue()}
    results_queue: asyncio.Queue = asyncio.Queue()
    subscriber = asyncio.create_task(listen_forever())
    results = asyncio.create_task(results_worker(FakeRedis(), results_queue))
    workers = [
        asyncio.create_task(worker(worker_queues["venue_x"], results_queue, Never()))
    ]
    # A result already produced must still be published before the exit.
    await results_queue.put(("t-1_lmb_es", "order", "{'id': 'venue-1'}"))

    await asyncio.wait_for(
        wind_down(subscriber, results, workers, worker_queues, results_queue), timeout=2
    )

    assert subscriber.cancelled()
    assert results.done() and workers[0].done()
    assert published == [("t-1_lmb_es", "order|{'id': 'venue-1'}")]


@pytest.mark.asyncio
async def test_a_repeated_init_does_not_repeat_the_start_up(monkeypatch):
    """A second INIT re-sends open orders without reloading or reconfiguring."""
    init = {"type": "message", "data": "INIT"}

    async def listen():
        yield init
        yield init

    pubsub = AsyncMock()
    pubsub.listen = listen
    redis = AsyncMock()
    load = AsyncMock()
    configure = AsyncMock()
    fetch = AsyncMock(return_value={"orders": []})
    monkeypatch.setattr(broker, "parse_message", lambda _: {"exchange": "INIT"})
    monkeypatch.setattr(broker, "load_clients", load)
    monkeypatch.setattr(broker, "configure_all", configure)
    monkeypatch.setattr(broker, "fetch_all_open", fetch)

    await redis_subscriber(redis, pubsub, {})

    load.assert_awaited_once()
    configure.assert_awaited_once()
    assert fetch.await_count == 2
    assert [c.args[0] for c in redis.publish.await_args_list] == ["INIT", "INIT"]


@pytest.mark.asyncio
async def test_a_rate_limited_fetch_waits_before_asking_again(monkeypatch):
    """A refused open orders fetch is retried after a pause, not at once."""
    client = AsyncMock()
    client.fetch_open_orders.side_effect = [RateLimitExceeded("429"), []]
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(broker, "symbols_per_venue", {"venue_x": {"BASE/QUOTE"}})
    monkeypatch.setattr(broker.asyncio, "sleep", sleep)

    batch = await fetch_all_open({"venue_x": client})

    assert batch["orders"] == []
    assert client.fetch_open_orders.await_count == 2
    assert sleeps == [broker.FETCH_BACKOFF_S]
