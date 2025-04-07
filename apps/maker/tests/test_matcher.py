import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from apps.maker.src.matcher import watch_orders
from apps.maker.src.structs import CustomExchange


@pytest.mark.asyncio
async def test_watch_orders_processes_and_skips_orders():
    mock_client = MagicMock(spec=CustomExchange)
    mock_client.name = "binance"
    ticker = "BTC/USDT"
    should_match = {"strategy123": "coinbase"}

    valid_order = {
        "id": "123",
        "status": "closed",
        "filled": 1,
        "clientOrderId": "t-some_strategy123_oid",
    }
    unfilled_order = {
        "id": "124",
        "status": "open",
        "filled": 0,
        "clientOrderId": "t-time_unrelated_oid",
    }
    unknown_strategy_order = {
        "id": "125",
        "status": "closed",
        "filled": 1,
        "clientOrderId": "t-unknown_strategy_oid",
    }

    # Mock the watch_orders call to return our list, then raise CancelledError to stop the loop
    call_count = 0

    async def fake_watch_orders_once(*_args, **_kwargs):
        nonlocal call_count
        if call_count == 0:
            call_count += 1
            return [valid_order, unfilled_order, unknown_strategy_order]
        else:
            raise asyncio.CancelledError()

    mock_client.watch_orders = AsyncMock(side_effect=fake_watch_orders_once)

    with patch(
        "apps.maker.src.matcher.process_order_update", new_callable=AsyncMock
    ) as mock_process:
        with pytest.raises(asyncio.CancelledError):
            await watch_orders(mock_client, ticker, should_match)

        await asyncio.sleep(0.1)
        # ✅ Ensure only valid order triggered the update
        mock_process.assert_awaited_once()
        args, kwargs = mock_process.call_args
        assert args[0] == "coinbase"
        assert args[1]["id"] == "123"
        assert args[1] == valid_order 
