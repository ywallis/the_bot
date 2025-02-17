# import sys
# import os
#
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
#
# import pytest
# import asyncio
# import json
# from decimal import Decimal
# from unittest.mock import AsyncMock, patch
# from datetime import datetime
# from src.message_processor import MessageProcessor  # Adjust this import
# from src.enums import MessageType, OrderSide
# from src.structs import OrderMessage, CancellationMessage
#
#
# @pytest.fixture
# async def processor():
#     """Fixture to initialize MessageProcessor before each test."""
#     processor = MessageProcessor()
#     yield processor
#     await processor.redis.close()
#     await processor.redis_pubsub.close()
#
#
# @pytest.mark.asyncio
# async def test_process_message_order(processor, mocker):
#     """Test processing an order message."""
#
#     # Mock Redis methods
#     mock_redis = mocker.patch.object(processor.redis, "publish", new_callable=AsyncMock)
#     mock_redis_sub = mocker.patch.object(processor.redis.pubsub(), "listen", new_callable=AsyncMock)
#
#     # Mock order message
#     order_msg = OrderMessage(
#         kind=MessageType.ORDER,
#         strategy="test_strategy",
#         exchange="mexc",
#         id="order_123",
#         pair="BTC/USDT",
#         side=OrderSide.BUY,
#         amount=Decimal(0.1),
#         price=Decimal(50000.0)
#     )
#
#     result = await processor.process_message(order_msg)
#
#     assert "order_123 was processed" in result
#     mock_redis.assert_called_with("broker", json.dumps(dict(order_msg), default=str))
#
#
# # @pytest.mark.asyncio
# # async def test_process_message_cancellation(processor, mocker):
# #     """Test processing a cancellation message."""
# #    
# #     # Mock Redis
# #     mock_redis = mocker.patch.object(processor.redis, "publish", new_callable=AsyncMock)
# #
# #     # Mock order cancellation
# #     cancel_msg = CancellationMessage(
# #         kind=MessageType.CANCELLATION,
# #         strategy="test_strategy",
# #         exchange="mexc",
# #         id="cancel_123",
# #         pair="BTC/USDT",
# #     )
# #
# #     result = await processor.process_message(cancel_msg)
# #
# #     assert "cancel_123 was processed" in result
# #     mock_redis.assert_called_with("broker", json.dumps(dict(cancel_msg), default=str))
# #
# #
# # @pytest.mark.asyncio
# # async def test_queued_message(processor, mocker):
# #     """Test queuing when the strategy lock is in use."""
# #
# #     # Mock Redis
# #     mocker.patch.object(processor.redis, "publish", new_callable=AsyncMock)
# #
# #     # Mock order messages
# #     order_1 = OrderMessage(
# #         kind=MessageType.ORDER,
# #         strategy="test_strategy",
# #         exchange="mexc",
# #         id="order_123",
# #         pair="BTC/USDT",
# #         side=OrderSide.BUY,
# #         amount=Decimal(0.1),
# #         price=Decimal(50000.0)
# #     )
# #     order_2 = OrderMessage(
# #         kind=MessageType.ORDER,
# #         strategy="test_strategy",
# #         exchange="mexc",
# #         id="order_123",
# #         pair="BTC/USDT",
# #         side=OrderSide.BUY,
# #         amount=Decimal(0.1),
# #         price=Decimal(50100.0)
# #     )
# #
# #     # Lock the strategy
# #     lock = processor.get_lock("test_strategy")
# #     await lock.acquire()
# #
# #     # Process first order (should queue)
# #     result = await processor.process_message(order_1)
# #     assert "was queued" in result
# #
# #     # Process second order (should replace the first)
# #     result = await processor.process_message(order_2)
# #     assert "replaced" in result
# #
# #
# # @pytest.mark.asyncio
# # async def test_send_to_broker(processor, mocker):
# #     """Test sending a message to the broker."""
# #    
# #     # Mock Redis publish
# #     mock_redis = mocker.patch.object(processor.redis, "publish", new_callable=AsyncMock)
# #    
# #     # Mock broker response
# #     mock_pubsub = mocker.patch.object(processor.redis.pubsub(), "listen", new_callable=AsyncMock)
# #     mock_pubsub.__aiter__.return_value = [
# #         {"type": "message", "data": b'{"status":"success"}'}
# #     ]
# #
# #     # Mock order message
# #     order_msg = OrderMessage(
# #         kind=MessageType.ORDER,
# #         strategy="test_strategy",
# #         exchange="mexc",
# #         id="order_123",
# #         pair="BTC/USDT",
# #         side=OrderSide.BUY,
# #         amount=Decimal(0.1),
# #         price=Decimal(50000.0)
# #     )
# #
# #     result = await processor.send_to_broker(order_msg)
# #
# #     assert result is True
# #     mock_redis.assert_called_with("broker", json.dumps(dict(order_msg), default=str))
# #
