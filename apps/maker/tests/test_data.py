from decimal import Decimal

from maker.src.structs import CancellationMessage, OrderMessage
from maker.src.enums import MessageType, OrderSide

order_raw: dict[str, str] = {
    "kind": "order",
    "strategy": "ALPH_gate",
    "exchange": "gate",
    "id": "gate_test_1",
    "exchange_id": "_",
    "pair": "ALPH/USDT",
    "side": "sell",
    "price": "100",
    "amount": "10",
} 

# order_1: OrderMessage = {
#     "kind": MessageType.ORDER,
#     "strategy": "ALPH_gate",
#     "exchange": "gate",
#     "id": "gate_test_1",
#     "exchange_id": "_",
#     "pair": "ALPH/USDT",
#     "side": OrderSide.SELL,
#     "price": Decimal(100),
#     "amount": Decimal(10),
# }
#
# order_2: OrderMessage = {
#     "kind": MessageType.ORDER,
#     "strategy": "ALPH_gate",
#     "exchange": "gate",
#     "id": "gate_test_2",
#     "exchange_id": "_",
#     "pair": "ALPH/USDT",
#     "side": OrderSide.SELL,
#     "price": Decimal(100),
#     "amount": Decimal(10),
# }
#
# cancellation_1: CancellationMessage = {
#     "kind": MessageType.CANCELLATION,
#     "strategy": "ALPH_gate",
#     "exchange": "gate",
#     "id": order_2["id"],
#     "pair": "ALPH/USDT",
# }

order_1: OrderMessage = OrderMessage(
    kind=MessageType.ORDER,
    strategy="ALPH_gate",
    exchange="gate",
    id="gate_test_1",
    exchange_id="_",
    pair="ALPH/USDT",
    side=OrderSide.SELL,
    price=Decimal(100),
    amount=Decimal(10),
)
print(order_1)

order_2: OrderMessage = OrderMessage(
    kind=MessageType.ORDER,
    strategy="ALPH_gate",
    exchange="gate",
    id="gate_test_2",
    exchange_id="_",
    pair="ALPH/USDT",
    side=OrderSide.SELL,
    price=Decimal(100),
    amount=Decimal(10),
)

cancellation_1: CancellationMessage = CancellationMessage(
    kind=MessageType.CANCELLATION,
    strategy="ALPH_gate",
    exchange="gate",
    id=order_2["id"],
    pair="ALPH/USDT",
)
