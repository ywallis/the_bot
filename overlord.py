from datetime import datetime
import ccxt
# from boiler import order_book_matcher

pair = 'ALPH/USDT'
gate = ccxt.gateio()
mexc = ccxt.mexc()
bitmart = ccxt.bitmart()


def compare_books(client_a, client_b, pair):
    bid_a = client_a.fetch_ticker(pair)['bid']
    ask_a = client_a.fetch_ticker(pair)['ask']
    bid_b = client_b.fetch_ticker(pair)['bid']
    ask_b = client_b.fetch_ticker(pair)['ask']

    print(bid_a, ask_a, bid_b, ask_b)

    # FAKE!! REMEMBER TO INVERT

    if ask_a > bid_b:
        print(client_a.name)
        return client_a

    if ask_b > bid_a:
        print(client_b.name)
        return client_b


higher = compare_books(gate, mexc, pair)
print(higher.name)

# gate_book = gate.fetch_order_book(symbol=pair, limit=20)
# mexc_book = mexc.fetch_order_book(symbol=pair, limit=20)
# bitmart_book = bitmart.fetch_order_book(symbol=pair, limit=20)
#
# print(gate_book)
# print(mexc_book)
# print(bitmart_book)
#
# for _ in range(4):
#     start = datetime.now()
#     gate.fetch_ticker(pair)
#     mexc.fetch_ticker(pair)
#     bitmart.fetch_ticker(pair)
#     # gate_book = gate.fetch_order_book(symbol=pair, limit=20)
#     # mexc_book = mexc.fetch_order_book(symbol=pair, limit=20)
#     # bitmart_book = bitmart.fetch_order_book(symbol=pair, limit=20)
#     # order_book_matcher(gate_book['bids'], mexc_book['asks'], spread=1)
#     # order_book_matcher(mexc_book['bids'], gate_book['asks'], spread=1)
#     # order_book_matcher(gate_book['bids'], bitmart_book['asks'], spread=1)
#     # order_book_matcher(bitmart_book['bids'], gate_book['asks'], spread=1)
#     end = datetime.now()
#     print(end - start)

# start = datetime.now()
# gate_highest_bid = gate_client.fetch_order_book(symbol='ALPH/USDT')
# print(gate_client.fetch_order_book(symbol='ALPH/USDT'))
# print(gate_highest_bid)
# end = datetime.now()
# print(end-start)
# gate_highest_bid = gate_client.fetch_order_book(symbol='ALPH/USDT')
# double = datetime.now()
# print(double-end)