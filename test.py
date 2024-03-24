import time
import ccxt
from datetime import datetime

start = time.time()

symbol = 'ALPH/USDT'
bot_min_spread = 1.008

gate_client = ccxt.gateio()
bitmart_client = ccxt.bitmart()


def get_and_match_books(min_spread):
    gate_orderbook = gate_client.fetch_order_book(symbol=symbol)
    bitmart_orderbook = bitmart_client.fetch_order_book(symbol=symbol)

    gate_bids = gate_orderbook['bids']
    bitmart_bids = bitmart_orderbook['bids']
    gate_asks = gate_orderbook['asks']
    bitmart_asks = bitmart_orderbook['asks']

    gate_highest_bid = gate_bids[0][0]
    gate_lowest_ask = gate_asks[0][0]
    bitmart_highest_bid = bitmart_bids[0][0]
    bitmart_lowest_ask = bitmart_asks[0][0]

    all_bids = {'bitmart': bitmart_highest_bid, 'gate': gate_highest_bid}
    all_asks = {'bitmart': bitmart_lowest_ask, 'gate': gate_lowest_ask}

    highest_bid = max(all_bids, key=all_bids.get)
    lowest_ask = min(all_asks, key=all_asks.get)

    spread = round((all_bids[highest_bid] / all_asks[lowest_ask] - 1) * 100, 2)


    print(highest_bid)
    print(lowest_ask)
    print(spread)

#
# while True:
#     get_and_match_books()
#     time.sleep(2)

end = time.time()
print(end - start, 's')











#
#
#
# def get_books():
#     gate_orderbook = gate_client.fetch_order_book(symbol=symbol)
#     bitmart_orderbook = bitmart_client.fetch_order_book(symbol=symbol)
#
#     gate_bids = gate_orderbook['bids']
#     bitmart_bids = bitmart_orderbook['bids']
#     gate_asks = gate_orderbook['asks']
#     bitmart_asks = bitmart_orderbook['asks']
#
#     gate_highest_bid = gate_bids[0][0]
#     gate_lowest_ask = gate_asks[0][0]
#     bitmart_highest_bid = bitmart_bids[0][0]
#     bitmart_lowest_ask = bitmart_asks[0][0]
#
#     all_bids = {'bitmart': bitmart_highest_bid, 'gate': gate_highest_bid}
#     all_asks = {'bitmart': bitmart_lowest_ask, 'gate': gate_lowest_ask}
#
#     highest_bid = max(all_bids, key=all_bids.get)
#     lowest_ask = min(all_asks, key=all_asks.get)
#
#     spread = round((all_bids[highest_bid] / all_asks[lowest_ask] - 1) * 100, 2)
#
#     print(highest_bid)
#     print(lowest_ask)
#     print(spread)





#
# def overlord(pair):
#
#     gate_price = gate_client.fetch_ticker(symbol=symbol)['last']
#     bitmart_price = bitmart_client.fetch_ticker(symbol=symbol)['last']
#
#     all_prices = {'bitmart': bitmart_price, 'gate': gate_price}
#
#     lowest = min(all_prices, key=all_prices.get)
#     highest = max(all_prices, key=all_prices.get)
#
#     spread = round((all_prices[highest] / all_prices[lowest] - 1) * 100, 2)
#
#     print(f'Watching at {datetime.now()}.'
#           f'\nLowest price on {lowest} for {all_prices[lowest]}, '
#           f'highest on {highest} for {all_prices[highest]} ({spread}%).')
#
#     return all_prices, lowest, highest
#
#
# while True:
#     all_prices, exchange_with_lowest_price, exchange_with_highest_price = overlord(pair=symbol)
#     time.sleep(2)
#     print(all_prices, exchange_with_lowest_price, exchange_with_highest_price)
