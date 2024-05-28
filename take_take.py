import ccxt
from boiler import retrieve_books, order_book_matcher
from config import *


def take_take(buy_client, sell_client, spread=min_spread, sizing=current_sizing, max_order_size=current_max_order_size):
    bids = retrieve_books(sell_client, side='bids', ticker=current_ticker)
    asks = retrieve_books(buy_client, side='asks', ticker=current_ticker)

    return order_book_matcher(bids, asks, spread, sizing, max_order_size)
    pass
