import ccxt
from boiler import retrieve_books, order_book_matcher


def take_take(buy_client, sell_client, pair, spread, sizing, max_order_size):
    # Only makes sense if always requesting books is too expensive. Likely to change as MM logic improves.
    bids = retrieve_books(sell_client, side='bids', ticker=pair)
    asks = retrieve_books(buy_client, side='asks', ticker=pair)

    return order_book_matcher(bids, asks, spread, sizing, max_order_size)
    pass
