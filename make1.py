import ccxt
from config import mexc_maker_key, mexc_maker_secret, gate_maker_key, gate_maker_secret
from boiler import check_and_take, check_if_solvent
from datetime import datetime
import time

gate_take = ccxt.gateio({'apiKey': gate_maker_key, 'secret': gate_maker_secret})
mexc_maker = ccxt.mexc({'apiKey': mexc_maker_key, 'secret': mexc_maker_secret})

# Move to config asap
mexc_maker.rateLimit = 25
maker_size = 15


def make_and_take(taker_client, maker_client, pair, spread):

    buy_exists = False
    sell_exists = False
    buy_order = None
    sell_order = None
    watching = True
    buy_arbitrage = False
    sell_arbitrage = False

    while watching:
        # slow watching if no open order
        if not buy_arbitrage and not sell_arbitrage:
            time.sleep(1)

        ticker_a = taker_client.fetch_ticker(pair)
        ticker_b = maker_client.fetch_ticker(pair)
        last_a = ticker_a['last']
        bid_a = ticker_a['bid']
        ask_a = ticker_a['ask']
        last_b = ticker_b['last']
        bid_b = ticker_b['bid']
        ask_b = ticker_b['ask']

        all_prices = {taker_client.name: last_a, maker_client.name: last_b}
        lowest = min(all_prices, key=all_prices.get)
        highest = max(all_prices, key=all_prices.get)

        watch_spread = round((all_prices[highest] / all_prices[lowest] - 1) * 100, 2)

        print(f'Watching at {datetime.now()}.'
              f'\nLowest price on {lowest} for {all_prices[lowest]}, '
              f'highest on {highest} for {all_prices[highest]} ({watch_spread}%).')

        if ask_b >= ask_a * spread:

            # Introducing parameter for speed control

            buy_arbitrage = True

            if not sell_exists:
                print(f'Make on {maker_client.name}')
                print(f'Sell {ask_b}')

                if check_if_solvent(gate_take, mexc_maker, ask_b, maker_size):
                    sell_order = maker_client.create_limit_sell_order(symbol=pair, amount=maker_size, price=ask_b)
                    sell_exists = True
                else:
                    print('Insufficient funds!')
            else:
                print(f"Sell order already present at {sell_order['price']}")
                # retrieve order
                # market sell any filled
                if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                    sell_exists = False

                elif sell_order['price'] != ask_b:
                    # if not bottom ask cancel

                    print('Order no longer at bottom of asks, cancelling.')

                    maker_client.cancel_order(id=sell_order['id'], symbol=pair)

                    sell_exists = False
        else:

            # Introducing parameter for speed control

            buy_arbitrage = False

            # retrieve order
            # market sell any filled

            if sell_exists:
                if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                    sell_exists = False
                else:
                    maker_client.cancel_order(id=sell_order['id'], symbol=pair)
                    print('No more arb, cancelling sells.')
                    sell_exists = False

        # Initiate second side of market making

        if bid_a >= bid_b * spread:

            # Introducing parameter for speed control

            sell_arbitrage = True

            if not buy_exists:
                print(f'Make on {maker_client.name}')
                print(f'Buy {bid_b}')

                if check_if_solvent(mexc_maker, gate_take, bid_b, maker_size):
                    buy_order = maker_client.create_limit_buy_order(symbol=pair, amount=maker_size, price=bid_b)
                    buy_exists = True
                else:
                    print('Insufficient funds!')
            else:
                print(f'Buy order already present at {buy_order["price"]}')
                # retrieve order
                # market sell any filled
                if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                    buy_exists = False

                elif buy_order['price'] != bid_b:
                    # if not top bid cancel

                    print('Order no longer at top of bids, cancelling.')

                    maker_client.cancel_order(id=buy_order['id'], symbol=pair)

                    buy_exists = False

        else:

            # Introducing parameter for speed control

            sell_arbitrage = False

            # retrieve order
            # market sell any filled
            if buy_exists:
                if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):
                    buy_exists = False
                else:
                    maker_client.cancel_order(id=buy_order['id'], symbol=pair)
                    print('No more arb, cancelling buys.')
                    buy_exists = False


if __name__ == '__main__':

    try:

        make_and_take(gate_take, mexc_maker, 'ALPH/USDT', 1.002)

    except ccxt.NetworkError as e:
        print('Network error')


# try:
#     mexc_maker.cancel_order()
#
# except ccxt.BadRequest:


# mexc_maker.cancel_all_orders('ALPH/USDT')

