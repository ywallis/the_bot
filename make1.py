import ccxt
from config import gateio_key, gateio_secret, mexc_key, mexc_secret
from datetime import datetime
import time

gate_maker = ccxt.gateio({'apiKey': gateio_key, 'secret': gateio_secret})
mexc_maker = ccxt.mexc({'apiKey': mexc_key, 'secret': mexc_secret})


def overwatch(client_a, client_b, pair, spread):
    buy_exists = False
    sell_exists = False
    watching = True
    while watching:
        time.sleep(1)
        ticker_a = client_a.fetch_ticker(pair)
        ticker_b = client_b.fetch_ticker(pair)
        last_a = ticker_a['last']
        bid_a = ticker_a['bid']
        ask_a = ticker_a['ask']
        last_b = ticker_b['last']
        bid_b = ticker_b['bid']
        ask_b = ticker_b['ask']

        all_prices = {client_a.name: last_a, client_b.name: last_b}
        lowest = min(all_prices, key=all_prices.get)
        highest = max(all_prices, key=all_prices.get)

        watch_spread = round((all_prices[highest] / all_prices[lowest] - 1) * 100, 2)

        print(f'Watching at {datetime.now()}.'
              f'\nLowest price on {lowest} for {all_prices[lowest]}, '
              f'highest on {highest} for {all_prices[highest]} ({watch_spread}%).')

        if ask_b >= ask_a * spread:
            if not sell_exists:
                print(f'Make on {client_b.name}')
                print(f'Sell {ask_b}')
                sell_exists = True
            else:
                print('Sell order already present')
                # retrieve order
                # market sell any filled
                # if not bottom change to bottom ask

        elif bid_a >= bid_b * spread:
            if not buy_exists:
                print(f'Make on {client_b.name}')
                print(f'Buy {bid_b}')
                buy_exists = True
            else:
                print('Buy order already present')
                # retrieve order
                # market sell any filled
                # if not top bid cancel

        else:
            # retrieve order
            # market sell any filled
            print('Cancel all')
            sell_exists = False
            buy_exists = False


# overwatch(gate_maker, mexc_maker, 'ALPH/USDT', 1.0025)

# print(gate_maker.fetch_open_orders('ALPH/USDT'))
#
# print(gate_maker.fetch_order(id='558708798916', symbol='ALPH/USDT'))

order = gate_maker.create_limit_buy_order('ALPH/USDT', 100, 1)
print(order)
time.sleep(5)
print(gate_maker.fetch_order(id=order['id'], symbol='ALPH/USDT')['filled'])
gate_maker.cancel_order(id=order['id'], symbol='ALPH/USDT')
