import ccxt
from config import mexc_maker_key, mexc_maker_secret, gate_maker_key, gate_maker_secret
from boiler import check_and_take, check_if_solvent
from datetime import datetime
import time

gate_take = ccxt.gateio({'apiKey': gate_maker_key, 'secret': gate_maker_secret})
mexc_maker = ccxt.mexc({'apiKey': mexc_maker_key, 'secret': mexc_maker_secret})


def overwatch(taker_client, maker_client, pair, spread):
    buy_exists = False
    sell_exists = False
    watching = True
    while watching:
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
            if not sell_exists:
                print(f'Make on {maker_client.name}')
                print(f'Sell {ask_b}')
                # FICTITIOUS PRICE
                if check_if_solvent(gate_take, mexc_maker, ask_b, 10):
                    sell_order = maker_client.create_limit_sell_order(symbol=pair, amount=10, price=ask_b)
                    sell_exists = True
                else:
                    print('Insufficient funds!')
            else:
                print('Sell order already present')
                # retrieve order
                # market sell any filled
                if check_and_take(taker_client, maker_client, sell_order, pair, 'buy'):
                    sell_exists = False

                # if not top bid cancel

                # if not bottom ask cancel

                elif sell_order['price'] != ask_b:
                    print('Order no longer at top of bids, cancelling.')

                    maker_client.cancel_order(id=sell_order['id'], symbol=pair)

                    sell_exists = False

        elif bid_a >= bid_b * spread:
            if not buy_exists:
                print(f'Make on {maker_client.name}')
                print(f'Buy {bid_b}')

                # FICTITIOUS PRICE
                if check_if_solvent(mexc_maker, gate_take, bid_b, 10):
                    buy_order = maker_client.create_limit_buy_order(symbol=pair, amount=10, price=bid_b)
                    buy_exists = True
                else:
                    print('Insufficient funds!')
            else:
                print('Buy order already present')
                if check_and_take(taker_client, maker_client, buy_order, pair, 'sell'):

                    buy_exists = False

                elif buy_order['price'] != bid_b:
                    # if not top bid cancel

                    print('Order no longer at top of bids, cancelling.')

                    maker_client.cancel_order(id=buy_order['id'], symbol=pair)

                    buy_exists = False

        else:
            # retrieve order
            # market sell any filled
            if buy_exists:
                check_and_take(taker_client, maker_client, buy_order, pair, 'sell')
                buy_exists = False
            if sell_exists:
                check_and_take(taker_client, maker_client, sell_order, pair, 'buy')
                sell_exists = False


overwatch(gate_take, mexc_maker, 'ALPH/USDT', 1.0025)

#
# print(gate_maker.fetch_order(id='558708798916', symbol='ALPH/USDT'))

# order = gate_maker.create_limit_buy_order('ALPH/USDT', 100, 1)
# print(order)
# time.sleep(5)
# print(gate_maker.fetch_order(id=order['id'], symbol='ALPH/USDT')['filled'])
# mexc_maker.cancel_order(id='C02__411012484669935616009', symbol='ALPH/USDT')
# print(mexc_maker.fetch_open_orders('ALPH/USDT'))
# mexc_maker.cancel_all_orders('ALPH/USDT')
