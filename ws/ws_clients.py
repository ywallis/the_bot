import ccxt.pro as ccxt
from config.option_picker import strategy_picker, status_client_picker


strategy = strategy_picker()
pair = strategy['pair']
all_clients = []
maker_clients = []

taker_client = getattr(ccxt, strategy['taker_exchange']['id'])({'apiKey': strategy['taker_exchange']['key'],
                                                                'secret': strategy['taker_exchange']['secret']})
all_clients.append(taker_client)

for exchange in strategy['maker_exchanges']:

    client = getattr(ccxt, exchange['id'])()

    if client.requiredCredentials['password']:
        auth_client = getattr(ccxt, exchange['id'])({'apiKey': exchange['key'],
                                                'secret': exchange['secret'],
                                                'password': exchange['password']})
    else:
        auth_client = getattr(ccxt, exchange['id'])({'apiKey': exchange['key'],
                                                'secret': exchange['secret']})
    all_clients.append(auth_client)
    maker_clients.append(auth_client)

all_clients = status_client_picker(all_clients)