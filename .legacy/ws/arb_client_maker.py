import ccxt.pro as ccxt

def arb_client_maker(strategy, maker_client_id, maker_client_index):
    """This helper function takes in a strategy, a client id, and it's index, and returns two
    async ccxt clients."""
    taker_client = getattr(ccxt, strategy['taker_exchange']['id'])({'apiKey': strategy['taker_exchange']['key'],
                                                                    'secret': strategy['taker_exchange']['secret']})
    maker_client_test = getattr(ccxt, maker_client_id)()

    if maker_client_test.requiredCredentials['password']:
        maker_client = getattr(ccxt, maker_client_id)({'apiKey': strategy['maker_exchanges'][maker_client_index]['key'],
                                                       'secret': strategy['maker_exchanges'][maker_client_index][
                                                           'secret'],
                                                       'password': strategy['maker_exchanges'][maker_client_index][
                                                           'password']})
    else:
        maker_client = getattr(ccxt, maker_client_id)({'apiKey': strategy['maker_exchanges'][maker_client_index]['key'],
                                                       'secret': strategy['maker_exchanges'][maker_client_index][
                                                           'secret']})

    return taker_client, maker_client