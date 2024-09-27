from config.env_var import *

ALPH = {'pair': 'ALPH/USDT',
        'production': True,
        'taker_exchange': {'id': 'gate',
                           'key': gateio_key,
                           'secret': gateio_secret,
                           },

        'maker_exchanges': [{'id': 'mexc',
                             'key': mexc_key,
                             'secret': mexc_secret,
                             'settings': {'maker_spread': 1.002,
                                          'maker_size': 80,
                                          'taker_spread': 1.0035,
                                          'taker_sizing': 0.8,
                                          'taker_max_order_size': 50,
                                          'taker_only': False,
                                          'spread_extension': 2,
                                          }
                             },
                            {'id': 'bitget',
                             'key': bitget_key,
                             'secret': bitget_secret,
                             'password': bitget_password,
                             'settings':
                                 {'maker_spread': 1.0025,
                                  'maker_size': 50,
                                  'taker_spread': 1.0035,
                                  'taker_sizing': 0.8,
                                  'taker_max_order_size': 30,
                                  'taker_only': False,
                                  'spread_extension': 1}
                             }]}

from config.env_var import *

ALPH_test = {'pair': 'ALPH/USDT',
             'production': False,
             'taker_exchange': {'id': 'gate',
                                'key': gateio_key_test,
                                'secret': gateio_secret_test,
                                },

             'maker_exchanges': [{'id': 'mexc',
                                  'key': mexc_key_test,
                                  'secret': mexc_secret_test,
                                  'settings': {'maker_spread': 1.002,
                                               'maker_size': 10,
                                               'taker_spread': 1.0035,
                                               'taker_sizing': 0.8,
                                               'taker_max_order_size': 10,
                                               'taker_only': False,
                                               'spread_extension': 2,
                                               }
                                  },
                                 {'id': 'bitget',
                                  'key': bitget_key_test,
                                  'secret': bitget_secret_test,
                                  'password': bitget_password_test,
                                  'settings':
                                      {'maker_spread': 1.0025,
                                       'maker_size': 10,
                                       'taker_spread': 1.0035,
                                       'taker_sizing': 0.8,
                                       'taker_max_order_size': 10,
                                       'taker_only': False,
                                       'spread_extension': 1}
                                  }]}
