import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

gateio_key = os.getenv('gateio_key')

gateio_secret = os.getenv('gateio_secret')

mexc_key = os.getenv('mexc_key')

mexc_secret = os.getenv('mexc_secret')

bitmart_key = os.getenv('bitmart_key') # Expired

bitmart_secret = os.getenv('bitmart_secret') # Expired

bitmart_UID = os.getenv('bitmart_UID')

coinex_key = os.getenv('coinex_key')

coinex_secret = os.getenv('coinex_secret')

bitget_key = os.getenv('bitget_key')

bitget_secret = os.getenv('bitget_secret')

bitget_password = os.getenv('bitget_password')

EMAIL = os.getenv('EMAIL')

PASSWORD = os.getenv('PASSWORD')

pair = 'ALPH/USDT'

if os.name == 'nt':
    path_to_data = '//B257_NAS/Data/Arb Bot Unified/'

elif os.uname()[1] == 'T490-Ubuntu':
    print('Using Laptop Data!')
    path_to_data = '../Data/'

else:
    path_to_data = '/home/yann/Data/Arb Bot Unified/'

gate_fee = 0
bitget_fee = 0
target = None
choosing = True
options = [0, 1, 2, 3, 4, 5]

while choosing is True:
    try:
        target = int(input('Enter 1 for MEXC, 2 for BitMart, 3 for Coinex, 4 for Bitget. 0 for unified status'))
        if target in options:
            choosing = False
        else:
            print('Invalid input, try again.')
    except ValueError:
        print('Invalid input, try again.')

# Initialize clients & Exchange dependent variables
if target == 0:  # Status

    gate_client = ccxt.gateio({'apiKey': gateio_key, 'secret': gateio_secret})
    mexc_client = ccxt.mexc({'apiKey': mexc_key, 'secret': mexc_secret})
    bitget_client = ccxt.bitget({'apiKey': bitget_key, 'secret': bitget_secret, 'password': bitget_password})
    low_balance_threshold = 1500

if target == 1:  # MEXC

    gate_client = ccxt.gateio({'apiKey': gateio_key, 'secret': gateio_secret})
    mexc_client = ccxt.mexc({'apiKey': mexc_key, 'secret': mexc_secret})

    gate_fee = gate_client.fetch_trading_fee(symbol=pair)['taker']
    instance_config = {'taker_client': gate_client,
              'maker_client': mexc_client,
              'maker_spread': 1.002,
              'maker_size': 80,
              'taker_spread': 1.0035,
              'taker_sizing': 0.8,
              'taker_max_order_size': 50,
              'taker_only': False,
              'spread_extension': 2,
                       }


if target == 2:  # Bitmart
    gate_client = ccxt.gateio({'apiKey': gateio_key, 'secret': gateio_secret})
    bitmart_client = ccxt.bitmart({'apiKey': bitmart_key, 'secret': bitmart_secret, 'uid': bitmart_UID})
    taker_client = gate_client
    maker_client = bitmart_client
    taker_min_spread = 1.006
    taker_sizing = 0.6
    taker_max_order_size = 10
    maker_spread = 1.0055
    maker_size = 10
    maker_client.rateLimit = 500
    low_balance_threshold = 100
    taker_only = True


if target == 3:  # Coinex

    gate_client = ccxt.gateio({'apiKey': gateio_key, 'secret': gateio_secret})
    coinex_client = ccxt.coinex({'apiKey': coinex_key, 'secret': coinex_secret})
    taker_client = gate_client
    maker_client = coinex_client
    taker_min_spread = 1.0035
    taker_sizing = 0.8
    taker_max_order_size = 15
    bitmart_client = None
    # maker_client.rateLimit = 40
    maker_spread = 1.003
    maker_size = 10
    low_balance_threshold = 500
    taker_only = True


if target == 4:  # Bitget

    gate_client = ccxt.gateio({'apiKey': gateio_key, 'secret': gateio_secret})
    bitget_client = ccxt.bitget({'apiKey': bitget_key, 'secret': bitget_secret, 'password': bitget_password})

    gate_fee = gate_client.fetch_trading_fee(symbol=pair)['taker']
    bitget_fee = bitget_client.fetch_trading_fee(symbol=pair)['taker']

    instance_config = {'taker_client': gate_client,
              'maker_client': bitget_client,
              'maker_spread': 1.0025,
              'maker_size': 50,
              'taker_spread': 1.0035,
              'taker_sizing': 0.8,
              'taker_max_order_size': 30,
              'taker_only': False,
              'spread_extension': 1,
                       }

if target == 5: # This is for testing only

    gateio_key_test = os.getenv('gateio_key_test')
    gateio_secret_test = os.getenv('gateio_secret_test')
    mexc_key_test = os.getenv('mexc_key_test')
    mexc_secret_test = os.getenv('mexc_secret_test')
    bitget_fee = None

