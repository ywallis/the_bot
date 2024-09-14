import ccxt.async_support as ccxt
import asyncio
from make1_async import make_and_take
from config import gateio_key_test, gateio_secret_test, mexc_key_test, mexc_secret_test

gate = ccxt.gateio({'apiKey': gateio_key_test, 'secret': gateio_secret_test})
mexc = ccxt.mexc({'apiKey': mexc_key_test, 'secret': mexc_secret_test})

asyncio.run(make_and_take(gate, mexc, 'ALPH/USDT', 1.002, 10, 1.0035, 0.8, 10, 2))