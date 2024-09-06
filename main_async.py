import ccxt.async_support as ccxt
import asyncio
from make1 import make_and_take

gate = ccxt.gateio()
mexc = ccxt.mexc()

asyncio.run(make_and_take(gate, mexc, 'ALPH/USDT', 1.01, 10, 1.01, 10, 10, 0))