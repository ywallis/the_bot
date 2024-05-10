import ccxt

from make1 import mexc_maker as maker_client
from make1 import gate_take as taker_client

# Except bad request

# try:
#     maker_client.cancel_order()
#
# except ccxt.BadRequest:
#
#     taker_client.create_market_order()


# TO DO

# Change top bid / bottom ask logic to immediately create a new order instead of waiting for an entire loop

# Rewrite main loop overwatch to possibly include maker1 (only if reliable accounting and performance adequate)

# Rewrite taker-taker main loop into a function to increase readability

# ? Should taker-taker / maker1 clients remain separated

# Gate fee does not belong in boiler - rewrite

# Move maker1 sizing / rate limiting to config


