# Except bad request DONE, check if order['amount'] is correct

# Check if there is a need to except errors as something

# try:
#     maker_client.cancel_order()
#
# except ccxt.BadRequest:
#
#     taker_client.create_market_order()


# TO DO

# Change top bid / bottom ask logic to immediately create a new order instead of waiting for an entire loop

# ----- Done, needs testing

# Rewrite main loop overwatch to possibly include maker1 (only if reliable accounting and performance adequate)

# Rewrite taker-taker main loop into a function to increase readability

# ? Should taker-taker / maker1 clients remain separated

# Gate fee does not belong in boiler - rewrite

# Move maker1 sizing / rate limiting to config

# ---- Done, to be tested, function to move to boiler if successful
