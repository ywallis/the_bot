"""How long one REST request of an authenticated CCXT client can take.

Kept apart from ``exchange_clients``, which builds every client at import,
so that a process holding no client can still bound a wait on one that
does: the order manager times its wait for the broker's reply from here.
"""

# CCXT's default leaky bucket refills only while requests are queued, so
# after any idle spell the next request still waits a full ``rateLimit``:
# about 50 ms on every order, measured from the trading host on 2026-10-14.
# A rolling window counts what was sent in the last window instead, so an
# idle client sends at once. Its budget is ``window / rateLimit`` requests,
# the leaky bucket's average rate, and a short window keeps a burst at that
# rate too; CCXT's default of 60 s would let a minute's budget out at once.
# The budget must still cover the costliest single endpoint, or that request
# never fits in the window: a market load costs 25 at a 50 ms rateLimit on
# one venue, which a 1 s window (budget 20) refuses forever. Five seconds
# gives a budget of 100 at that rateLimit. An endpoint that costs more still
# never fits, so check the costs a new venue declares against this budget.
RATE_LIMITER = "rollingWindow"
RATE_WINDOW_MS = 5000

# Timeout of one HTTP request, and how many times CCXT repeats a request
# that failed on the network before raising. Venues that need a passphrase
# keep CCXT's defaults, which are shorter on both counts.
CLIENT_TIMEOUT_MS = 30000
CLIENT_RETRIES_ON_FAILURE = 1


def request_bound_s() -> float:
    """
    Return the longest one CCXT REST call of an authenticated client takes.

    A call first waits for the rolling window to have room, which takes at
    most one window while its cost fits the budget. It then sends the
    request once and repeats it ``CLIENT_RETRIES_ON_FAILURE`` times, each
    send bounded by ``CLIENT_TIMEOUT_MS``.

    Returns
    -------
    float
        Seconds.
    """
    sends = 1 + CLIENT_RETRIES_ON_FAILURE
    return (RATE_WINDOW_MS + sends * CLIENT_TIMEOUT_MS) / 1000
