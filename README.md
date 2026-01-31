# The Bot

A BYOS (Bring Your Own Strategy) event-driven liquidity arbitrage framework.

While this code and repository actually run in production, it is not designed for anyone else to use on their own.

Rather, it's a place for me to share my way of working, without raising too much attention about who I am in the battle of the order books.

I realize the choice of Python is an odd one for a trading framework. This choice was made for the following reasons:

- The availability of the CCXT library allows to dramatically reduce the amount of needed API integration.
- Speed of implementation beats raw performance speed for a one-man team.
- Many of my implemented strategies involve trading of venue far away from each other with significant latency. This reduces the relative importance of raw performance in the total time to order.

This does however mean that this is a framework made for small and niche inefficiencies. If you expect to beat Wintermute trading BTC on Binance, you're going to have a hard time.

One thing to note is that although the public section of this framework is in Python, it's microservice-like infrastructure allows to implement strategies in any language.
