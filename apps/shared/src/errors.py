"""
Module re-exporting common exceptions for shared use.

This module aggregates exceptions from `asyncio` and `ccxt` to provide a single import point.
"""

from asyncio.exceptions import CancelledError as CancelledError

from ccxt.async_support import BadRequest as BadRequest
from ccxt.async_support import ExchangeClosedByUser as ExchangeClosedByUser
from ccxt.async_support import ExchangeError as ExchangeError
from ccxt.async_support import InvalidNonce as InvalidNonce
from ccxt.async_support import InvalidOrder as InvalidOrder
from ccxt.async_support import NetworkError as NetworkError
from ccxt.async_support import RequestTimeout as RequestTimeout
from ccxt.async_support import UnsubscribeError as UnsubscribeError
