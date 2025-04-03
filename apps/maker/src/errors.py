from ccxt.async_support import BadRequest as BadRequest
from ccxt.async_support import ExchangeError as ExchangeError
from ccxt.async_support import RequestTimeout as RequestTimeout
from ccxt.async_support import NetworkError as NetworkError 


class BrokerError(Exception):
    """Error specifically relating to a custom CCXT order broker."""

    def __init__(self, message: str, code: int = 400):
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self):
        return f"[Error {self.code}]: {self.message}"
