from ccxt.async_support import ExchangeError as ExchangeError

class BrokerError(Exception):
    """A custom exception class for specific error handling."""

    def __init__(self, message: str, code: int = 400):
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self):
        return f"[Error {self.code}]: {self.message}"
