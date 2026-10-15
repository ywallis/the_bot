"""Module containing errors for the maker application."""


class BrokerError(Exception):
    """Error specifically relating to a custom CCXT order broker."""

    def __init__(self, message: str, code: int = 400):
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self):
        """Represent the error as a string.

        Returns
        -------
        str
            The string representation of the error

        """
        return f"[Error {self.code}]: {self.message}"


class BrokerTimeout(BrokerError):
    """No reply from the broker in time; the request may still have landed."""

    def __init__(self, message: str):
        super().__init__(message, code=504)
