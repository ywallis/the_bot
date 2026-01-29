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
