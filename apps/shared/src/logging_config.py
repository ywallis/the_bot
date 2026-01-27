"""
This module provides a setup function for configuring the application's logging.
It sets up a JSON formatter for both console and file handlers.
"""

import logging

from pythonjsonlogger.json import JsonFormatter


def setup_logging(log_file="app.log"):
    """
    Set up the logging configuration.

    Configures the root logger with a JSON formatter, a stream handler (console),
    and a file handler. Sets the logging level to ERROR.

    Parameters
    ----------
    log_file : str, optional
        The path to the log file, by default "app.log".
    """
    logger = logging.getLogger()

    # Prevent duplicate handlers
    if logger.hasHandlers():
        return

    formatter = JsonFormatter(
        "{levelname} {filename} {funcName} {lineno} {asctime} {message} {exc_info}",
        style="{",
    )

    # Console handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.setLevel(logging.ERROR)
