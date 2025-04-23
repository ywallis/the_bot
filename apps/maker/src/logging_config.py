import logging

from pythonjsonlogger.json import JsonFormatter


def setup_logging(log_file="app.log"):
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

    logger.setLevel(logging.DEBUG)
