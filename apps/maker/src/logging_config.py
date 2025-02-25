import logging
from pythonjsonlogger.json import JsonFormatter

logger = logging.getLogger()

logHandler = logging.StreamHandler()
formatter = JsonFormatter("{filename}{asctime}{message}{exc_info}", style="{")
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)


import logging
from pythonjsonlogger.json import JsonFormatter

def setup_logging():
    logger = logging.getLogger()

    # Prevent duplicate handlers
    if logger.hasHandlers():
        return

    logHandler = logging.StreamHandler()
    
    formatter = JsonFormatter(
        "{levelname} {filename} {funcName} {lineno} {asctime} {message} {exc_info}", 
        style="{"
    )
    
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)
    logger.setLevel(logging.INFO)


