"""Module containing constants for the maker application."""

from apps.shared.src.config import load_app_config

_config = load_app_config()

BROKER_CHANNEL = "broker"
REDIS_HOSTNAME = _config.redis.host
REDIS_PORT = _config.redis.port
