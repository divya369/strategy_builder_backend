"""
Broker access token storage via Redis.

Stores the daily Zerodha access token in Redis so the running server
can read it without restart.  Token auto-expires after 24 hours.

Usage:
    from app.core.broker_token_store import save_broker_token, get_broker_token

    save_broker_token("your_token")   # called by daily cron
    token = get_broker_token()        # called by zerodha.py at runtime
"""
from __future__ import annotations
import redis
from app.core.config import settings

_redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

ZERODHA_TOKEN_KEY = "zerodha_access_token"
DEFAULT_TTL = 86400  # 24 hours


def save_broker_token(token: str, ttl: int = DEFAULT_TTL) -> None:
    """Save Zerodha access token to Redis with TTL (default 24h)."""
    _redis_client.set(ZERODHA_TOKEN_KEY, token, ex=ttl)


def get_broker_token() -> str:
    """Read Zerodha access token from Redis. Returns empty string if expired/missing."""
    return _redis_client.get(ZERODHA_TOKEN_KEY) or ""
