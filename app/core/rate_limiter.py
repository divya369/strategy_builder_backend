"""
Universal Rate Limiter — Sliding Window Counter algorithm.

Provides two ways to rate-limit:

1. **Middleware (config-based)** — imported by RateLimitMiddleware,
   applies broad limits per route prefix defined in .env.

2. **Decorator (per-endpoint)** — use as a FastAPI dependency:

       from app.core.rate_limiter import rate_limit

       @router.post("/run", dependencies=[Depends(rate_limit(max_requests=5, window_seconds=60))])
       async def run_backtest(...):
           ...

Storage: Redis (primary, shared across workers) with automatic
         fallback to in-memory (for dev / single-instance setups).
"""

import time
import logging
import asyncio
from dataclasses import dataclass
from typing import Optional

from fastapi import Request, HTTPException

from app.core.config import settings

logger = logging.getLogger(__name__)


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    limit: int
    remaining: int
    reset_at: float  # Unix timestamp when the current window resets


# ── In-Memory Backend ─────────────────────────────────────────────────────────

class InMemoryBackend:
    """
    Thread-safe in-memory sliding window counter.
    Used as fallback when Redis is unavailable.
    Periodically cleans up expired entries.
    """

    def __init__(self):
        self._store: dict[str, list[float]] = {}
        self._lock = asyncio.Lock()
        self._last_cleanup = time.time()
        self._cleanup_interval = 60  # seconds

    async def check(self, key: str, max_requests: int, window_seconds: int) -> RateLimitResult:
        now = time.time()
        window_start = now - window_seconds
        reset_at = now + window_seconds

        async with self._lock:
            # Periodic cleanup of expired keys
            if now - self._last_cleanup > self._cleanup_interval:
                await self._cleanup(window_seconds)
                self._last_cleanup = now

            # Get or create timestamp list for this key
            if key not in self._store:
                self._store[key] = []

            # Remove timestamps outside the current window
            self._store[key] = [ts for ts in self._store[key] if ts > window_start]

            current_count = len(self._store[key])

            if current_count >= max_requests:
                # Find when the oldest request in the window will expire
                if self._store[key]:
                    reset_at = self._store[key][0] + window_seconds
                return RateLimitResult(
                    allowed=False,
                    limit=max_requests,
                    remaining=0,
                    reset_at=reset_at,
                )

            # Record this request
            self._store[key].append(now)

            return RateLimitResult(
                allowed=True,
                limit=max_requests,
                remaining=max_requests - current_count - 1,
                reset_at=reset_at,
            )

    async def _cleanup(self, default_window: int = 60):
        """Remove keys whose timestamps are all expired."""
        cutoff = time.time() - default_window
        expired_keys = [
            k for k, timestamps in self._store.items()
            if not timestamps or all(ts <= cutoff for ts in timestamps)
        ]
        for k in expired_keys:
            del self._store[k]
        if expired_keys:
            logger.debug("[RateLimit] Cleaned up %d expired in-memory keys", len(expired_keys))


# ── Redis Backend ─────────────────────────────────────────────────────────────

class RedisBackend:
    """
    Redis-backed sliding window using INCR + EXPIRE.
    Each window is a single key: rl:{identifier}:{window_bucket}.
    Shared across all Uvicorn workers.
    """

    def __init__(self):
        self._redis = None
        self._connected = False

    async def _get_redis(self):
        """Lazy-connect to Redis."""
        if self._redis is None:
            try:
                import redis.asyncio as aioredis
                self._redis = aioredis.from_url(
                    settings.RATE_LIMIT_REDIS_URL,
                    decode_responses=True,
                    socket_connect_timeout=2,
                )
                # Test connection
                await self._redis.ping()
                self._connected = True
                logger.info("[RateLimit] Connected to Redis at %s", settings.RATE_LIMIT_REDIS_URL)
            except Exception as exc:
                logger.warning("[RateLimit] Redis unavailable (%s), will use in-memory fallback", exc)
                self._redis = None
                self._connected = False
        return self._redis

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def check(self, key: str, max_requests: int, window_seconds: int) -> Optional[RateLimitResult]:
        """
        Returns RateLimitResult if Redis is available, None if Redis is down
        (caller should fall back to in-memory).
        """
        redis_client = await self._get_redis()
        if redis_client is None:
            return None

        try:
            now = time.time()
            # Bucket key: rounds down to the nearest window
            window_bucket = int(now // window_seconds)
            redis_key = f"rl:{key}:{window_bucket}"
            reset_at = (window_bucket + 1) * window_seconds

            # Atomic increment + set expiry
            pipe = redis_client.pipeline()
            pipe.incr(redis_key)
            pipe.expire(redis_key, window_seconds + 1)  # +1s buffer
            results = await pipe.execute()

            current_count = results[0]

            if current_count > max_requests:
                return RateLimitResult(
                    allowed=False,
                    limit=max_requests,
                    remaining=0,
                    reset_at=reset_at,
                )

            return RateLimitResult(
                allowed=True,
                limit=max_requests,
                remaining=max_requests - current_count,
                reset_at=reset_at,
            )

        except Exception as exc:
            logger.warning("[RateLimit] Redis error during check (%s), falling back", exc)
            self._connected = False
            self._redis = None
            return None


# ── Unified Rate Limiter (singleton) ──────────────────────────────────────────

class SlidingWindowRateLimiter:
    """
    Unified rate limiter that tries Redis first, falls back to in-memory.
    Use as a singleton via the module-level `limiter` instance.
    """

    def __init__(self):
        self._redis_backend = RedisBackend()
        self._memory_backend = InMemoryBackend()

    async def check(self, key: str, max_requests: int, window_seconds: int) -> RateLimitResult:
        """Check if a request is allowed under the rate limit."""

        # Try Redis first
        if settings.RATE_LIMIT_REDIS_URL:
            result = await self._redis_backend.check(key, max_requests, window_seconds)
            if result is not None:
                return result

        # Fallback to in-memory
        return await self._memory_backend.check(key, max_requests, window_seconds)

    @property
    def backend_name(self) -> str:
        """Returns which backend is currently active."""
        if self._redis_backend.is_connected:
            return "redis"
        return "in-memory"


# ── Module-level singleton ────────────────────────────────────────────────────
limiter = SlidingWindowRateLimiter()


# ── FastAPI Dependency (Decorator-based approach) ─────────────────────────────

def rate_limit(max_requests: int = 10, window_seconds: int = 60):
    """
    FastAPI dependency for per-endpoint rate limiting.

    Usage:
        from fastapi import Depends
        from app.core.rate_limiter import rate_limit

        @router.post("/expensive-op", dependencies=[Depends(rate_limit(max_requests=5, window_seconds=60))])
        async def expensive_op():
            ...

        # Or inject into a single endpoint parameter:
        @router.get("/data")
        async def get_data(_rl=Depends(rate_limit(max_requests=20, window_seconds=30))):
            ...
    """

    async def _rate_limit_dependency(request: Request):
        if not settings.RATE_LIMIT_ENABLED:
            return

        # Extract client IP
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"

        # Whitelisted IP? Skip rate limiting entirely
        if client_ip in settings.RATE_LIMIT_WHITELISTED_IPS:
            return

        # Build a key specific to this endpoint
        route_path = request.url.path
        key = f"{client_ip}:{route_path}"

        result = await limiter.check(key, max_requests, window_seconds)

        # Mark the request so the middleware knows to skip it
        request.state.rate_limit_handled = True
        request.state.rate_limit_result = result

        if not result.allowed:
            retry_after = int(result.reset_at - time.time())
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "Too Many Requests",
                    "message": f"Rate limit exceeded. Max {result.limit} requests per {window_seconds}s.",
                    "retry_after": max(retry_after, 1),
                },
                headers={
                    "Retry-After": str(max(retry_after, 1)),
                    "X-RateLimit-Limit": str(result.limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(result.reset_at)),
                },
            )

    return _rate_limit_dependency
