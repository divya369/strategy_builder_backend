"""
Config-based Rate Limit Middleware.

Applies rate limits defined in .env / Settings to all incoming requests
based on route-prefix matching. Automatically skips endpoints that are
already handled by the decorator-based `rate_limit()` dependency.

Middleware order in main.py (outermost runs first):
    1. RateLimitMiddleware   ← this file (checks rate limits)
    2. APIKeyMiddleware      (checks API key)
    3. CORSMiddleware        (handles CORS)

Standard response headers added to every response:
    X-RateLimit-Limit      — max allowed requests in the window
    X-RateLimit-Remaining  — requests remaining in the current window
    X-RateLimit-Reset      — Unix timestamp when the window resets
"""

import time
import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.core.config import settings
from app.core.rate_limiter import limiter

logger = logging.getLogger(__name__)

# ── Paths to bypass (no rate limiting) ────────────────────────────────────────
BYPASS_PATHS = frozenset({"/docs", "/redoc", "/openapi.json"})
# Suffixes that should bypass rate limiting (e.g. broker postbacks)
BYPASS_SUFFIXES = ("/publisher/postback",)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Starlette middleware that enforces config-based rate limits.

    Priority:
      1. Route-specific config limits (e.g., RATE_LIMIT_AUTH_REQUESTS for /api/v1/auth/*)
      2. Global default limit (RATE_LIMIT_REQUESTS / RATE_LIMIT_WINDOW_SECONDS)

    If the endpoint already has a `rate_limit()` decorator (Depends),
    the middleware detects `request.state.rate_limit_handled` and skips
    its own check — only injecting headers from the decorator's result.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        # ── Disabled? Pass through ────────────────────────────────────────────
        if not settings.RATE_LIMIT_ENABLED:
            return await call_next(request)

        # ── Bypass OPTIONS, documentation paths, and broker postbacks ─────────
        if request.method == "OPTIONS" or request.url.path in BYPASS_PATHS:
            return await call_next(request)
        if any(request.url.path.endswith(s) for s in BYPASS_SUFFIXES):
            return await call_next(request)

        # ── Extract client IP ─────────────────────────────────────────────────
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"

        # ── Whitelisted IP? Skip rate limiting entirely ───────────────────────
        if client_ip in settings.RATE_LIMIT_WHITELISTED_IPS:
            return await call_next(request)

        # ── Determine limits for this route ───────────────────────────────────
        max_requests, window_seconds = self._get_route_limits(request.url.path)

        # ── Build rate limit key ──────────────────────────────────────────────
        key = f"{client_ip}:{request.url.path}"

        # ── Check rate limit ──────────────────────────────────────────────────
        result = await limiter.check(key, max_requests, window_seconds)

        if not result.allowed:
            retry_after = max(int(result.reset_at - time.time()), 1)
            logger.warning(
                "[RateLimit] 429 for %s on %s — limit %d/%ds exceeded",
                client_ip, request.url.path, max_requests, window_seconds,
            )
            return JSONResponse(
                status_code=429,
                content={
                    "detail": {
                        "error": "Too Many Requests",
                        "message": f"Rate limit exceeded. Max {result.limit} requests per {window_seconds}s.",
                        "retry_after": retry_after,
                    }
                },
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(result.limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(result.reset_at)),
                },
            )

        # ── Store result for downstream use ───────────────────────────────────
        request.state.rate_limit_result = result

        # ── Call the next middleware / route handler ───────────────────────────
        response: Response = await call_next(request)

        # ── If the decorator already handled it, use its result for headers ───
        decorator_result = getattr(request.state, "rate_limit_handled", False)
        if decorator_result:
            # Decorator already injected headers via HTTPException or response
            final_result = getattr(request.state, "rate_limit_result", result)
        else:
            final_result = result

        # ── Inject rate limit headers into every successful response ──────────
        response.headers["X-RateLimit-Limit"] = str(final_result.limit)
        response.headers["X-RateLimit-Remaining"] = str(final_result.remaining)
        response.headers["X-RateLimit-Reset"] = str(int(final_result.reset_at))

        return response

    @staticmethod
    def _get_route_limits(path: str) -> tuple[int, int]:
        """
        Match the request path against configured route-prefix limits.
        Returns (max_requests, window_seconds).

        Matching order:
          1. /api/v1/auth/*      → RATE_LIMIT_AUTH_*
          2. /api/v1/backtests/* → RATE_LIMIT_BACKTESTS_*
          3. /api/v1/screeners/* → RATE_LIMIT_SCREENERS_*
          4. Everything else     → RATE_LIMIT_REQUESTS / RATE_LIMIT_WINDOW_SECONDS
        """
        # Route-prefix → (requests setting, window setting)
        route_limits = [
            ("/api/v1/auth", settings.RATE_LIMIT_AUTH_REQUESTS, settings.RATE_LIMIT_AUTH_WINDOW_SECONDS),
            ("/api/v1/backtests", settings.RATE_LIMIT_BACKTESTS_REQUESTS, settings.RATE_LIMIT_BACKTESTS_WINDOW_SECONDS),
            ("/api/v1/screeners", settings.RATE_LIMIT_SCREENERS_REQUESTS, settings.RATE_LIMIT_SCREENERS_WINDOW_SECONDS),
        ]

        for prefix, max_req, window_sec in route_limits:
            if path.startswith(prefix):
                return max_req, window_sec

        # Global default
        return settings.RATE_LIMIT_REQUESTS, settings.RATE_LIMIT_WINDOW_SECONDS
