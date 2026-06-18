"""
API-key authentication middleware.

Every incoming request must carry the header:
    Authorization: Basic <API_KEY>

If the key is missing or does not match, the request is rejected with 401.
CORS preflight (OPTIONS) requests are allowed through without checking.
"""

import hmac
from app.core.config import settings
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware



class APIKeyMiddleware(BaseHTTPMiddleware):
    """Validates the API key sent by the frontend in every request."""

    async def dispatch(self, request: Request, call_next):
        # Allow CORS preflight requests through without auth
        if request.method == "OPTIONS":
            return await call_next(request)

        # Allow docs / openapi schema through (optional, remove if you want them gated)
        if request.url.path in ("/docs", "/redoc", "/openapi.json"):
            return await call_next(request)

        # Allow Kite Publisher postback through — it has its own security
        # (checksum verification + client_id lock in the service layer)
        if request.url.path.endswith("/publisher/postback"):
            return await call_next(request)

        auth_header: str | None = request.headers.get("Authorization")

        if not auth_header:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing Authorization header"},
            )

        # Expected format: "Basic <api-key>"
        parts = auth_header.split(" ", 1)
        if len(parts) != 2 or parts[0] != "Basic":
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid Authorization format. Expected: Basic <api-key>"},
            )

        provided_key = parts[1]

        # Constant-time comparison to avoid timing attacks
        if not hmac.compare_digest(provided_key, settings.API_KEY):
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid API key"},
            )

        return await call_next(request)
