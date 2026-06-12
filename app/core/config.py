"""
Application configuration.
All settings are loaded from a .env file (via python-dotenv) and can be
overridden by real environment variables.  No secrets are hardcoded.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root  (screener-builder/.env)
_env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_env_path)


class Settings:
    PROJECT_NAME: str = "Screener Backtest Platform"

    # ── Primary App DB (screeners, users, backtest results) ──────────────────
    POSTGRES_USER: str     = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_SERVER: str   = os.getenv("POSTGRES_SERVER", "localhost")
    POSTGRES_PORT: str     = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB: str       = os.getenv("POSTGRES_DB", "screener_backtest_db")

    # ── Equity OHLC DB (read-only; per-symbol tables like "RELIANCE", "NIFTY_500") ──
    # This is the equity_ohlc database that holds all stock/index price history.
    EQUITY_OHLC_DB: str = os.getenv("EQUITY_OHLC_DB", "equity_ohlc")

    # ── Security ──────────────────────────────────────────────────────────────
    SECRET_KEY: str = os.getenv("SECRET_KEY", "")
    API_KEY: str = os.getenv("API_KEY", "")

    # ── CORS ──────────────────────────────────────────────────────────────────
    ALLOWED_ORIGINS: list = os.getenv(
        "ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000,http://localhost:8000"
    ).split(",")

    # ── CSV Data Paths ────────────────────────────────────────────────────────
    # Index constituent CSVs:  one file per index, e.g. "NIFTY 500.csv"
    #   Format: dates as column headers, stock symbols as row values
    INDEX_CSV_DIR: str = os.getenv("INDEX_CSV_DIR", "")

    # Screener CSVs: one file per trading day, e.g. "2024-04-16_screener.csv"
    #   Format: flat CSV with tradingsymbol + all indicator columns
    SCREENER_CSV_DIR: str = os.getenv("SCREENER_CSV_DIR", "")

    # ── Celery / Redis ────────────────────────────────────────────────────
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/2")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/3")
    BACKTEST_STALE_MINUTES: int = int(os.getenv("BACKTEST_STALE_MINUTES", "30"))
    BACKTEST_HEARTBEAT_SECONDS: int = int(os.getenv("BACKTEST_HEARTBEAT_SECONDS", "30"))

    # ── Equity DB column config ───────────────────────────────────────────────
    # Columns expected in every equity_ohlc per-symbol table
    EQUITY_TABLE_DATE_COL: str   = "date"
    EQUITY_TABLE_OPEN_COL: str   = "open"
    EQUITY_TABLE_CLOSE_COL: str  = "close"

    # ── Rate Limiting ─────────────────────────────────────────────────────────
    RATE_LIMIT_ENABLED: bool = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"
    RATE_LIMIT_REDIS_URL: str = os.getenv("RATE_LIMIT_REDIS_URL", "redis://localhost:6379/4")

    # IPs that bypass rate limiting entirely (comma-separated)
    RATE_LIMIT_WHITELISTED_IPS: set = set(
        ip.strip() for ip in os.getenv("RATE_LIMIT_WHITELISTED_IPS", "").split(",") if ip.strip()
    )

    # Global default limits
    RATE_LIMIT_REQUESTS: int = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
    RATE_LIMIT_WINDOW_SECONDS: int = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

    # Per-route-prefix limits (0 = use global default)
    RATE_LIMIT_AUTH_REQUESTS: int = int(os.getenv("RATE_LIMIT_AUTH_REQUESTS", "10"))
    RATE_LIMIT_AUTH_WINDOW_SECONDS: int = int(os.getenv("RATE_LIMIT_AUTH_WINDOW_SECONDS", "60"))
    RATE_LIMIT_BACKTESTS_REQUESTS: int = int(os.getenv("RATE_LIMIT_BACKTESTS_REQUESTS", "20"))
    RATE_LIMIT_BACKTESTS_WINDOW_SECONDS: int = int(os.getenv("RATE_LIMIT_BACKTESTS_WINDOW_SECONDS", "60"))
    RATE_LIMIT_SCREENERS_REQUESTS: int = int(os.getenv("RATE_LIMIT_SCREENERS_REQUESTS", "50"))
    RATE_LIMIT_SCREENERS_WINDOW_SECONDS: int = int(os.getenv("RATE_LIMIT_SCREENERS_WINDOW_SECONDS", "60"))

    # ── Broker API Keys ───────────────────────────────────────────────────
    # Zerodha login credentials (used by daily token refresh cron only)
    ZERODHA_USER_ID: str = os.getenv("ZERODHA_USER_ID", "")
    ZERODHA_PASSWORD: str = os.getenv("ZERODHA_PASSWORD", "")
    ZERODHA_API_KEY: str = os.getenv("ZERODHA_API_KEY", "")
    ZERODHA_API_SECRET: str = os.getenv("ZERODHA_API_SECRET", "")
    ZERODHA_TOTP: str = os.getenv("ZERODHA_TOTP", "")

    # ── Email / Notifications (Resend) ────────────────────────────────────
    RESEND_API_KEY: str = os.getenv("RESEND_API_KEY", "")
    EMAIL_FROM: str = os.getenv("EMAIL_FROM", "EquityCase <noreply@equitycase.com>")
    FRONTEND_BASE_URL: str = os.getenv("FRONTEND_BASE_URL", "https://www.equitycase.com")

    @property
    def sqlalchemy_database_uri(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def equity_ohlc_database_uri(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.EQUITY_OHLC_DB}"
        )


settings = Settings()
