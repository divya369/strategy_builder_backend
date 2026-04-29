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

    # ── Equity DB column config ───────────────────────────────────────────────
    # Columns expected in every equity_ohlc per-symbol table
    EQUITY_TABLE_DATE_COL: str   = "date"
    EQUITY_TABLE_OPEN_COL: str   = "open"
    EQUITY_TABLE_CLOSE_COL: str  = "close"

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
