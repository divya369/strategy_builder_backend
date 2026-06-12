"""
Database connection setup.

Two separate SQLAlchemy engines:
  - `engine`        → screener_backtest_db  (app data: users, screeners, backtest results)
  - `equity_engine` → equity_ohlc           (market data: per-symbol OHLC tables, read-only)

Each has its own connection pool and session factory.
Use `get_db()` for app writes, `get_equity_db()` for OHLC reads.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

# ── App DB (screener_backtest_db) ─────────────────────────────────────────────
engine = create_engine(
    settings.sqlalchemy_database_uri,
    pool_pre_ping=True,   # drop stale connections before using them
    pool_size=10,         # max persistent connections in the pool
    max_overflow=20,      # extra connections allowed under burst load
    pool_timeout=30,      # seconds to wait for a free connection before raising
    pool_recycle=1800,    # recycle connections every 30 min
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ── Equity OHLC DB (equity_ohlc) — read-only ─────────────────────────────────
equity_engine = create_engine(
    settings.equity_ohlc_database_uri,
    pool_pre_ping=True,
    pool_size=5,          # smaller pool — only reads, no writes
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    execution_options={"isolation_level": "AUTOCOMMIT"},  # read-only: no transactions needed
)
EquitySessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=equity_engine)

# ── EquityCase DB (equitycase) — read-only ────────────────────────────────
# The main equitycase product DB holds the users table (with email, name)
# which we query for sending notifications. Read-only from this service.
equitycase_engine = create_engine(
    settings.equitycase_database_uri,
    pool_pre_ping=True,
    pool_size=3,          # minimal — only used for user email lookups
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=1800,
    execution_options={"isolation_level": "AUTOCOMMIT"},
)
EquitycaseSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=equitycase_engine)


# ── FastAPI dependency helpers ────────────────────────────────────────────────

def get_db():
    """Yields a session for the app (screener_backtest_db)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_equity_db():
    """Yields a read-only session for the equity_ohlc DB."""
    db = EquitySessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_equitycase_db():
    """Yields a read-only session for the equitycase DB (user lookups)."""
    db = EquitycaseSessionLocal()
    try:
        yield db
    finally:
        db.close()

def dispose_engines():
    engine.dispose()
    equity_engine.dispose()
    equitycase_engine.dispose()
