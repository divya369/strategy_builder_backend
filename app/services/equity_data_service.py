"""
Equity Data Service — reads OHLC price data from the equity_ohlc PostgreSQL database.

The equity_ohlc database has one table per stock/index symbol (e.g. "RELIANCE", "NIFTY_500").
Each table has at minimum: date, open, close columns.

All queries here use raw SQL via SQLAlchemy's text() to handle dynamic table names
(SQLAlchemy ORM requires declared models, but we can't declare 1000+ per-symbol models).
"""
import logging
from datetime import date
from typing import List, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.benchmark_registry import resolve_benchmark_table

logger = logging.getLogger(__name__)

# Column name constants (driven by settings so they can be changed without code edits)
_DATE_COL  = settings.EQUITY_TABLE_DATE_COL
_OPEN_COL  = settings.EQUITY_TABLE_OPEN_COL
_CLOSE_COL = settings.EQUITY_TABLE_CLOSE_COL


def _quote(name: str) -> str:
    """
    PostgreSQL-quote a table/column name to preserve case and handle special chars.
    e.g. "NIFTY_500" → '"NIFTY_500"'
    """
    return f'"{name}"'


def get_stock_ohlc(
    symbol: str,
    from_date: date,
    to_date: date,
    db: Session,
) -> pd.DataFrame:
    """
    Fetch daily OHLC for a single stock symbol from equity_ohlc.
    Returns DataFrame with columns: date, open, close.
    Returns empty DataFrame if the table does not exist or has no data in range.
    """
    table = _quote(symbol)
    sql = text(f"""
        SELECT
            {_quote(_DATE_COL)}  AS date,
            {_quote(_OPEN_COL)}  AS open,
            {_quote(_CLOSE_COL)} AS close
        FROM public.{table}
        WHERE {_quote(_DATE_COL)} BETWEEN :from_date AND :to_date
        ORDER BY {_quote(_DATE_COL)}
    """)
    try:
        result = db.execute(sql, {"from_date": from_date, "to_date": to_date})
        rows = result.fetchall()
        if not rows:
            logger.debug("No OHLC data for symbol '%s' in range %s–%s", symbol, from_date, to_date)
            return pd.DataFrame(columns=["date", "open", "close"])
        df = pd.DataFrame(rows, columns=["date", "open", "close"])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["open"]  = pd.to_numeric(df["open"],  errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df
    except Exception as exc:
        # Table might not exist for some symbols — log and return empty
        logger.warning("Could not fetch OHLC for '%s': %s", symbol, exc)
        return pd.DataFrame(columns=["date", "open", "close"])


def get_benchmark_ohlc(
    index_name: str,
    from_date: date,
    to_date: date,
    db: Session,
) -> pd.DataFrame:
    """
    Fetch daily close prices for a benchmark index (e.g. "NIFTY_500").
    Benchmark tables live in the same equity_ohlc DB as stock tables.
    The index_name from the frontend is resolved to the actual DB table name
    via the benchmark_registry mapping.
    Returns DataFrame with columns: date, close.
    """
    # Resolve frontend name → actual DB table name
    table_name = resolve_benchmark_table(index_name)
    table = _quote(table_name)
    sql = text(f"""
        SELECT
            {_quote(_DATE_COL)}  AS date,
            {_quote(_CLOSE_COL)} AS close
        FROM public.{table}
        WHERE {_quote(_DATE_COL)} BETWEEN :from_date AND :to_date
        ORDER BY {_quote(_DATE_COL)}
    """)
    try:
        result = db.execute(sql, {"from_date": from_date, "to_date": to_date})
        rows = result.fetchall()
        if not rows:
            logger.warning("No benchmark data for '%s' in range %s–%s", index_name, from_date, to_date)
            return pd.DataFrame(columns=["date", "close"])
        df = pd.DataFrame(rows, columns=["date", "close"])
        df["date"]  = pd.to_datetime(df["date"]).dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df
    except Exception as exc:
        logger.error("Could not fetch benchmark '%s': %s", index_name, exc)
        return pd.DataFrame(columns=["date", "close"])


def get_multi_stock_ohlc(
    symbols: List[str],
    from_date: date,
    to_date: date,
    db: Session,
) -> pd.DataFrame:
    """
    Fetch OHLC for multiple symbols and return a combined long-format DataFrame.
    Columns: date, symbol, open, close.
    Symbols with no table are silently skipped.
    """
    frames = []
    for symbol in symbols:
        df = get_stock_ohlc(symbol, from_date, to_date, db)
        if not df.empty:
            df["symbol"] = symbol
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "open", "close"])
    combined = pd.concat(frames, ignore_index=True)
    return combined


def list_available_symbols(db: Session) -> List[str]:
    """
    Query pg_tables to list all daily stock tables in equity_ohlc (excludes _weekly).
    Useful for 'ALL stocks' universe type.
    """
    sql = text("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename NOT LIKE '%_weekly'
        ORDER BY tablename
    """)
    try:
        result = db.execute(sql)
        return [row[0] for row in result.fetchall()]
    except Exception as exc:
        logger.error("Failed to list available symbols: %s", exc)
        return []
