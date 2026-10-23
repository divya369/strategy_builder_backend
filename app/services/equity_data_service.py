"""
Equity Data Service — reads OHLC price data from the equity_ohlc PostgreSQL database.

The equity_ohlc database has one table per stock/index symbol (e.g. "RELIANCE", "NIFTY_500").
Each table has at minimum: date, open, close columns.

All queries here use raw SQL via SQLAlchemy's text() to handle dynamic table names
(SQLAlchemy ORM requires declared models, but we can't declare 1000+ per-symbol models).
"""
import logging
from datetime import date
from typing import List, Optional, Tuple
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.benchmark_registry import resolve_benchmark_table
from app.core.symbol_registry import resolve_stock_symbol

logger = logging.getLogger(__name__)

# Column name constants (driven by settings so they can be changed without code edits)
_DATE_COL  = settings.EQUITY_TABLE_DATE_COL
_OPEN_COL  = settings.EQUITY_TABLE_OPEN_COL
_CLOSE_COL = settings.EQUITY_TABLE_CLOSE_COL

# Symbols per query in get_multi_stock_price_frames — bounds the rows held at once
_OHLC_CHUNK_SIZE = 50


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
    Old/renamed symbols are resolved to the current name via symbol_registry.
    Returns DataFrame with columns: date, open, close.
    Returns empty DataFrame if the table does not exist or has no data in range.
    """
    # Resolve old/renamed symbols to current DB table name
    table_symbol = resolve_stock_symbol(symbol)
    table = _quote(table_symbol)
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

    Uses a single UNION ALL query instead of per-symbol queries for speed
    (1 DB round-trip instead of N). Falls back to per-symbol loop on failure.
    """
    if not symbols:
        return pd.DataFrame(columns=["date", "symbol", "open", "close"])

    # ── Resolve symbols to table names and check which tables exist ────────
    existing_tables = set(list_available_symbols(db))
    sym_table_pairs = []
    for sym in symbols:
        table_sym = resolve_stock_symbol(sym)
        if table_sym in existing_tables:
            sym_table_pairs.append((sym, table_sym))
        else:
            logger.debug("No OHLC table for symbol '%s' (resolved: '%s') — skipping", sym, table_sym)

    if not sym_table_pairs:
        return pd.DataFrame(columns=["date", "symbol", "open", "close"])

    # ── Build single UNION ALL query ──────────────────────────────────────
    date_col = _quote(_DATE_COL)
    open_col = _quote(_OPEN_COL)
    close_col = _quote(_CLOSE_COL)

    parts = []
    for sym, table_sym in sym_table_pairs:
        # Use double-dollar quoting for the symbol literal to avoid SQL issues
        safe_sym = sym.replace("'", "''")
        table = _quote(table_sym)
        parts.append(
            f"SELECT '{safe_sym}' AS symbol, "
            f"{date_col} AS date, {open_col} AS open, {close_col} AS close "
            f"FROM public.{table} "
            f"WHERE {date_col} BETWEEN :from_date AND :to_date"
        )

    full_sql = text(" UNION ALL ".join(parts))

    try:
        result = db.execute(full_sql, {"from_date": from_date, "to_date": to_date})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "symbol", "open", "close"])
        df = pd.DataFrame(rows, columns=["symbol", "date", "open", "close"])
        df["date"]  = pd.to_datetime(df["date"]).dt.date
        df["open"]  = pd.to_numeric(df["open"],  errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df[["date", "symbol", "open", "close"]]
    except Exception as exc:
        logger.warning("Batch OHLC query failed, falling back to per-symbol loop: %s", exc)
        return _get_multi_stock_ohlc_loop(symbols, from_date, to_date, db)


def get_multi_stock_price_frames(
    symbols: List[str],
    from_date: date,
    to_date: date,
    db: Session,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Wide open/close price frames (index = date as Timestamp, one column per
    symbol) for a long date range — the backtest's price load.

    Same values as pivoting get_multi_stock_ohlc(), with a bounded memory peak:
      - prices are cast to float8 in SQL, so psycopg2 returns plain floats
        instead of ~100-byte Decimal objects (numeric→float8 rounds exactly
        like Decimal→float, so the numbers are identical);
      - symbols are fetched _OHLC_CHUNK_SIZE at a time and each chunk is
        pivoted before the next is fetched, so the long row-per-price
        format never exists for all symbols at once.
    Index dates are the union of every symbol's dates (unsorted); rows are
    not forward-filled. Symbols with no table are skipped.
    """
    existing_tables = set(list_available_symbols(db))
    sym_table_pairs = []
    for sym in symbols:
        table_sym = resolve_stock_symbol(sym)
        if table_sym in existing_tables:
            sym_table_pairs.append((sym, table_sym))
        else:
            logger.debug("No OHLC table for symbol '%s' (resolved: '%s') — skipping", sym, table_sym)

    open_parts, close_parts = [], []
    for start in range(0, len(sym_table_pairs), _OHLC_CHUNK_SIZE):
        chunk = _fetch_ohlc_chunk(sym_table_pairs[start:start + _OHLC_CHUNK_SIZE], from_date, to_date, db)
        if chunk.empty:
            continue
        chunk = chunk.drop_duplicates(subset=["date", "symbol"], keep="last")
        chunk["date"] = pd.to_datetime(chunk["date"])
        open_parts.append(chunk.pivot(index="date", columns="symbol", values="open"))
        close_parts.append(chunk.pivot(index="date", columns="symbol", values="close"))
        del chunk

    if not open_parts:
        return pd.DataFrame(), pd.DataFrame()
    return pd.concat(open_parts, axis=1), pd.concat(close_parts, axis=1)


def _fetch_ohlc_chunk(
    sym_table_pairs: List[Tuple[str, str]],
    from_date: date,
    to_date: date,
    db: Session,
) -> pd.DataFrame:
    """One UNION ALL query for a few symbols, prices as float8. Columns: date,
    symbol, open, close. Falls back to per-symbol queries on failure."""
    date_col = _quote(_DATE_COL)
    open_col = _quote(_OPEN_COL)
    close_col = _quote(_CLOSE_COL)

    parts = []
    for sym, table_sym in sym_table_pairs:
        safe_sym = sym.replace("'", "''")
        parts.append(
            f"SELECT '{safe_sym}' AS symbol, "
            f"{date_col} AS date, {open_col}::float8 AS open, {close_col}::float8 AS close "
            f"FROM public.{_quote(table_sym)} "
            f"WHERE {date_col} BETWEEN :from_date AND :to_date"
        )

    try:
        rows = db.execute(text(" UNION ALL ".join(parts)), {"from_date": from_date, "to_date": to_date}).fetchall()
    except Exception as exc:
        logger.warning("Chunk OHLC query failed, falling back to per-symbol loop: %s", exc)
        db.rollback()
        return _get_multi_stock_ohlc_loop([sym for sym, _ in sym_table_pairs], from_date, to_date, db)

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "open", "close"])
    df = pd.DataFrame(rows, columns=["symbol", "date", "open", "close"])
    df["date"]  = pd.to_datetime(df["date"]).dt.date
    df["open"]  = pd.to_numeric(df["open"],  errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df[["date", "symbol", "open", "close"]]


def _get_multi_stock_ohlc_loop(
    symbols: List[str],
    from_date: date,
    to_date: date,
    db: Session,
) -> pd.DataFrame:
    """Original per-symbol loop fallback for get_multi_stock_ohlc."""
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
