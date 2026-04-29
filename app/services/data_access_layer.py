"""
Data Access Layer (DAL) — Abstract base for tabular data reads.

DESIGN INTENT:
  All market data reads (screener data, index constituents) go through this layer.
  Currently backed by CSV files. When you're ready to migrate to PostgreSQL,
  only the concrete implementations below need to change — all callers stay the same.

FUTURE MIGRATION GUIDE:
  To move screener data from CSV → DB table:
    1. Create a `ScreenerDataDbReader` subclass below
    2. In csv_data_service.py, swap `CsvScreenerReader()` → `ScreenerDataDbReader(db)`
    3. No changes needed in screener_execution_service.py or backtest_engine.py

USAGE:
  This module is imported and used only by csv_data_service.py.
  Do NOT import DAL classes directly in API/service layers — go through csv_data_service.
"""
import logging
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ── Abstract interface ────────────────────────────────────────────────────────

class ScreenerDataReader(ABC):
    """Abstract reader for screener data (one DataFrame per date)."""

    @abstractmethod
    def get_latest_date(self) -> Optional[date]:
        """Return the most recent date for which screener data exists."""
        ...

    @abstractmethod
    def read(self, target_date: date) -> pd.DataFrame:
        """
        Return screener data for `target_date` (or the closest available date before it).
        Returns an empty DataFrame if no data is found.
        Columns must include: tradingsymbol + all indicator columns.
        """
        ...


class IndexConstituentReader(ABC):
    """Abstract reader for index constituent lists."""

    @abstractmethod
    def list_indices(self) -> List[str]:
        """Return the names of all available indices."""
        ...

    @abstractmethod
    def get_constituents(self, index_name: str, as_of_date: date) -> List[str]:
        """
        Return the list of constituent symbols for `index_name` as of `as_of_date`.
        Uses the latest available date <= `as_of_date`.
        Returns empty list if index not found or no data before `as_of_date`.
        """
        ...


# ── CSV implementations ───────────────────────────────────────────────────────

class CsvScreenerReader(ScreenerDataReader):
    """
    Reads screener data from a folder of dated CSV files.

    File naming convention: <YYYY-MM-DD>_screener.csv
    Example: 2024-04-16_screener.csv

    Expected columns: tradingsymbol (required) + any indicator columns.
    The `date` column is NOT required in the file (date comes from filename).
    """

    def __init__(self, directory: str):
        self._dir = Path(directory)
        self._date_map: dict = {}   # cache: date → Path
        self._df_cache: dict = {}   # cache: date → DataFrame (loaded on demand)
        self._scan()

    def _scan(self):
        """Scan directory and build a date → filepath map."""
        self._date_map.clear()
        if not self._dir.exists():
            logger.warning("Screener CSV directory not found: %s", self._dir)
            return
        for path in self._dir.glob("*_screener.csv"):
            date_str = path.stem.replace("_screener", "")
            try:
                parsed = date.fromisoformat(date_str)
                self._date_map[parsed] = path
            except ValueError:
                logger.debug("Skipping non-date screener file: %s", path.name)
        logger.info("Screener CSV reader: found %d dated files in %s", len(self._date_map), self._dir)

    def get_latest_date(self) -> Optional[date]:
        if not self._date_map:
            return None
        return max(self._date_map)

    def _find_closest_date(self, target_date: date) -> Optional[date]:
        """Find the latest available date <= target_date."""
        candidates = [d for d in self._date_map if d <= target_date]
        return max(candidates) if candidates else None

    def read(self, target_date: date) -> pd.DataFrame:
        closest = self._find_closest_date(target_date)
        if closest is None:
            logger.warning("No screener CSV found on or before %s", target_date)
            return pd.DataFrame()

        # Return cached DataFrame if available
        if closest in self._df_cache:
            return self._df_cache[closest]

        path = self._date_map[closest]
        try:
            df = pd.read_csv(path, low_memory=False)
            # Normalize symbol column name
            df = _normalize_symbol_col(df)
            if df.empty or "tradingsymbol" not in df.columns:
                logger.error("Screener CSV %s has no 'tradingsymbol' column", path.name)
                return pd.DataFrame()
            self._df_cache[closest] = df
            logger.debug("Loaded screener CSV: %s (%d rows)", path.name, len(df))
            return df
        except Exception as exc:
            logger.error("Failed to read screener CSV %s: %s", path, exc)
            return pd.DataFrame()

    @property
    def available_dates(self) -> List[date]:
        """Sorted list of all dates with screener CSV data."""
        return sorted(self._date_map.keys())


class CsvIndexConstituentReader(IndexConstituentReader):
    """
    Reads index constituent data from CSV files.

    File naming convention: <INDEX_NAME>.csv  (e.g. "NIFTY 500.csv")
    File structure (pivot format):
        - Column headers = dates (e.g. 2015-01-01, 2015-02-03, ...)
        - Row values     = constituent stock symbols for that date
        - Each date column may have different number of entries
    """

    def __init__(self, directory: str):
        self._dir = Path(directory)
        self._df_cache: dict = {}   # cache: index_name → DataFrame

    def list_indices(self) -> List[str]:
        """Return index names derived from CSV filenames (without extension)."""
        if not self._dir.exists():
            logger.warning("Index CSV directory not found: %s", self._dir)
            return []
        names = [p.stem for p in self._dir.glob("*.csv")]
        logger.debug("Index CSV reader: found indices %s", names)
        return sorted(names)

    def _load(self, index_name: str) -> Optional[pd.DataFrame]:
        """Load and cache the pivot CSV for an index."""
        if index_name in self._df_cache:
            return self._df_cache[index_name]

        path = self._dir / f"{index_name}.csv"
        if not path.exists():
            logger.warning("Index CSV not found: %s", path)
            return None
        try:
            df = pd.read_csv(path, index_col=0, header=0)
            # Parse column names as dates
            df.columns = pd.to_datetime(df.columns, errors="coerce")
            df = df.loc[:, df.columns.notna()]  # drop any columns that failed date parse
            self._df_cache[index_name] = df
            logger.info("Loaded index CSV: %s (%d date columns)", path.name, len(df.columns))
            return df
        except Exception as exc:
            logger.error("Failed to read index CSV %s: %s", path, exc)
            return None

    def get_constituents(self, index_name: str, as_of_date: date) -> List[str]:
        df = self._load(index_name)
        if df is None or df.empty:
            return []

        # Find the latest date column <= as_of_date
        as_of_ts = pd.Timestamp(as_of_date)
        valid_cols = [c for c in df.columns if c <= as_of_ts]
        if not valid_cols:
            logger.warning("No index data for '%s' on or before %s", index_name, as_of_date)
            return []

        latest_col = max(valid_cols)
        constituents = df[latest_col].dropna().tolist()
        # Normalize to strings and strip whitespace
        constituents = [str(s).strip() for s in constituents if str(s).strip()]
        logger.debug("Index '%s' as of %s → %d constituents", index_name, as_of_date, len(constituents))
        return constituents


# ── Utility ───────────────────────────────────────────────────────────────────

def _normalize_symbol_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the symbol column is named 'tradingsymbol'.
    Handles common alternate names: symbol, Symbol, ticker, Ticker, TradingSymbol.
    """
    aliases = {"symbol", "ticker", "tradingsymbol"}
    for col in df.columns:
        if col.lower().strip() in aliases:
            df = df.rename(columns={col: "tradingsymbol"})
            return df
    return df
