"""
CSV Data Service — single access point for all file-based market data.

This service wraps the Data Access Layer (data_access_layer.py) and is the
ONLY place in the codebase that knows the CSV directory paths.

All other services call this module — they never read CSV files directly.
"""
import logging
from datetime import date
from typing import List, Optional

from app.core.config import settings
from app.services.data_access_layer import CsvScreenerReader, CsvIndexConstituentReader

logger = logging.getLogger(__name__)


# ── Module-level singletons (created once at import time) ─────────────────────
# Paths come from settings (env-var overridable for server deployment).
_screener_reader = CsvScreenerReader(settings.SCREENER_CSV_DIR)
_index_reader    = CsvIndexConstituentReader(settings.INDEX_CSV_DIR)


# ── Public API ────────────────────────────────────────────────────────────────

def get_latest_screener_date() -> Optional[date]:
    """Return the most recent date for which a screener CSV exists."""
    return _screener_reader.get_latest_date()


def get_screener_data(target_date: date):
    """
    Return screener DataFrame for the given date (or closest prior date).
    Columns: tradingsymbol + all indicator columns from the CSV.
    Returns an empty DataFrame if no data is available.
    """
    return _screener_reader.read(target_date)


def get_available_screener_dates() -> List[date]:
    """Sorted list of all dates with screener CSV data."""
    return _screener_reader.available_dates


def list_available_indices() -> List[str]:
    """Return names of all indices with a constituent CSV file."""
    return _index_reader.list_indices()


def get_index_constituents(index_name: str, as_of_date: date) -> List[str]:
    """
    Return constituent symbols for `index_name` as of `as_of_date`.
    Uses the latest available date <= `as_of_date` (handles gaps in history).
    Returns empty list if index not found.
    """
    return _index_reader.get_constituents(index_name, as_of_date)
