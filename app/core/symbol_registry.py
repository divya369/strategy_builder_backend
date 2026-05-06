"""
Stock symbol rename mapping — Old trading symbol → Current trading symbol.

When companies rename/merge, the exchange migrates all historical OHLC data
under the new symbol. But screener CSVs still reference the old symbol for
historical dates. This registry maps old → new so the backtest engine can
find the correct table in equity_ohlc DB.

Data lives in symbol_rename_map.json (same directory). The JSON is auto-reloaded
when modified — no server restart needed.

JSON format (new_symbol → [list of old symbols]):
  {
    "WELSPUNLIV": ["WELSPUNIND"],
    "LANDSMILL":  ["EXCEL", "EXCELINFO"]
  }

To add a new rename:
  1. Edit symbol_rename_map.json
  2. That's it — the server picks up changes automatically

For automated scripts updating the JSON when a symbol renames again:
  old_names = data.pop("OLD_KEY")       # remove old key, get its list
  old_names.insert(0, "OLD_KEY")        # add old key itself to the list
  data["NEW_KEY"] = old_names           # create new key with combined history
"""
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_MAP_FILE = Path(__file__).parent / "symbol_rename_map.json"
_cached_mtime: float = 0.0
_OLD_TO_NEW: dict = {}


def _reload_if_needed():
    """Check file mtime and reload the JSON map only if it changed."""
    global _cached_mtime, _OLD_TO_NEW

    try:
        current_mtime = os.path.getmtime(_MAP_FILE)
    except OSError:
        logger.warning("Symbol rename map not found: %s", _MAP_FILE)
        return

    if current_mtime == _cached_mtime:
        return  # file unchanged, use cached data

    # File changed — reload
    try:
        with open(_MAP_FILE, encoding="utf-8") as f:
            rename_map = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load symbol rename map: %s", exc)
        return

    new_lookup = {}
    for new_sym, old_list in rename_map.items():
        for old_sym in old_list:
            new_lookup[old_sym] = new_sym

    _OLD_TO_NEW = new_lookup
    _cached_mtime = current_mtime
    logger.info("Symbol rename map reloaded (%d old→new entries)", len(_OLD_TO_NEW))


# Initial load at import time
_reload_if_needed()


def resolve_stock_symbol(symbol: str) -> str:
    """
    If the symbol was renamed, return the current name (DB table exists
    under this name). Otherwise return as-is.

    Auto-reloads the JSON map if the file was modified since last check.
    """
    _reload_if_needed()
    resolved = _OLD_TO_NEW.get(symbol, symbol)
    if resolved != symbol:
        logger.debug("Symbol renamed: '%s' → '%s'", symbol, resolved)
    return resolved
