"""
Universe size registry — index name → number of stocks in that index.

WHY A HAND-MAINTAINED MAP:
  Counting rows in the constituent CSV looks tempting, but that data is not
  always complete — a partial or late file would silently report "NIFTY 100 has
  63 stocks" and reject valid screeners. A declared size is stable no matter what
  the CSV looks like on any given day.

  Size here is the NOMINAL index size (what the index is defined to hold), which
  is exactly what "top N out of this universe" should be validated against.

To add a new index:
  1. Add a new entry to INDEX_SIZE_MAP below
  2. That's it — no other code changes needed

  Key = name as the frontend sends it (CSV filename stem / universe value).
  Keep keys in sync with app/core/benchmark_registry.py.

NOT LISTED HERE = SIZE UNKNOWN:
  Lookups return None for anything unregistered, and callers skip size checks
  rather than blocking the request. Drop an index CSV in without touching this
  file and screeners keep working — you just lose size validation for it until
  the entry is added. `missing_entries()` reports that gap.

  "All Stocks" has no size by definition (it is whatever the screener CSV holds),
  so it is represented as None everywhere and is not listed below.
"""
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Key   = universe value sent by the frontend (index CSV filename stem)
# Value = nominal number of stocks in that index
INDEX_SIZE_MAP: Dict[str, int] = {
    "NIFTY 50":              50,
    "NIFTY 100":             100,
    "NIFTY 200":             200,
    "NIFTY 500":             500,
    "NIFTY ALPHA 50":        50,
    "NIFTY LARGEMIDCAP 250": 250,
    "NIFTY MICROCAP 250":    250,
    "NIFTY MIDCAP 50":       50,
    "NIFTY MIDCAP 100":      100,
    "NIFTY MIDCAP 150":      150,
    "NIFTY MIDSMALLCAP 400": 400,
    "NIFTY NEXT 50":         50,
    "NIFTY SMALLCAP 50":     50,
    "NIFTY SMALLCAP 100":    100,
    "NIFTY SMALLCAP 250":    250,
    "NIFTY TOTAL MARKET":    750,   # verify — the only entry not implied by its name
}


def _normalize(name: str) -> str:
    """Frontend sometimes sends underscored / differently-cased names."""
    return str(name).replace("_", " ").strip().casefold()


# Normalized lookup table, built once at import.
_NORMALIZED: Dict[str, str] = {_normalize(k): k for k in INDEX_SIZE_MAP}


def resolve_index_name(name: Optional[str]) -> Optional[str]:
    """
    Resolve a client-supplied index name to its canonical registry key
    ("nifty_50" → "NIFTY 50"). Returns None if the index is not registered.
    """
    if not name:
        return None
    return _NORMALIZED.get(_normalize(name))


def get_index_size(name: Optional[str]) -> Optional[int]:
    """
    Nominal size of `name`, or None if it is not registered.

    None means "unknown", never "unlimited" — callers must decide whether to skip
    validation or reject. A warning is logged so an unregistered index that is
    actually in use shows up in the logs.
    """
    canonical = resolve_index_name(name)
    if canonical is None:
        if name:
            logger.warning(
                "Index '%s' has no INDEX_SIZE_MAP entry — size checks skipped. "
                "Add it to app/core/universe_registry.py.", name
            )
        return None
    return INDEX_SIZE_MAP[canonical]


def is_registered(name: Optional[str]) -> bool:
    """True if `name` has a declared size."""
    return resolve_index_name(name) is not None


def missing_entries(index_names: List[str]) -> List[str]:
    """
    Which of `index_names` have no registered size — pass the available index
    CSV names to spot a newly added index before it reaches a user.
    """
    return [n for n in index_names if not is_registered(n)]
