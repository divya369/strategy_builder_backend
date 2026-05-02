"""
Benchmark name mapping — Frontend (CSV filename) → DB table name (equity_ohlc).

The frontend gets index names from CSV filenames (e.g. "NIFTY MICROCAP 250").
The equity_ohlc DB may have slightly different table names (e.g. "NIFTY MICROCAP250").
This registry is the single source of truth for that mapping.

To add a new benchmark:
  1. Add a new entry to BENCHMARK_NAME_MAP below
  2. That's it — no other code changes needed
"""
import logging

logger = logging.getLogger(__name__)

# Key   = name as frontend sends it (CSV filename stem / universe value)
# Value = actual table name in equity_ohlc DB
BENCHMARK_NAME_MAP = {
    "NIFTY 50":              "NIFTY 50",
    "NIFTY 100":             "NIFTY 100",
    "NIFTY 200":             "NIFTY 200",
    "NIFTY 500":             "NIFTY 500",
    "NIFTY ALPHA 50":        "NIFTY ALPHA 50",
    "NIFTY LARGEMIDCAP 250": "NIFTY LARGEMID250",
    "NIFTY MICROCAP 250":    "NIFTY MICROCAP250",
    "NIFTY MIDCAP 50":       "NIFTY MIDCAP 50",
    "NIFTY MIDCAP 100":      "NIFTY MIDCAP 100",
    "NIFTY MIDCAP 150":      "NIFTY MIDCAP 150",
    "NIFTY MIDSMALLCAP 400": "NIFTY MIDSML 400",
    "NIFTY NEXT 50":         "NIFTY NEXT 50",
    "NIFTY SMALLCAP 50":     "NIFTY SMLCAP 50",
    "NIFTY SMALLCAP 100":    "NIFTY SMLCAP 100",
    "NIFTY SMALLCAP 250":    "NIFTY SMLCAP 250",
    "NIFTY TOTAL MARKET":    "NIFTY TOTAL MKT",
}


def resolve_benchmark_table(frontend_name: str) -> str:
    """
    Convert frontend benchmark name to the actual equity_ohlc DB table name.

    Falls back to the original name if no mapping exists (handles future
    indices whose names happen to match between CSV and DB).
    """
    # Normalize: replace underscores with spaces (frontend sometimes sends underscored names)
    normalized = frontend_name.replace("_", " ").strip()
    resolved = BENCHMARK_NAME_MAP.get(normalized, normalized)
    if resolved != normalized:
        logger.debug("Benchmark name mapped: '%s' → '%s'", frontend_name, resolved)
    return resolved
