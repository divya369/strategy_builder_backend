"""
Filter Registry — single source of truth for all filter & sort metadata.

Both the API route layer (screeners.py) and the execution service import
from here. To add/rename a filter, edit ONLY this file.

Each entry in FILTER_CONFIG_MAP carries:
  - label        : human-readable name (used in UI dropdowns AND result columns)
  - type         : filter type (metric_value | metric_period_value | relative_level | field_comparison)
  - dbKey        : CSV column name override (if omitted, the dict key IS the column name)
  - operators    : allowed comparison operators
  - periods/periodValues : for period-based filters
  - sortable/sortGroup   : whether the field appears in sort options
  - relations    : for relative_level filters (above/below)
"""

from typing import Dict, List, Optional


# ── Master filter configuration ──────────────────────────────────────────────
FILTER_CONFIG_MAP: Dict[str, dict] = {
    "return_pct": {
        "label": "Return (%)",
        "type": "metric_period_value",
        "periods": ["1 Year", "9 Months", "6 Months", "3 Months", "1 Month"],
        "periodValues": ["1y", "9m", "6m", "3m", "1m"],
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Returns",
    },
    "sharpe_return_pct": {
        "label": "Sharpe Return (%)",
        "type": "metric_period_value",
        "periods": ["1 Year", "9 Months", "6 Months", "3 Months", "1 Month"],
        "periodValues": ["1y", "9m", "6m", "3m", "1m"],
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Returns",
    },
    "positive_days_pct": {
        "label": "Positive Days (%)",
        "type": "metric_period_value",
        "periods": ["1 Year", "9 Months", "6 Months", "3 Months", "1 Month"],
        "periodValues": ["1y", "9m", "6m", "3m", "1m"],
        "operators": [">=", "<=", ">", "<", "=="],
    },
    "max_circuits_pct": {
        "label": "Maximum Circuits (%)",
        "type": "metric_value",
        "dbKey": "circuits_in_1y",
        "operators": [">=", "<=", ">", "<", "=="],
    },
    "median_daily_volume_1y": {
        "label": "Median Daily Volume 1Y",
        "type": "metric_value",
        "dbKey": "median_volume_rupees",
        "operators": [">=", "<=", ">", "<", "=="],
    },
    "away_from_ath_pct": {
        "label": "Away From ATH (%)",
        "type": "metric_value",
        "dbKey": "away_from_ath",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Technical",
    },
    "away_from_1y_high_pct": {
        "label": "Away From 1Y High (%)",
        "type": "metric_value",
        "dbKey": "away_from_1y_high",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Technical",
    },
    "marketcap": {
        "label": "Marketcap",
        "type": "metric_value",
        "dbKey": "market_cap_crores",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Fundamentals",
    },
    "turnover": {
        "label": "Turnover",
        "type": "metric_value",
        "dbKey": "volume_rupees",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Fundamentals",
    },
    "price": {
        "label": "Price",
        "type": "metric_value",
        "dbKey": "close",
        "operators": [">=", "<=", ">", "<", "=="],
    },
    "volume": {
        "label": "Volume (in rupees)",
        "type": "metric_value",
        "operators": [">=", "<=", ">", "<", "=="],
    },
    "beta": {
        "label": "Beta",
        "type": "metric_value",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Risk",
    },
    "moving_average": {
        "label": "Moving Avg",
        "type": "relative_level",
        "relations": ["above", "below"],
        "periods": ["200 Days", "100 Days", "50 Days", "20 Days"],
        "periodValues": ["200d", "100d", "50d", "20d"],
    },
    "ema": {
        "label": "Exponential Moving Avg",
        "type": "relative_level",
        "relations": ["above", "below"],
        "periods": ["200 Days", "100 Days", "50 Days", "20 Days"],
        "periodValues": ["200d", "100d", "50d", "20d"],
    },
    "compare_params": {
        "label": "Compare Parameters",
        "type": "field_comparison",
        "operators": [">=", "<=", ">", "<", "=="],
        "comparableFields": [
            "return_pct", "sharpe_return_pct", "positive_days_pct",
            "marketcap", "turnover", "price", "volume", "beta",
            "moving_average", "ema",
        ],
    },
}


# ── Extra sort-only fields (not filters, just sortable columns) ──────────────
EXTRA_SORT_FIELDS: List[dict] = [
    {"value": "price",              "label": "Price (Close)",      "group": "Fundamentals"},
    {"value": "volume",             "label": "Volume",             "group": "Fundamentals"},
    {"value": "median_volume_rupees","label": "Median Volume",     "group": "Fundamentals"},
    {"value": "1y_volatility",      "label": "Volatility 1Y",     "group": "Risk"},
    {"value": "away_from_ath",      "label": "Away from ATH (%)",  "group": "Technical"},
    {"value": "away_from_1y_high",  "label": "Away from 1Y High",  "group": "Technical"},
]


# ── Short period labels (for compact table column headers) ───────────────────
PERIOD_DISPLAY: Dict[str, str] = {
    "1y": "1Y", "9m": "9M", "6m": "6M", "3m": "3M", "1m": "1M",
    "200d": "200D", "100d": "100D", "50d": "50D", "20d": "20D",
}


# ── Helper functions ─────────────────────────────────────────────────────────

def get_filter_label(field: str, period: str = None) -> str:
    """
    Return a human-readable column label for a filter field + optional period.
    E.g. get_filter_label("return_pct", "1y") → "1Y Return (%)"
    """
    conf = FILTER_CONFIG_MAP.get(field, {})
    base = conf.get("label", field)
    if period:
        p_short = PERIOD_DISPLAY.get(period, "")
        return f"{p_short} {base}".strip()
    return base


def get_db_key(field: str) -> Optional[str]:
    """
    Return the CSV column name override for a UI field, or None if the field
    name itself is the column name.
    """
    conf = FILTER_CONFIG_MAP.get(field)
    if conf:
        return conf.get("dbKey")
    return None
