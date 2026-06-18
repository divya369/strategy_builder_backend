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
        "description": "## Return (%)\n\n**What it means:** Measures how much a stock price has increased or decreased over a selected time period.\n\n**Example:** If a stock moved from ₹100 to ₹130, its return is 30%.\n\n**Why investors use it:** Used to identify strong momentum stocks.",
        "type": "metric_period_value",
        "periods": ["1 Year", "9 Months", "6 Months", "3 Months", "1 Month"],
        "periodValues": ["1y", "9m", "6m", "3m", "1m"],
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Returns",
    },
    "sharpe_return_pct": {
        "label": "Sharpe Return (%)",
        "description": "## Sharpe Return (%)\n\n**What it means:** Measures return compared to the amount of risk taken.\n\n**Example:** Two stocks may give the same return, but the one with smoother price movement gets a better Sharpe score.\n\n**Why investors use it:** Helps identify quality momentum rather than risky speculation.",
        "type": "metric_period_value",
        "periods": ["1 Year", "9 Months", "6 Months", "3 Months", "1 Month"],
        "periodValues": ["1y", "9m", "6m", "3m", "1m"],
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Returns",
    },
    "positive_days_pct": {
        "label": "Positive Days (%)",
        "description": "## Positive Days (%)\n\n**What it means:** Shows the percentage of trading days where the stock closed higher than the previous day.\n\n**Example:** If a stock closed positive on 70 out of 100 days, its Positive Days value is 70%.\n\n**Why investors use it:** Higher values indicate consistency and steady buying interest.",
        "type": "metric_period_value",
        "periods": ["1 Year", "9 Months", "6 Months", "3 Months", "1 Month"],
        "periodValues": ["1y", "9m", "6m", "3m", "1m"],
        "operators": [">=", "<=", ">", "<", "=="],
    },
    "max_circuits_pct": {
        "label": "Maximum Circuits",
        "description": "## Maximum Circuits\n\n**What it means:** Measures how frequently a stock hits upper or lower circuit limits.\n\n**Example:** Stocks repeatedly hitting circuits are often extremely volatile or speculative.\n\n**Why investors use it:** Helps investors avoid highly unstable stocks.",
        "type": "metric_value",
        "dbKey": "circuits_in_1y",
        "operators": [">=", "<=", ">", "<", "=="],
    },
    "median_daily_volume_1y": {
        "label": "Median Daily Volume 1Y",
        "description": "## Median Daily Volume 1Y\n\n**What it means:** Measures the typical daily trading volume over the last one year.\n\n**Example:** Unlike average volume, median volume avoids distortion from a few unusually high-volume days.\n\n**Why investors use it:** Used to identify consistently liquid stocks.",
        "type": "metric_value",
        "dbKey": "median_volume_rupees",
        "operators": [">=", "<=", ">", "<", "=="],
    },
    "away_from_ath_pct": {
        "label": "Away From ATH (%)",
        "description": "## Away From ATH (%)\n\n**What it means:** Measures how far the stock is trading from its All-Time High.\n\n**Example:** A stock trading only 5% below its ATH is generally considered strong.\n\n**Why investors use it:** Momentum investors often prefer stocks near their highs.",
        "type": "metric_value",
        "dbKey": "away_from_ath",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Technical",
    },
    "away_from_1y_high_pct": {
        "label": "Away From 1Y High (%)",
        "description": "## Away From 1Y High (%)\n\n**What it means:** Measures how far the stock is from its highest price in the last one year.\n\n**Example:** Stocks near their yearly highs usually indicate recent strength.\n\n**Why investors use it:** Used to identify stocks already in strong trends.",
        "type": "metric_value",
        "dbKey": "away_from_1y_high",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Technical",
    },
    "marketcap": {
        "label": "Marketcap",
        "description": "## Marketcap\n\n**What it means:** Represents the total size of the company in the stock market.\n\n**Example:** Large-cap companies are generally more stable, while small-cap companies may grow faster but carry higher risk.\n\n**Why investors use it:** Used for balancing growth potential and stability.",
        "type": "metric_value",
        "dbKey": "market_cap_crores",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Fundamentals",
    },
    "turnover": {
        "label": "Turnover",
        "description": "## Turnover\n\n**What it means:** Measures the total value of shares traded.\n\n**Example:** Higher turnover means more market participation and easier buying/selling.\n\n**Why investors use it:** Important for liquidity-focused investors.",
        "type": "metric_value",
        "dbKey": "volume_rupees",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Liquidity",
    },
    "price": {
        "label": "Price(Close)",
        "description": "## Price (Close)\n\n**What it means:** The latest closing price of the stock.\n\n**Example:** Some investors use price filters to avoid penny stocks or extremely expensive stocks.\n\n**Why investors use it:** Useful for portfolio allocation and stock selection.",
        "type": "metric_value",
        "dbKey": "close",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Fundamentals",
    },
    "volume": {
        "label": "Volume (in rupees)",
        "description": "## Volume (in Rupees)\n\n**What it means:** Measures the total traded value in rupee terms.\n\n**Example:** High-volume stocks generally have better liquidity and lower execution issues.\n\n**Why investors use it:** Momentum strategies often prefer high-volume stocks.",
        "type": "metric_value",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Liquidity",
    },
    "beta": {
        "label": "Beta",
        "description": "## Beta\n\n**What it means:** Measures how aggressively a stock moves compared to the overall market.\n\n**Example:** Beta above 1 means the stock moves more sharply than the market.\n\n**Why investors use it:** Used to understand market sensitivity and risk.",
        "type": "metric_value",
        "operators": [">=", "<=", ">", "<", "=="],
        "sortable": True,
        "sortGroup": "Risk",
    },
    "moving_average": {
        "label": "Moving Avg",
        "description": "## Moving Average\n\n**What it means:** A Moving Average smooths price data to show the overall trend direction.\n\n**Example:** If price is above its moving average, the stock is generally considered to be in an uptrend.\n\n**Why investors use it:** Helps investors identify trend direction.",
        "type": "relative_level",
        "relations": ["above", "below"],
        "periods": ["200 Days", "100 Days", "50 Days", "20 Days"],
        "periodValues": ["200d", "100d", "50d", "20d"],
    },
    "ema": {
        "label": "Exponential Moving Avg",
        "description": "## Exponential Moving Average (EMA)\n\n**What it means:** An EMA is similar to a moving average but gives more importance to recent prices.\n\n**Example:** This makes EMA react faster to trend changes.\n\n**Why investors use it:** Widely used in momentum and trend-following strategies.",
        "type": "relative_level",
        "relations": ["above", "below"],
        "periods": ["200 Days", "100 Days", "50 Days", "20 Days"],
        "periodValues": ["200d", "100d", "50d", "20d"],
    },
    "compare_params": {
        "label": "Compare Parameters",
        "description": "## Compare Parameters\n\n**What it means:** Allows investors to compare one metric against another.\n\n**Example:** For example, comparing Return against Volatility can help identify efficient momentum stocks.\n\n**Why investors use it:** Useful for creating more advanced custom strategies.",
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
    {"value": "1y_volatility",      "label": "Volatility 1 Year",     "group": "Risk",
     "description": "## Volatility 1 Year\n\n**What it means:** Sorts by how much a stock's price fluctuates over 1 year. Lower volatility means more predictable price behavior.\n\n**Why investors use it:** Helps identify stable stocks with smoother price trends."},
    {"value": "away_from_ath",      "label": "Away from ATH (%)",  "group": "Technical",
     "description": "## Away from ATH (%)\n\n**What it means:** Sorts by distance from the All-Time High price. Stocks near ATH indicate strong momentum.\n\n**Why investors use it:** Momentum investors prefer stocks trading close to their all-time highs."},
    {"value": "away_from_1y_high",  "label": "Away from 1 Year High",  "group": "Technical",
     "description": "## Away from 1 Year High\n\n**What it means:** Sorts by distance from the 52-week high. Useful for finding stocks in recent uptrends.\n\n**Why investors use it:** Stocks near their yearly highs usually indicate recent strength."},
    {"value": "average_sharpe_12_9_6_3_1_months", "label": "Avg Sharpe Return 12 9 6 3 1 months", "group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 9 6 3 1 months\n\n**What it means:** Average of Sharpe returns across 12, 9, 6, 3, and 1 month periods. Measures risk-adjusted momentum across all major timeframes.\n\n**Why investors use it:** Helps identify stocks showing consistent risk-adjusted strength from long-term trends to recent performance."},
    {"value": "average_sharpe_12_9_6_3_months","label": "Avg Sharpe Return 12 9 6 3 months", "group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 9 6 3 months\n\n**What it means:** Average of Sharpe returns across 12, 9, 6, and 3 month periods. Identifies stocks with consistently strong risk-adjusted performance.\n\n**Why investors use it:** Rewards stocks that deliver quality momentum across multiple timeframes."},
    {"value": "average_sharpe_12_9_6_months","label": "Avg Sharpe Return 12 9 6 months", "group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 9 6 months\n\n**What it means:** Average of Sharpe returns across 12, 9, and 6 month periods. Focuses on medium to long-term risk-adjusted momentum.\n\n**Why investors use it:** Filters out short-term noise and rewards stocks with sustained quality performance."},
    {"value": "average_sharpe_12_9_months","label": "Avg Sharpe Return 12 9 months", "group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 9 months\n\n**What it means:** Average of Sharpe returns across 12 and 9 month periods. Emphasizes long-term risk-adjusted trends.\n\n**Why investors use it:** Suitable for investors seeking durable momentum rather than recent price acceleration."},
    {"value": "average_sharpe_12_6_3_1_months","label": "Avg Sharpe Return 12 6 3 1 months","group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 6 3 1 months\n\n**What it means:** Average of Sharpe returns across 12, 6, 3, and 1 month periods. Balances long-term trend strength with recent momentum.\n\n**Why investors use it:** Captures stocks that remain strong across multiple investment horizons."},
    {"value": "average_sharpe_12_6_3_months","label": "Avg Sharpe Return 12 6 3 months","group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 6 3 months\n\n**What it means:** Average of Sharpe returns across 12, 6, and 3 month periods. Measures risk-adjusted momentum over mixed timeframes.\n\n**Why investors use it:** Helps identify stocks with both established and emerging positive trends."},
    {"value": "average_sharpe_12_6_months","label": "Avg Sharpe Return 12 6 months", "group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 6 months\n\n**What it means:** Average of Sharpe returns across 12 and 6 month periods. Measures medium to long-term risk-adjusted performance.\n\n**Why investors use it:** Popular for identifying stable momentum stocks while reducing short-term market noise."},
    {"value": "average_sharpe_12_9_3_1_months","label": "Avg Sharpe Return 12 9 3 1 months", "group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 9 3 1 months\n\n**What it means:** Average of Sharpe returns across 12, 9, 3, and 1 month periods. Combines long-term strength with recent momentum acceleration.\n\n**Why investors use it:** Useful for spotting stocks where momentum is strengthening across timeframes."},
    {"value": "average_sharpe_12_9_3_months","label": "Avg Sharpe Return 12 9 3 months", "group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 9 3 months\n\n**What it means:** Average of Sharpe returns across 12, 9, and 3 month periods. Blends long-term trend quality with recent performance.\n\n**Why investors use it:** Helps identify stocks maintaining strong momentum while gaining recent traction."},
    {"value": "average_sharpe_12_3_1_months","label": "Avg Sharpe Return 12 3 1 months", "group": "Risk-Adjusted Momentum",
    "description": "## Avg Sharpe Return 12 3 1 months\n\n**What it means:** Average of Sharpe returns across 12, 3, and 1 month periods. Combines long-term trend strength with short-term momentum.\n\n**Why investors use it:** Suitable for investors seeking stocks with both established and emerging momentum."},
    {"value": "average_sharpe_12_3_months",      "label": "Avg Sharpe Return 12 3 months",  "group": "Risk-Adjusted Momentum",
     "description": "## Avg Sharpe Return 12 3 months\n\n**What it means:** Average of Sharpe returns across 12 and 3 month periods. Combines long-term and short-term quality momentum.\n\n**Why investors use it:** Captures both sustained trends and recent acceleration."},
    {"value": "average_return_12_9_6_3_1_months","label": "Avg Return 12 9 6 3 1 months","group": "Momentum",
    "description": "## Avg Return 12 9 6 3 1 months\n\n**What it means:** Average of raw returns across 12, 9, 6, 3, and 1 month periods. Provides a comprehensive momentum score across all major timeframes.\n\n**Why investors use it:** Helps identify stocks with broad-based momentum consistency."},
    {"value": "average_return_12_9_6_3_months",      "label": "Avg Return 12 9 6 3 months",  "group": "Momentum",
     "description": "## Avg Return 12 9 6 3 months\n\n**What it means:** Average of raw returns across 12, 9, 6, and 3 month periods. Identifies stocks with consistent momentum across multiple timeframes.\n\n**Why investors use it:** A comprehensive multi-period momentum composite."},
    {"value": "average_return_12_9_6_months","label": "Avg Return 12 9 6 months","group": "Momentum",
    "description": "## Avg Return 12 9 6 months\n\n**What it means:** Average of raw returns across 12, 9, and 6 month periods. Focuses on medium to long-term momentum.\n\n**Why investors use it:** Filters out short-term volatility and highlights sustained performers."},
    {"value": "average_return_12_9_months","label": "Avg Return 12 9 months","group": "Momentum",
    "description": "## Avg Return 12 9 months\n\n**What it means:** Average of raw returns across 12 and 9 month periods. Measures longer-term momentum strength.\n\n**Why investors use it:** Useful for investors preferring durable trends over recent price spikes."},
    {"value": "average_return_12_6_3_1_months","label": "Avg Return 12 6 3 1 months","group": "Momentum",
    "description": "## Avg Return 12 6 3 1 months\n\n**What it means:** Average of raw returns across 12, 6, 3, and 1 month periods. Captures momentum from long-term trends through recent performance.\n\n**Why investors use it:** Rewards stocks showing strength across multiple market cycles."},    
    {"value": "average_return_12_6_3_months",      "label": "Avg Return 12 6 3 months",  "group": "Momentum",
     "description": "## Avg Return 12 6 3 months\n\n**What it means:** Average of raw returns across 12, 6, and 3 month periods. A popular multi-period momentum composite.\n\n**Why investors use it:** Widely used in momentum investing to rank stocks."},
    {"value": "average_return_12_6_months","label": "Avg Return 12 6 months","group": "Momentum",
    "description": "## Avg Return 12 6 months\n\n**What it means:** Average of raw returns across 12 and 6 month periods. Measures medium to long-term momentum.\n\n**Why investors use it:** A simple and effective ranking method for trend-following strategies."},
    {"value": "average_return_12_9_3_1_months","label": "Avg Return 12 9 3 1 months","group": "Momentum",
    "description": "## Avg Return 12 9 3 1 months\n\n**What it means:** Average of raw returns across 12, 9, 3, and 1 month periods. Combines established trends with recent momentum acceleration.\n\n**Why investors use it:** Helps identify stocks where momentum remains strong and is still improving."},
    {"value": "average_return_12_9_3_months","label": "Avg Return 12 9 3 months","group": "Momentum",
    "description": "## Avg Return 12 9 3 months\n\n**What it means:** Average of raw returns across 12, 9, and 3 month periods. Balances long-term momentum with recent strength.\n\n**Why investors use it:** Useful for finding stocks that are both trending and attracting fresh buying interest."},
    {"value": "average_return_12_3_1_months","label": "Avg Return 12 3 1 months","group": "Momentum",
    "description": "## Avg Return 12 3 1 months\n\n**What it means:** Average of raw returns across 12, 3, and 1 month periods. Combines long-term trend with short-term momentum acceleration.\n\n**Why investors use it:** Helps identify stocks that are both established leaders and currently gaining strength."},
    {"value": "average_return_12_3_months",      "label": "Avg Return 12 3 months",  "group": "Momentum",
     "description": "## Avg Return 12 3 months\n\n**What it means:** Average of raw returns across 12 and 3 month periods. Blends long-term trend with short-term strength.\n\n**Why investors use it:** Captures stocks with both sustained and recent momentum."},
    {"value": "roc_12m_minus_1m_pct",      "label": "Return 12 minus 1 months",  "group": "Technical",
     "description": "## Return 12 minus 1 months\n\n**What it means:** 12-month return minus 1-month return. High values indicate strong long-term momentum with a recent pause — a classic momentum strategy signal.\n\n**Why investors use it:** Avoids stocks that have spiked recently and may be due for a pullback."},
    {"value": "roc_12m_minus_2m_pct",      "label": "Return 12 minus 2 months",  "group": "Technical",
     "description": "## Return 12 minus 2 months\n\n**What it means:** 12-month return minus 2-month return. Filters out stocks with only recent spikes, favoring sustained long-term trends.\n\n**Why investors use it:** A variation of the momentum minus reversal strategy."},
    {"value": "1y_sharpe_return_per_beta",      "label": "Sharpe Return 1 Year per Beta",  "group": "Risk-Adjusted Returns",
     "description": "## Sharpe Return 1 Year per Beta\n\n**What it means:** 1-year Sharpe return divided by Beta. Rewards high-quality returns while penalizing market sensitivity.\n\n**Why investors use it:** Identifies stocks delivering the best risk-adjusted returns relative to their market exposure."},
    {"value": "avg_sharpe_12_9_6_3_per_beta",      "label": "Avg Sharpe Return 12 9 6 3 per Beta",  "group": "Risk-Adjusted Returns",
     "description": "## Avg Sharpe Return 12 9 6 3 per Beta\n\n**What it means:** Average Sharpe across 12, 9, 6, 3 months divided by Beta. Identifies the most efficient risk-adjusted momentum stocks.\n\n**Why investors use it:** The most comprehensive risk-efficiency ranking available."},
    {"value": "avg_sharpe_of_12_6_3_per_beta",      "label": "Avg Sharpe Return 12 6 3 per Beta",  "group": "Risk-Adjusted Returns",
     "description": "## Avg Sharpe Return 12 6 3 per Beta\n\n**What it means:** Average Sharpe across 12, 6, 3 months divided by Beta. A balanced risk-efficiency ranking.\n\n**Why investors use it:** Focuses on key timeframes while adjusting for market sensitivity."},
    {"value": "avg_sharpe_of_12_6_per_beta",      "label": "Avg Sharpe Return 12 6 per Beta",  "group": "Risk-Adjusted Returns",
     "description": "## Avg Sharpe Return 12 6 per Beta\n\n**What it means:** Average Sharpe across 12 and 6 months divided by Beta. Focuses on medium to long-term risk-efficient momentum.\n\n**Why investors use it:** Ideal for investors seeking stable, low-risk momentum stocks."},
    {"value": "volar","label": "Volar","group": "Trend Efficiency",
     "description": "## Volar\n\n**What it means:** Measures 1-year return relative to the stock's average daily price movement. Higher values indicate the stock generated stronger returns with comparatively smoother price action.\n\n**Why investors use it:** Helps identify efficient trends where returns were achieved without excessive day-to-day volatility."}
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
