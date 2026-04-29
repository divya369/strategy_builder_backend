# app/api/v1/backtest_metric_formatter.py

OVERVIEW_METRICS_CONFIG = [
    ("cagr", "CAGR", "%", "Performance"),
    ("total_return", "Total Return", "%", "Performance"),
    ("max_drawdown", "Max Drawdown", "%", "Performance"),
    ("volatility", "Volatility", "%", "Performance"),
    ("sharpe", "Sharpe Ratio", "", "Performance"),
    ("sortino", "Sortino Ratio", "", "Performance"),
    ("calmar", "Calmar Ratio", "", "Performance"),

    ("best_month", "Best Month", "%", "Monthly"),
    ("worst_month", "Worst Month", "%", "Monthly"),
    ("avg_month", "Avg Month", "%", "Monthly"),
    ("positive_month_pct", "Positive Month %", "%", "Monthly"),

    ("benchmark_cagr", "Benchmark CAGR", "%", "Benchmark"),
    ("excess_cagr", "Excess CAGR (α)", "%", "Benchmark"),
    ("hit_ratio_vs_benchmark", "Hit Ratio vs Bench", "%", "Benchmark"),
    ("upside_capture", "Upside Capture", "", "Benchmark"),
    ("downside_capture", "Downside Capture", "", "Benchmark"),

    ("total_rebalances", "Total Rebalances", "#", "Turnover & Cost"),
    ("avg_turnover", "Avg Turnover", "%", "Turnover & Cost"),
    ("annualized_turnover", "Annualized Turnover", "%", "Turnover & Cost"),
    ("total_cost_drag", "Total Cost Drag", "%", "Turnover & Cost"),

    ("avg_holding_days", "Avg Holding Days", "days", "Holding"),
    ("median_holding_days", "Median Holding Days", "days", "Holding"),
    ("avg_retention_pct", "Avg Retention %", "%", "Holding"),
    ("avg_churn_pct", "Avg Churn %", "%", "Holding"),
]


def format_metric_value(value, unit: str):
    if value is None:
        return None

    value = float(value)

    if unit == "%":
        return round(value * 100, 2)

    if unit == "x":
        return round(value, 2)

    if unit == "#":
        return int(value)

    if unit == "days":
        return round(value, 1)

    return value