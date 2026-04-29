"""Debug: trace why field_comparison gives 0 results."""
import json
from app.services.screener_execution_service import screener_execution_service as s
from app.services import csv_data_service
import pandas as pd

# 1) Check actual CSV column values side by side
latest = csv_data_service.get_latest_screener_date()
df = csv_data_service.get_screener_data(latest)
print(f"CSV date: {latest}, rows: {len(df)}")
print(f"Columns check: '1y_return_pct' present={('1y_return_pct' in df.columns)}, '50_days_ma' present={('50_days_ma' in df.columns)}")
print()

# Show sample values for both columns
sample = df[["tradingsymbol", "1y_return_pct", "50_days_ma", "close"]].head(10)
print("Sample data (first 10 rows):")
for _, row in sample.iterrows():
    sym = row["tradingsymbol"]
    ret = row["1y_return_pct"]
    ma50 = row["50_days_ma"]
    close = row["close"]
    print(f"  {sym:15s}  1y_return_pct={str(ret):>10s}  50_days_ma={str(ma50):>10s}  close={str(close):>10s}")

print()
print("The field_comparison filter does:  1y_return_pct > 50_days_ma")
print("  1y_return_pct is a PERCENTAGE (e.g. 45.2 means 45.2%)")
print("  50_days_ma is a PRICE in rupees (e.g. 2380.0)")
print()

# Count how many stocks have return_pct > 50_days_ma
ret_s = pd.to_numeric(df["1y_return_pct"], errors="coerce")
ma_s = pd.to_numeric(df["50_days_ma"], errors="coerce")
matches = (ret_s > ma_s).sum()
print(f"Stocks where 1y_return_pct > 50_days_ma: {matches}")

# 2) Run WITHOUT field_comparison
r1 = s.execute_adhoc(
    universe={"type": "index", "value": "NIFTY 500"},
    filters=[
        {"type": "metric_period_value", "field": "return_pct", "period": "1y", "operator": ">=", "value": 10},
        {"type": "metric_value", "field": "beta", "operator": "<=", "value": 1.5},
        {"type": "relative_level", "field": "moving_average", "relation": "above", "period": "200d"},
    ],
    ranking={"field": "1y_return_pct", "order": "desc"},
    limit=5, offset=0,
)
print(f"\nWITHOUT field_comparison: {r1['total_matches']} matches")

# 3) Run WITH field_comparison
r2 = s.execute_adhoc(
    universe={"type": "index", "value": "NIFTY 500"},
    filters=[
        {"type": "metric_period_value", "field": "return_pct", "period": "1y", "operator": ">=", "value": 10},
        {"type": "metric_value", "field": "beta", "operator": "<=", "value": 1.5},
        {"type": "relative_level", "field": "moving_average", "relation": "above", "period": "200d"},
        {"type": "field_comparison", "field": "compare_params", "left_field": "return_pct", "left_period": "1y", "operator": ">", "right_field": "moving_average", "right_period": "50d"},
    ],
    ranking={"field": "1y_return_pct", "order": "desc"},
    limit=5, offset=0,
)
print(f"WITH field_comparison: {r2['total_matches']} matches")
