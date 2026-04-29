# market_data.py
# All previous DB-backed market data tables (DailyStockPrices, BenchmarkPricesDaily,
# DailyScreenerData, RebalanceCalendar) have been removed.
#
# Data is now sourced from:
#   - Stock/Index OHLC  → equity_ohlc DB per-symbol tables (via equity_data_service)
#   - Screener data     → CSV files (via csv_data_service)
#   - Index constituents → CSV files (via csv_data_service)
#   - Rebalance dates   → Computed from screener CSV dates (via backtest_engine)
