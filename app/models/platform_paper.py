"""
Platform paper-trading tables for ready-to-use (role="platform") strategies.

Purpose:
- Track admin/platform strategies as a virtual (paper) portfolio that continues
  daily AFTER the historical backtest, without ever liquidating at an end date.
- Physically separate from live-investment tables (automate_equity etc.) so
  paper flows can never touch real-money broker paths. Industry rule:
  shared functions, separate storage.

IMPORTANT — column-name compatibility:
Column names are kept IDENTICAL to the live tables (including the FK name
`automate_equity_ra_id`, which here references platform_paper_portfolio.id).
This is intentional: the shared equitycase-style functions in
live_investment_service (model_df, update_tradelog, update_equitycurve,
get_sell_df_investment, get_buy_df_investment, upsert_df_to_db, ...) operate on
DataFrames keyed by these exact column names. Identical names = 100% function
reuse with ZERO modification of the running live-investment code.

No broker/publisher columns exist here (publisher_tag, broker_status, ...):
fills are synthetic (order_id = "PAPER-xxxxxxxx", actual_* = close price).
"""
import uuid
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Date, DateTime, ForeignKey,
    Text, func
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.models.base import Base


class PaperPortfolio(Base):
    """
    Paper equivalent of LiveStrategy/automate_equity for platform strategies.

    Duck-typed into update_equitycurve()/make_equitycurve_table() which only
    read: .id, .initial_aum, .universe_json — all present here.
    """
    __tablename__ = "platform_paper_portfolio"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)

    # Source linkage
    screener_id = Column(UUID(as_uuid=True), ForeignKey("screeners.id", ondelete="SET NULL"), nullable=True, index=True)
    screener_version_id = Column(UUID(as_uuid=True), ForeignKey("screener_versions.id", ondelete="SET NULL"), nullable=True, index=True)
    backtest_run_id = Column(UUID(as_uuid=True), ForeignKey("backtest_run.id", ondelete="SET NULL"), nullable=True, index=True)

    strategy_name = Column(String(255), nullable=True)

    # Rebalance params — copied from the source backtest run at start
    portfolio_size = Column(Integer, nullable=False)
    worst_hold_rank = Column(Integer, nullable=False)
    rebalance_frequency = Column(String(20), nullable=False)  # WEEKLY / MONTHLY

    # Equitycase-style AUM fields (same names as LiveStrategy)
    initial_aum = Column(Float, nullable=False, default=0.0)
    cash = Column(Float, nullable=False, default=0.0)
    stock_value = Column(Float, nullable=False, default=0.0)
    final_aum = Column(Float, nullable=False, default=0.0)
    pnl = Column(Float, nullable=False, default=0.0)
    todays_pnl = Column(Float, nullable=False, default=0.0)

    # Plain string status — paper has no broker states
    status = Column(String(20), nullable=False, default="ACTIVE", server_default="ACTIVE", index=True)  # ACTIVE / PAUSED / ARCHIVED

    start_date = Column(Date, nullable=False)          # backtest from_date (curve history start)
    backfill_end_date = Column(Date, nullable=True)    # backtest to_date (paper continues after this)
    last_updated_date = Column(Date, nullable=True)
    next_rebalance_date = Column(Date, nullable=True)

    # Config snapshot for audit + benchmark resolution (_get_benchmark_index_name)
    filters_json = Column(JSONB, nullable=True)
    universe_json = Column(JSONB, nullable=True)
    ranking_json = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class PaperBuyStock(Base):
    """Same column layout as LiveBuyStock minus publisher/broker columns."""
    __tablename__ = "platform_paper_buy_stock"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("platform_paper_portfolio.id", ondelete="CASCADE"), nullable=False, index=True)
    tradingsymbol = Column(String, nullable=False)
    isin = Column(String, nullable=False, default="")
    date = Column(Date, nullable=False)
    qty = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    weightage = Column(Float, nullable=True)
    actual_qty = Column(Integer, nullable=True)
    actual_price = Column(Float, nullable=True)
    actual_amount = Column(Float, nullable=True)
    stoploss = Column(Float, nullable=True)
    volatility = Column(Float, nullable=True)
    order_id = Column(String, nullable=True, index=True)   # "PAPER-xxxxxxxx"
    circuit = Column(Boolean, default=False, nullable=True)
    updated_in_tradelog = Column(Boolean, default=False, nullable=True)


class PaperSellStock(Base):
    """Same column layout as LiveSellStock minus publisher/broker columns."""
    __tablename__ = "platform_paper_sell_stock"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("platform_paper_portfolio.id", ondelete="CASCADE"), nullable=False, index=True)
    tradingsymbol = Column(String, nullable=False)
    isin = Column(String, nullable=False, default="")
    date = Column(Date, nullable=False)
    qty = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    actual_qty = Column(Integer, nullable=True)
    actual_price = Column(Float, nullable=True)
    actual_amount = Column(Float, nullable=True)
    order_id = Column(String, nullable=True, index=True)   # "PAPER-xxxxxxxx"
    method = Column(String, nullable=True)
    circuit = Column(Boolean, default=False, nullable=True)
    updated_in_tradelog = Column(Boolean, default=False, nullable=True)


class PaperTradelog(Base):
    """Same column layout as LiveTradelog (tradelog_automate_equity)."""
    __tablename__ = "platform_paper_tradelog"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("platform_paper_portfolio.id", ondelete="CASCADE"), nullable=False, index=True)
    tradingsymbol = Column(String, nullable=False)
    isin = Column(String, nullable=False, default="")
    buy_date = Column(Date, nullable=False)
    sell_date = Column(Date, nullable=True)
    hold = Column(Integer, nullable=False)
    weightage = Column(Float, nullable=True)
    buy_qty = Column(Integer, nullable=False)
    buy_price = Column(Float, nullable=False)
    buy_amount = Column(Float, nullable=False)
    sell_qty = Column(Integer, nullable=True)
    sell_price = Column(Float, nullable=True)
    sell_amount = Column(Float, nullable=True)
    pyramiding = Column(Integer, nullable=True)
    volatility = Column(Float, nullable=True)
    ltp = Column(Float, nullable=True)
    stoploss = Column(Float, nullable=True)
    risk = Column(Float, nullable=True)
    risk_percent = Column(Float, nullable=True)
    current_value = Column(Float, nullable=True)
    unrealised_pnl = Column(Float, nullable=True)
    realised_pnl = Column(Float, nullable=True)
    profit_percent = Column(Float, nullable=True)
    buy_charges = Column(Float, nullable=True)
    sell_charges = Column(Float, nullable=True)
    active = Column(Boolean, nullable=False, default=False)
    buy_order_id = Column(String, nullable=True, index=True)
    sell_order_id = Column(String, nullable=True, index=True)
    pyramiding_data = Column(String, nullable=True)
    profit_booking_data = Column(String, nullable=True)


class PaperEquityCurve(Base):
    """Same column layout as LiveEquityCurve (equitycurve_automate_equity)."""
    __tablename__ = "platform_paper_equitycurve"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("platform_paper_portfolio.id", ondelete="CASCADE"), nullable=False, index=True)
    date = Column(Date, nullable=False)
    total_days = Column(Integer, nullable=False)
    portfolio_size = Column(Integer, nullable=True)
    stocks_value = Column(Float, nullable=True)
    cash = Column(Float, nullable=True)
    aum = Column(Float, nullable=True)
    index_price = Column(Float, nullable=True)
    strategy_roc = Column(Float, nullable=True)
    index_roc = Column(Float, nullable=True)
    strategy_daily_return = Column(Float, nullable=True)
    index_daily_return = Column(Float, nullable=True)
    strategy_daily_performance = Column(Float, nullable=True)
    index_daily_performance = Column(Float, nullable=True)
    unrealised_pnl = Column(Float, nullable=True)
    realised_pnl = Column(Float, nullable=True)
    total_pnl = Column(Float, nullable=True)
    winning_trades = Column(Integer, nullable=True)
    losing_trades = Column(Integer, nullable=True)
    total_trades = Column(Integer, nullable=True)
    winning_percent = Column(Float, nullable=True)
    losing_percent = Column(Float, nullable=True)
    avg_win = Column(Float, nullable=True)
    avg_loss = Column(Float, nullable=True)
    rr = Column(Float, nullable=True)
    profit_factor = Column(Float, nullable=True)
    biggest_winning_trade = Column(Float, nullable=True)
    biggest_losing_trade = Column(Float, nullable=True)
    expectancy = Column(Float, nullable=True)
    avg_profit_per_day = Column(Float, nullable=True)
    max_dd_percent = Column(Float, nullable=True)
    max_dd_absolute = Column(Float, nullable=True)
    current_dd_percent = Column(Float, nullable=True)
    sqn = Column(Float, nullable=True)
    k_multiple = Column(Float, nullable=True)
    sharpe = Column(Float, nullable=True)
    calmar = Column(Float, nullable=True)
    sortino_ratio = Column(Float, nullable=True)
    equitycurve_percent = Column(Float, nullable=True)
    cagr_percent = Column(Float, nullable=True)
    neg_2sd = Column(Float, nullable=True)
    equitycurve_avg = Column(Float, nullable=True)
    pos_2sd = Column(Float, nullable=True)
    total_charges = Column(Float, nullable=True)
    rebalance = Column(Boolean, default=False, nullable=True)
    weekly_return = Column(Float, nullable=True)
    monthly_return = Column(Float, nullable=True)
    quarterly_return = Column(Float, nullable=True)
    yearly_return = Column(Float, nullable=True)
    benchmark_price = Column(Float, nullable=True)
    benchmark_roc = Column(Float, nullable=True)
    benchmark_daily_return = Column(Float, nullable=True)
    benchmark_daily_performance = Column(Float, nullable=True)
