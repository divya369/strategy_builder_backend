"""
Backtest run and trade-level models.
UUID keys throughout. config_snapshot_json removed — all params are
stored in typed columns and the screener_version_id FK relationship.
This eliminates the duplication that existed between the JSON blob and
the individual columns.
"""
import uuid
from sqlalchemy import (
    Column, Integer, String, Text, Date, DateTime,
    ForeignKey, Numeric, Boolean
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.models.base import Base


class BacktestRun(Base):
    __tablename__ = "backtest_run"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    request_hash = Column(String(64), nullable=True, index=True, unique=True)

    # Ownership & screener linkage
    user_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    screener_id = Column(UUID(as_uuid=True), ForeignKey("screeners.id", ondelete="SET NULL"), nullable=True, index=True)
    screener_version_id = Column(UUID(as_uuid=True), ForeignKey("screener_versions.id", ondelete="SET NULL"), nullable=True, index=True)

    run_name = Column(String(255), nullable=True)
    benchmark_symbol = Column(String(50), nullable=True)  # e.g. "NIFTY_500"

    # Backtest parameters (authoritative — no JSON duplication)
    from_date = Column(Date, nullable=False)
    to_date = Column(Date, nullable=False)
    rebalance_frequency = Column(String(50), nullable=False, default="WEEKLY")
    portfolio_size = Column(Integer, nullable=False, default=30)
    top_rank = Column(Integer, nullable=False, default=30)
    wrh = Column(Integer, nullable=False, default=40)
    transaction_cost_bps = Column(Numeric(10, 4), nullable=False, default=0)
    slippage_bps = Column(Numeric(10, 4), nullable=False, default=0)
    initial_capital = Column(Numeric(20, 4), nullable=False, default=1_000_000)

    # Run metadata
    status = Column(String(50), nullable=False, default="QUEUED")  # QUEUED/RUNNING/COMPLETED/FAILED
    progress_pct = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Result relationships
    summary = relationship("BacktestSummary", back_populates="run", uselist=False, cascade="all, delete-orphan")
    daily_navs = relationship("BacktestDailyNav", back_populates="run", cascade="all, delete-orphan")
    rebalance_events = relationship("BacktestRebalanceEvent", back_populates="run", cascade="all, delete-orphan")
    rebalance_constituents = relationship("BacktestRebalanceConstituent", back_populates="run", cascade="all, delete-orphan")
    holding_periods = relationship("BacktestHoldingPeriod", back_populates="run", cascade="all, delete-orphan")
    drawdown_episodes = relationship("BacktestDrawdownEpisode", back_populates="run", cascade="all, delete-orphan")
    monthly_returns = relationship("BacktestMonthlyReturn", back_populates="run", cascade="all, delete-orphan")


class BacktestRebalanceConstituent(Base):
    """Per-rebalance-date basket membership: which symbols were BUY / RETAIN / SELL."""
    __tablename__ = "backtest_rebalance_constituent"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id = Column(UUID(as_uuid=True), ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True)
    rebalance_date = Column(Date, nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    rank_position = Column(Integer, nullable=True)
    action = Column(String(10), nullable=False)        # BUY / RETAIN / SELL
    target_weight = Column(Numeric(18, 8), nullable=False)
    is_new_entry = Column(Boolean, nullable=False, default=False)
    is_retained = Column(Boolean, nullable=False, default=False)
    is_exited = Column(Boolean, nullable=False, default=False)

    run = relationship("BacktestRun", back_populates="rebalance_constituents")


class BacktestHoldingPeriod(Base):
    """Trade log: one row per holding from entry to exit with P&L."""
    __tablename__ = "backtest_holding_period"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id = Column(UUID(as_uuid=True), ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)

    entry_date = Column(Date, nullable=False)
    exit_date = Column(Date, nullable=True)
    entry_rank = Column(Integer, nullable=True)
    entry_price = Column(Numeric(18, 8), nullable=False)
    exit_price = Column(Numeric(18, 8), nullable=True)
    entry_weight = Column(Numeric(18, 8), nullable=False)
    exit_weight = Column(Numeric(18, 8), nullable=True)
    holding_days = Column(Integer, nullable=True)
    gross_return = Column(Numeric(18, 8), nullable=True)
    net_return = Column(Numeric(18, 8), nullable=True)
    exit_reason = Column(String(50), nullable=True)  # NOT_IN_TOP_N / END_OF_BACKTEST

    run = relationship("BacktestRun", back_populates="holding_periods")
