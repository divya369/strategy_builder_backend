"""
Backtest result models: daily NAV, rebalance events, summary statistics,
drawdown episodes, and monthly returns.
All use UUID primary keys and foreign keys.
"""
import uuid
from sqlalchemy import (
    Column, Integer, String, Date, DateTime, Numeric, JSON,
    ForeignKey, Boolean, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.models.base import Base


class BacktestDailyNav(Base):
    """One row per trading date: portfolio and benchmark NAV, drawdown, cost."""
    __tablename__ = "backtest_daily_nav"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id = Column(UUID(as_uuid=True), ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    portfolio_return_gross = Column(Numeric(18, 8), nullable=False)
    portfolio_return_net = Column(Numeric(18, 8), nullable=False)
    portfolio_nav_gross = Column(Numeric(18, 8), nullable=False)
    portfolio_nav_net = Column(Numeric(18, 8), nullable=False)
    benchmark_return = Column(Numeric(18, 8), nullable=True)
    benchmark_nav = Column(Numeric(18, 8), nullable=True)
    running_peak_nav = Column(Numeric(18, 8), nullable=True)
    drawdown = Column(Numeric(18, 8), nullable=True)
    daily_turnover = Column(Numeric(18, 8), nullable=True, default=0)
    daily_cost = Column(Numeric(18, 8), nullable=True, default=0)

    __table_args__ = (
        UniqueConstraint("backtest_run_id", "trade_date", name="uq_backtest_daily_nav_date"),
    )

    run = relationship("BacktestRun", back_populates="daily_navs")


class BacktestRebalanceEvent(Base):
    """Summary stats for each rebalance event (turnover, cost, position changes)."""
    __tablename__ = "backtest_rebalance_event"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id = Column(UUID(as_uuid=True), ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True)
    rebalance_date = Column(Date, nullable=False, index=True)
    portfolio_value_before = Column(Numeric(18, 8), nullable=False)
    portfolio_value_after = Column(Numeric(18, 8), nullable=False)
    turnover = Column(Numeric(18, 8), nullable=False)
    transaction_cost = Column(Numeric(18, 8), nullable=False, default=0)
    positions_before = Column(Integer, nullable=False)
    positions_after = Column(Integer, nullable=False)
    added_count = Column(Integer, nullable=False, default=0)
    dropped_count = Column(Integer, nullable=False, default=0)
    retained_count = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint("backtest_run_id", "rebalance_date", name="uq_backtest_rebalance_event_date"),
    )

    run = relationship("BacktestRun", back_populates="rebalance_events")


class BacktestSummary(Base):
    """
    Aggregated performance metrics for the entire backtest period.
    All metrics stored as a flat JSON dict — adding new metrics
    requires zero migrations, just compute in engine + add to config.
    """
    __tablename__ = "backtest_summary"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("backtest_run.id", ondelete="CASCADE"),
        nullable=False, unique=True, index=True,
    )
    metrics_json = Column(JSON, nullable=False, default=dict)

    run = relationship("BacktestRun", back_populates="summary")


class BacktestDrawdownEpisode(Base):
    """One row per continuous drawdown episode with peak/trough/recovery dates."""
    __tablename__ = "backtest_drawdown_episode"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id = Column(UUID(as_uuid=True), ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True)
    peak_date = Column(Date, nullable=False)
    trough_date = Column(Date, nullable=False)
    recovery_date = Column(Date, nullable=True)
    drawdown_pct = Column(Numeric(18, 8), nullable=False)
    peak_to_trough_days = Column(Integer, nullable=False)
    trough_to_recovery_days = Column(Integer, nullable=True)
    total_recovery_days = Column(Integer, nullable=True)

    run = relationship("BacktestRun", back_populates="drawdown_episodes")


class BacktestMonthlyReturn(Base):
    """Monthly portfolio vs benchmark returns for the heatmap view."""
    __tablename__ = "backtest_monthly_return"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id = Column(UUID(as_uuid=True), ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    monthly_return = Column(Numeric(18, 8), nullable=False)
    benchmark_monthly_return = Column(Numeric(18, 8), nullable=True)
    excess_monthly_return = Column(Numeric(18, 8), nullable=True)

    __table_args__ = (
        UniqueConstraint("backtest_run_id", "year", "month", name="uq_backtest_monthly_return_ym"),
    )

    run = relationship("BacktestRun", back_populates="monthly_returns")
