"""
Backtest result model: BacktestSummary with JSONB columns.
Daily NAV, rebalance events, constituents, drawdowns, and monthly returns
are stored as JSONB in the summary row (hybrid architecture).
Old child tables (backtest_daily_nav, backtest_rebalance_event,
backtest_drawdown_episode, backtest_monthly_return) are dropped.
"""
import uuid
from sqlalchemy import (
    Column, ForeignKey
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.models.base import Base


class BacktestSummary(Base):
    """
    Aggregated performance metrics for the entire backtest period.
    All metrics stored as a flat JSON dict — adding new metrics
    requires zero migrations, just compute in engine + add to config.
    Bulk display data (daily NAV, monthly returns, rebalance events,
    constituents, drawdowns) stored as JSONB columns to avoid
    high-row-count child tables.
    """
    __tablename__ = "backtest_summary"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("backtest_run.id", ondelete="CASCADE"),
        nullable=False, unique=True, index=True,
    )
    metrics_json = Column(JSONB, nullable=False, default=dict)
    daily_nav_json = Column(JSONB, nullable=True)           # replaces backtest_daily_nav table
    monthly_returns_json = Column(JSONB, nullable=True)     # replaces backtest_monthly_return table
    rebalance_events_json = Column(JSONB, nullable=True)    # replaces backtest_rebalance_event table
    constituents_json = Column(JSONB, nullable=True)        # replaces backtest_rebalance_constituent table
    drawdowns_json = Column(JSONB, nullable=True)           # replaces backtest_drawdown_episode table

    run = relationship("BacktestRun", back_populates="summary")
