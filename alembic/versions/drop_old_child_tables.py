"""Drop old child tables after JSONB migration

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-05-11

Phase 2 migration: Drop the 5 old child tables that are now replaced
by JSONB columns on backtest_summary. Data from these tables is no longer
written or read by the application.

Dropped tables:
  - backtest_daily_nav          → daily_nav_json
  - backtest_monthly_return     → monthly_returns_json
  - backtest_rebalance_event    → rebalance_events_json
  - backtest_rebalance_constituent → constituents_json
  - backtest_drawdown_episode   → drawdowns_json
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


# revision identifiers
revision = 'c3d4e5f6a7b8'
down_revision = 'b2c3d4e5f6a7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table('backtest_daily_nav')
    op.drop_table('backtest_monthly_return')
    op.drop_table('backtest_rebalance_event')
    op.drop_table('backtest_rebalance_constituent')
    op.drop_table('backtest_drawdown_episode')


def downgrade() -> None:
    # Recreate tables if needed (schema only, data not recoverable)
    op.create_table(
        'backtest_daily_nav',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('backtest_run_id', UUID(as_uuid=True), sa.ForeignKey('backtest_run.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('trade_date', sa.Date, nullable=False, index=True),
        sa.Column('portfolio_return_gross', sa.Numeric(18, 8), nullable=False),
        sa.Column('portfolio_return_net', sa.Numeric(18, 8), nullable=False),
        sa.Column('portfolio_nav_gross', sa.Numeric(18, 8), nullable=False),
        sa.Column('portfolio_nav_net', sa.Numeric(18, 8), nullable=False),
        sa.Column('benchmark_return', sa.Numeric(18, 8), nullable=True),
        sa.Column('benchmark_nav', sa.Numeric(18, 8), nullable=True),
        sa.Column('running_peak_nav', sa.Numeric(18, 8), nullable=True),
        sa.Column('drawdown', sa.Numeric(18, 8), nullable=True),
        sa.Column('daily_turnover', sa.Numeric(18, 8), nullable=True),
        sa.Column('daily_cost', sa.Numeric(18, 8), nullable=True),
        sa.UniqueConstraint('backtest_run_id', 'trade_date', name='uq_backtest_daily_nav_date'),
    )
    op.create_table(
        'backtest_monthly_return',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('backtest_run_id', UUID(as_uuid=True), sa.ForeignKey('backtest_run.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('year', sa.Integer, nullable=False),
        sa.Column('month', sa.Integer, nullable=False),
        sa.Column('monthly_return', sa.Numeric(18, 8), nullable=False),
        sa.Column('benchmark_monthly_return', sa.Numeric(18, 8), nullable=True),
        sa.Column('excess_monthly_return', sa.Numeric(18, 8), nullable=True),
        sa.UniqueConstraint('backtest_run_id', 'year', 'month', name='uq_backtest_monthly_return_ym'),
    )
    op.create_table(
        'backtest_rebalance_event',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('backtest_run_id', UUID(as_uuid=True), sa.ForeignKey('backtest_run.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('rebalance_date', sa.Date, nullable=False, index=True),
        sa.Column('portfolio_value_before', sa.Numeric(18, 8), nullable=False),
        sa.Column('portfolio_value_after', sa.Numeric(18, 8), nullable=False),
        sa.Column('turnover', sa.Numeric(18, 8), nullable=False),
        sa.Column('transaction_cost', sa.Numeric(18, 8), nullable=False),
        sa.Column('positions_before', sa.Integer, nullable=False),
        sa.Column('positions_after', sa.Integer, nullable=False),
        sa.Column('added_count', sa.Integer, nullable=False),
        sa.Column('dropped_count', sa.Integer, nullable=False),
        sa.Column('retained_count', sa.Integer, nullable=False),
        sa.UniqueConstraint('backtest_run_id', 'rebalance_date', name='uq_backtest_rebalance_event_date'),
    )
    op.create_table(
        'backtest_rebalance_constituent',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('backtest_run_id', UUID(as_uuid=True), sa.ForeignKey('backtest_run.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('rebalance_date', sa.Date, nullable=False, index=True),
        sa.Column('symbol', sa.String(50), nullable=False, index=True),
        sa.Column('rank_position', sa.Integer, nullable=True),
        sa.Column('action', sa.String(10), nullable=False),
        sa.Column('target_weight', sa.Numeric(18, 8), nullable=False),
        sa.Column('is_new_entry', sa.Boolean, nullable=False),
        sa.Column('is_retained', sa.Boolean, nullable=False),
        sa.Column('is_exited', sa.Boolean, nullable=False),
    )
    op.create_table(
        'backtest_drawdown_episode',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('backtest_run_id', UUID(as_uuid=True), sa.ForeignKey('backtest_run.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('peak_date', sa.Date, nullable=False),
        sa.Column('trough_date', sa.Date, nullable=False),
        sa.Column('recovery_date', sa.Date, nullable=True),
        sa.Column('drawdown_pct', sa.Numeric(18, 8), nullable=False),
        sa.Column('peak_to_trough_days', sa.Integer, nullable=False),
        sa.Column('trough_to_recovery_days', sa.Integer, nullable=True),
        sa.Column('total_recovery_days', sa.Integer, nullable=True),
    )
