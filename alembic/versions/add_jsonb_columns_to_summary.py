"""Add JSONB columns to backtest_summary for hybrid architecture

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-05-11

Phase 1 migration: Add JSONB columns only.
Old child tables are NOT dropped — they remain for dual-write verification.
A future Phase 2 migration will drop them after successful testing.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers
revision = 'b2c3d4e5f6a7'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add JSONB columns to backtest_summary
    op.add_column('backtest_summary', sa.Column('daily_nav_json', JSONB, nullable=True))
    op.add_column('backtest_summary', sa.Column('monthly_returns_json', JSONB, nullable=True))
    op.add_column('backtest_summary', sa.Column('rebalance_events_json', JSONB, nullable=True))
    op.add_column('backtest_summary', sa.Column('constituents_json', JSONB, nullable=True))
    op.add_column('backtest_summary', sa.Column('drawdowns_json', JSONB, nullable=True))

    # Also upgrade metrics_json from JSON to JSONB for consistency
    op.alter_column('backtest_summary', 'metrics_json',
                    type_=JSONB,
                    existing_type=sa.JSON(),
                    existing_nullable=False)

    # NOTE: Old child tables (backtest_daily_nav, backtest_monthly_return,
    # backtest_rebalance_event, backtest_rebalance_constituent,
    # backtest_drawdown_episode) are intentionally NOT dropped here.
    # They will be dropped in a future migration after dual-write verification.


def downgrade() -> None:
    op.alter_column('backtest_summary', 'metrics_json',
                    type_=sa.JSON(),
                    existing_type=JSONB,
                    existing_nullable=False)
    op.drop_column('backtest_summary', 'drawdowns_json')
    op.drop_column('backtest_summary', 'constituents_json')
    op.drop_column('backtest_summary', 'rebalance_events_json')
    op.drop_column('backtest_summary', 'monthly_returns_json')
    op.drop_column('backtest_summary', 'daily_nav_json')
