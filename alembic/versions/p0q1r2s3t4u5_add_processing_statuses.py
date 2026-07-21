"""Add INITIAL_PROCESSING, REBALANCE_PROCESSING, EXIT_PROCESSING status enum values.

Revision ID: p0q1r2s3t4u5
Revises: o9p0q1r2s3t4
Create Date: 2026-07-21

Changes:
- Add INITIAL_PROCESSING to strategy_builder_live_status_enum
- Add REBALANCE_PROCESSING to strategy_builder_live_status_enum
- Add EXIT_PROCESSING to strategy_builder_live_status_enum

These statuses lock the strategy during active broker order processing,
preventing race conditions from user refreshes and stale recovery.
"""
from alembic import op

revision = 'p0q1r2s3t4u5'
down_revision = 'o9p0q1r2s3t4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE strategy_builder_live_status_enum ADD VALUE IF NOT EXISTS 'INITIAL_PROCESSING'")
    op.execute("ALTER TYPE strategy_builder_live_status_enum ADD VALUE IF NOT EXISTS 'REBALANCE_PROCESSING'")
    op.execute("ALTER TYPE strategy_builder_live_status_enum ADD VALUE IF NOT EXISTS 'EXIT_PROCESSING'")


def downgrade() -> None:
    # Cannot easily remove enum values in PostgreSQL
    pass
