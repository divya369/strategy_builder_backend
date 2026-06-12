"""Add REBALANCE_SELL_COMPLETE status to livestatus enum.

Revision ID: e2f3a4b5c6d7
Revises: ca7d9d6415e0
Create Date: 2026-06-12

Changes:
- Add REBALANCE_SELL_COMPLETE to strategy_builder_live_status_enum
  (intermediate state between sell basket completion and buy basket start)
"""
from alembic import op

# revision identifiers
revision = "e2f3a4b5c6d7"
down_revision = "ca7d9d6415e0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE strategy_builder_live_status_enum ADD VALUE IF NOT EXISTS 'REBALANCE_SELL_COMPLETE'")


def downgrade() -> None:
    # Cannot easily remove enum value in PostgreSQL
    pass
