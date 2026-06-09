"""Add CANCELLED status to livestatus enum.

Revision ID: c9f3b4d5e6f7
Revises: b8f2a3c4d5e6
Create Date: 2026-06-09

Changes:
- Add CANCELLED to strategy_builder_live_status_enum
"""
from alembic import op

# revision identifiers
revision = "c9f3b4d5e6f7"
down_revision = "b8f2a3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE strategy_builder_live_status_enum ADD VALUE IF NOT EXISTS 'CANCELLED'")


def downgrade() -> None:
    # Cannot easily remove enum value in PostgreSQL
    pass
