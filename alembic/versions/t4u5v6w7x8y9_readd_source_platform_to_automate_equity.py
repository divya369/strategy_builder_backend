"""Re-add source_platform_screener_id to automate_equity (clone-on-activation).

Revision ID: t4u5v6w7x8y9
Revises: s3t4u5v6w7x8
Create Date: 2026-07-25

Changes:
- Add automate_equity.source_platform_screener_id UUID NULL + index

Purpose:
- Platform 'Invest Now' now DEFERS creating the user's own screener until the
  strategy first goes ACTIVE. Until then the live strategy references the PLATFORM
  version directly, and this column records which platform screener to clone from
  at activation. NULL for ordinary user-built strategies and after the strategy
  has been re-pointed to its user clone.
- Complements screeners.source_platform_screener_id (which marks the adopted
  user clone, for get-or-create reuse).
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = 't4u5v6w7x8y9'
down_revision = 's3t4u5v6w7x8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'automate_equity',
        sa.Column('source_platform_screener_id', UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        'ix_automate_equity_source_platform_screener_id',
        'automate_equity',
        ['source_platform_screener_id'],
    )


def downgrade() -> None:
    op.drop_index('ix_automate_equity_source_platform_screener_id', table_name='automate_equity')
    op.drop_column('automate_equity', 'source_platform_screener_id')
