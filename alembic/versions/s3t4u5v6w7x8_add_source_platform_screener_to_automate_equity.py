"""Add source_platform_screener_id to screeners.

Revision ID: s3t4u5v6w7x8
Revises: r2s3t4u5v6w7
Create Date: 2026-07-25

Changes:
- Add screeners.source_platform_screener_id UUID NULL + index

Purpose:
- Marks a user screener that was ADOPTED from a platform (ready-to-use) strategy
  via 'Invest Now'. Points at the source platform screener.
- Lets Invest Now clone a platform strategy into the user's builder exactly ONCE
  per user; subsequent invests reuse that same clone (like any normal screener),
  so the normal Go Live restrictions apply.
- NULL for all existing rows and for ordinary user-built screeners.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = 's3t4u5v6w7x8'
down_revision = 'r2s3t4u5v6w7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'screeners',
        sa.Column('source_platform_screener_id', UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        'ix_screeners_source_platform_screener_id',
        'screeners',
        ['source_platform_screener_id'],
    )


def downgrade() -> None:
    op.drop_index('ix_screeners_source_platform_screener_id', table_name='screeners')
    op.drop_column('screeners', 'source_platform_screener_id')
