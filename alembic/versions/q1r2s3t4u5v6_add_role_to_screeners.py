"""Add role column to screeners.

Revision ID: q1r2s3t4u5v6
Revises: p0q1r2s3t4u5
Create Date: 2026-07-23

Changes:
- Add screeners.role VARCHAR(20) NOT NULL DEFAULT 'user' + index

Values:
- "user"     = normal user-created strategy (default; all existing rows)
- "platform" = ready-to-use strategy created by the admin backend
"""
import sqlalchemy as sa
from alembic import op

revision = 'q1r2s3t4u5v6'
down_revision = 'p0q1r2s3t4u5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'screeners',
        sa.Column('role', sa.String(length=20), nullable=False, server_default='user'),
    )
    op.create_index('ix_screeners_role', 'screeners', ['role'])


def downgrade() -> None:
    op.drop_index('ix_screeners_role', table_name='screeners')
    op.drop_column('screeners', 'role')
