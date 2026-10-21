"""Add detail_json to platform_paper_portfolio.

Revision ID: v6w7x8y9z0a1
Revises: u5v6w7x8y9z0
Create Date: 2026-10-17

Changes:
- platform_paper_portfolio.detail_json (JSONB, nullable).

Purpose:
- The ready-to-use strategy detail page recomputed the full stitched
  backtest + paper summary on every page open, although the data changes only
  once a day. The summary is now computed once by the nightly paper update (and
  at paper start), stored here, and served as-is.
- Existing rows start NULL; the first page open computes and stores the
  payload once, so no backfill is needed.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = 'v6w7x8y9z0a1'
down_revision = 'u5v6w7x8y9z0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'platform_paper_portfolio',
        sa.Column('detail_json', postgresql.JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('platform_paper_portfolio', 'detail_json')
