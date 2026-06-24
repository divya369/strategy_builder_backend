"""Drop unused top_rank column from backtest_run.

Revision ID: f3a4b5c6d7e8
Revises: e2f3a4b5c6d7
Create Date: 2026-06-23

Changes:
- Drop top_rank from backtest_run (never read or written — portfolio_size
  and wrh serve its purpose)
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers
revision = "f3a4b5c6d7e8"
down_revision = "cf5f70da9f70"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("backtest_run", "top_rank")


def downgrade() -> None:
    op.add_column(
        "backtest_run",
        sa.Column("top_rank", sa.Integer(), nullable=False, server_default="30"),
    )
