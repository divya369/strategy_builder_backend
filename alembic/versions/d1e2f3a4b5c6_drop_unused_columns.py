"""Drop unused columns: auto_code, rms_code, top_rank, compare.

Revision ID: d1e2f3a4b5c6
Revises: c9f3b4d5e6f7
Create Date: 2026-06-09

Changes:
- Drop auto_code, rms_code from buy_stock, sell_stock, circuit_stock
- Drop top_rank from automate_equity
- Drop compare from equitycurve_automate_equity
"""
from alembic import op

# revision identifiers
revision = "d1e2f3a4b5c6"
down_revision = "c9f3b4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # buy_stock
    op.drop_column("buy_stock", "auto_code")
    op.drop_column("buy_stock", "rms_code")

    # sell_stock
    op.drop_column("sell_stock", "auto_code")
    op.drop_column("sell_stock", "rms_code")

    # circuit_stock
    op.drop_column("circuit_stock", "auto_code")
    op.drop_column("circuit_stock", "rms_code")

    # automate_equity
    op.drop_column("automate_equity", "top_rank")

    # equitycurve_automate_equity
    op.drop_column("equitycurve_automate_equity", "compare")


def downgrade() -> None:
    import sqlalchemy as sa

    op.add_column("equitycurve_automate_equity", sa.Column("compare", sa.Boolean(), nullable=True, server_default="false"))
    op.add_column("automate_equity", sa.Column("top_rank", sa.Integer(), nullable=True))
    op.add_column("circuit_stock", sa.Column("rms_code", sa.String(), nullable=True))
    op.add_column("circuit_stock", sa.Column("auto_code", sa.Boolean(), nullable=True, server_default="false"))
    op.add_column("sell_stock", sa.Column("rms_code", sa.String(), nullable=True))
    op.add_column("sell_stock", sa.Column("auto_code", sa.Boolean(), nullable=True, server_default="false"))
    op.add_column("buy_stock", sa.Column("rms_code", sa.String(), nullable=True))
    op.add_column("buy_stock", sa.Column("auto_code", sa.Boolean(), nullable=True, server_default="false"))
