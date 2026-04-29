"""Drop backtest_batches table and remove batch_id from backtest_run.

The backtest_batches table was originally a grouping layer for multiple runs,
but every batch always contained exactly one run. All columns in
backtest_batches (user_id, screener_id, screener_version_id, status,
created_at) are already present in backtest_run, making the table fully
redundant.

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-23
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Drop the FK constraint + batch_id column from backtest_run
    with op.batch_alter_table("backtest_run") as batch_op:
        # Drop the foreign key constraint referencing backtest_batches
        batch_op.drop_constraint("backtest_run_batch_id_fkey", type_="foreignkey")
        batch_op.drop_column("batch_id")

    # 2. Drop the now-orphaned backtest_batches table
    op.drop_table("backtest_batches")


def downgrade() -> None:
    # Re-create backtest_batches
    op.create_table(
        "backtest_batches",
        sa.Column("id",                  sa.Integer(),  primary_key=True, index=True),
        sa.Column("user_id",             sa.Integer(),  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("screener_id",         sa.Integer(),  nullable=True),
        sa.Column("screener_version_id", sa.Integer(),  nullable=True),
        sa.Column("status",              sa.String(50), nullable=False, server_default="PENDING"),
        sa.Column("created_at",          sa.DateTime(), nullable=False),
    )

    # Re-add batch_id to backtest_run
    with op.batch_alter_table("backtest_run") as batch_op:
        batch_op.add_column(
            sa.Column("batch_id", sa.Integer(), nullable=True, index=True)
        )
        batch_op.create_foreign_key(
            "backtest_run_batch_id_fkey",
            "backtest_batches",
            ["batch_id"],
            ["id"],
            ondelete="CASCADE",
        )
