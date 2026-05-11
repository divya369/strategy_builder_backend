"""Add qty, cost_drag, pnl_abs to backtest_holding_period; drop entry_weight, exit_weight

Revision ID: a1b2c3d4e5f6
Revises: 0ae3f5205846
Create Date: 2026-05-11
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers
revision = 'a1b2c3d4e5f6'
down_revision = '0ae3f5205846'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add new columns
    op.add_column('backtest_holding_period', sa.Column('qty', sa.Integer(), nullable=True))
    op.add_column('backtest_holding_period', sa.Column('cost_drag', sa.Numeric(18, 8), nullable=True))
    op.add_column('backtest_holding_period', sa.Column('pnl_abs', sa.Numeric(18, 8), nullable=True))

    # Set default for existing rows (if any)
    op.execute("UPDATE backtest_holding_period SET qty = 0 WHERE qty IS NULL")

    # Now make qty NOT NULL
    op.alter_column('backtest_holding_period', 'qty', nullable=False, server_default='0')

    # Drop old weight columns
    op.drop_column('backtest_holding_period', 'entry_weight')
    op.drop_column('backtest_holding_period', 'exit_weight')


def downgrade() -> None:
    # Re-add weight columns
    op.add_column('backtest_holding_period', sa.Column('exit_weight', sa.Numeric(18, 8), nullable=True))
    op.add_column('backtest_holding_period', sa.Column('entry_weight', sa.Numeric(18, 8), nullable=False, server_default='0'))

    # Drop new columns
    op.drop_column('backtest_holding_period', 'pnl_abs')
    op.drop_column('backtest_holding_period', 'cost_drag')
    op.drop_column('backtest_holding_period', 'qty')
