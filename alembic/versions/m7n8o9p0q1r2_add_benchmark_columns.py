"""Add benchmark columns to equitycurve_automate_equity

Revision ID: m7n8o9p0q1r2
Revises: g4h5i6j7k8l9
Create Date: 2026-07-01
"""
from alembic import op
import sqlalchemy as sa

revision = 'm7n8o9p0q1r2'
down_revision = 'g4h5i6j7k8l9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('equitycurve_automate_equity', sa.Column('benchmark_price', sa.Float(), nullable=True))
    op.add_column('equitycurve_automate_equity', sa.Column('benchmark_roc', sa.Float(), nullable=True))
    op.add_column('equitycurve_automate_equity', sa.Column('benchmark_daily_return', sa.Float(), nullable=True))
    op.add_column('equitycurve_automate_equity', sa.Column('benchmark_daily_performance', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('equitycurve_automate_equity', 'benchmark_daily_performance')
    op.drop_column('equitycurve_automate_equity', 'benchmark_daily_return')
    op.drop_column('equitycurve_automate_equity', 'benchmark_roc')
    op.drop_column('equitycurve_automate_equity', 'benchmark_price')
