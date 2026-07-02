"""Add broker_orderbook_daily and postback_log tables

Revision ID: n8o9p0q1r2s3
Revises: m7n8o9p0q1r2
Create Date: 2026-07-01
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = 'n8o9p0q1r2s3'
down_revision = 'm7n8o9p0q1r2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── broker_orderbook_daily: daily filtered orderbook backup per broker_account ──
    op.create_table(
        'broker_orderbook_daily',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('broker_account_id', UUID(as_uuid=True), sa.ForeignKey('broker_account.id', ondelete='CASCADE'), nullable=False),
        sa.Column('broker_user_id', sa.String(100), nullable=True),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('order_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('filtered_orderbook', JSONB, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('ix_broker_orderbook_daily_broker_account_id', 'broker_orderbook_daily', ['broker_account_id'])
    op.create_index('ix_broker_orderbook_daily_date', 'broker_orderbook_daily', ['date'])
    op.create_unique_constraint('uq_orderbook_daily_account_date', 'broker_orderbook_daily', ['broker_account_id', 'date'])

    # ── postback_log: append-only postback audit log ──
    op.create_table(
        'postback_log',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('received_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('publisher_tag', sa.String(8), nullable=True),
        sa.Column('order_id', sa.String(), nullable=True),
        sa.Column('status', sa.String(30), nullable=True),
        sa.Column('tradingsymbol', sa.String(), nullable=True),
        sa.Column('raw_payload', JSONB, nullable=False),
        sa.Column('matched_strategy_id', UUID(as_uuid=True), nullable=True),
        sa.Column('processing_note', sa.Text(), nullable=True),
    )
    op.create_index('ix_postback_log_received_at', 'postback_log', ['received_at'])
    op.create_index('ix_postback_log_publisher_tag', 'postback_log', ['publisher_tag'])


def downgrade() -> None:
    op.drop_table('postback_log')
    op.drop_table('broker_orderbook_daily')
