"""Drop postback_log table — broker_raw_postback column on buy/sell/circuit tables is sufficient.

Revision ID: o9p0q1r2s3t4
Revises: n8o9p0q1r2s3
Create Date: 2026-07-03
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = 'o9p0q1r2s3t4'
down_revision = 'n8o9p0q1r2s3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop postback_log table — redundant with broker_raw_postback column on order tables
    op.drop_index('ix_postback_log_publisher_tag', table_name='postback_log')
    op.drop_index('ix_postback_log_received_at', table_name='postback_log')
    op.drop_table('postback_log')


def downgrade() -> None:
    # Re-create postback_log table
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
