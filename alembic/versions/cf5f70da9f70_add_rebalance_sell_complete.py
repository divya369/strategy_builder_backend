"""add_rebalance_sell_complete

Revision ID: cf5f70da9f70
Revises: e2f3a4b5c6d7
Create Date: 2026-06-15 10:34:27.212244

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cf5f70da9f70'
down_revision: Union[str, Sequence[str], None] = 'e2f3a4b5c6d7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE strategy_builder_live_status_enum ADD VALUE IF NOT EXISTS 'REBALANCE_SELL_COMPLETE'")


def downgrade() -> None:
    """Downgrade schema."""
    # Note: PostgreSQL does not support dropping a value from an ENUM type
    pass
