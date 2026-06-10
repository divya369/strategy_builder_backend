"""drop_user_id_fk_from_broker_account_and_automate_equity

Revision ID: ca7d9d6415e0
Revises: d1e2f3a4b5c6
Create Date: 2026-06-10 09:36:57.337104

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ca7d9d6415e0'
down_revision: Union[str, Sequence[str], None] = 'd1e2f3a4b5c6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Drop user_id foreign key constraints from broker_account and automate_equity."""
    op.drop_constraint('strategy_builder_automate_equity_user_id_fkey', 'automate_equity', type_='foreignkey')
    op.drop_constraint('strategy_builder_broker_account_user_id_fkey', 'broker_account', type_='foreignkey')


def downgrade() -> None:
    """Re-create user_id foreign key constraints on broker_account and automate_equity."""
    op.create_foreign_key(
        'strategy_builder_broker_account_user_id_fkey',
        'broker_account', 'users',
        ['user_id'], ['id'],
        ondelete='CASCADE'
    )
    op.create_foreign_key(
        'strategy_builder_automate_equity_user_id_fkey',
        'automate_equity', 'users',
        ['user_id'], ['id'],
        ondelete='CASCADE'
    )
