"""Scope the broker_account label uniqueness to active rows + normalize broker ids.

Revision ID: u5v6w7x8y9z0
Revises: t4u5v6w7x8y9
Create Date: 2026-08-18

Changes:
- Replace UNIQUE (user_id, broker, broker_account_label) with a PARTIAL unique
  index over the same columns WHERE is_active.
- Backfill broker_account.broker to lowercase and broker_account.broker_user_id
  to uppercase, both trimmed.

Purpose:
- Go Live upserts the broker account by broker_user_id but the table was unique
  on the label, so any row invisible to that lookup still reserved its nickname.
  Two ways that bit users: DELETE /broker-accounts only sets is_active=False,
  which held the nickname forever; and hand-typed client ids drift in case, so
  'xyz1234' on file never matched 'XYZ1234' on retry. Either way the endpoint
  fell through to an INSERT that raised IntegrityError -> 500.
- The partial index fixes the soft-delete half. The backfill converges existing
  rows onto the casing the API now writes and matches on, so locked_client_id
  and the postback client-id check compare like for like.

Note: downgrade re-creates the full constraint and will fail if a soft-deleted
row shares (user_id, broker, label) with another row — deactivate or relabel the
duplicates first.
"""
import sqlalchemy as sa
from alembic import op

revision = 'u5v6w7x8y9z0'
down_revision = 't4u5v6w7x8y9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        'uq_broker_account_user_broker_label',
        'broker_account',
        type_='unique',
    )
    op.create_index(
        'uq_broker_account_user_broker_label_active',
        'broker_account',
        ['user_id', 'broker', 'broker_account_label'],
        unique=True,
        postgresql_where=sa.text('is_active'),
    )

    # Normalize after the constraint swap: the label is untouched, so this can
    # only collapse rows that the partial index already permits.
    op.execute(
        """
        UPDATE broker_account
           SET broker = lower(btrim(broker)),
               broker_user_id = upper(btrim(broker_user_id))
         WHERE broker IS DISTINCT FROM lower(btrim(broker))
            OR broker_user_id IS DISTINCT FROM upper(btrim(broker_user_id))
        """
    )
    op.execute(
        """
        UPDATE automate_equity
           SET broker = lower(btrim(broker)),
               locked_client_id = upper(btrim(locked_client_id))
         WHERE broker IS DISTINCT FROM lower(btrim(broker))
            OR locked_client_id IS DISTINCT FROM upper(btrim(locked_client_id))
        """
    )


def downgrade() -> None:
    op.drop_index(
        'uq_broker_account_user_broker_label_active',
        table_name='broker_account',
    )
    op.create_unique_constraint(
        'uq_broker_account_user_broker_label',
        'broker_account',
        ['user_id', 'broker', 'broker_account_label'],
    )
