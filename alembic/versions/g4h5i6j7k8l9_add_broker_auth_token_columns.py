"""Add broker auth token columns to broker_account.

Revision ID: g4h5i6j7k8l9
Revises: f3a4b5c6d7e8
Create Date: 2026-06-30

Adds columns for storing encrypted broker access tokens obtained from
Publisher redirect-callback flow:
- access_token_encrypted: Fernet-encrypted access token
- token_date: Date the token was issued
- token_expires_at: When the token expires (~6 AM next day for Zerodha)
- last_request_token: Last request_token received from Publisher redirect
- last_authorised_at: Timestamp of last successful token exchange
- auth_status: CONNECTED / ACCOUNT_MISMATCH / TOKEN_EXCHANGE_FAILED
- broker_profile: JSONB with user profile from broker (user_id, name, email, etc.)

All columns are nullable — existing rows are unaffected.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "g4h5i6j7k8l9"
down_revision = "f3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("broker_account", sa.Column("access_token_encrypted", sa.Text(), nullable=True))
    op.add_column("broker_account", sa.Column("token_date", sa.Date(), nullable=True))
    op.add_column("broker_account", sa.Column("token_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("broker_account", sa.Column("last_request_token", sa.Text(), nullable=True))
    op.add_column("broker_account", sa.Column("last_authorised_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("broker_account", sa.Column("auth_status", sa.String(length=30), nullable=True))
    op.add_column("broker_account", sa.Column("broker_profile", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("broker_account", "broker_profile")
    op.drop_column("broker_account", "auth_status")
    op.drop_column("broker_account", "last_authorised_at")
    op.drop_column("broker_account", "last_request_token")
    op.drop_column("broker_account", "token_expires_at")
    op.drop_column("broker_account", "token_date")
    op.drop_column("broker_account", "access_token_encrypted")
