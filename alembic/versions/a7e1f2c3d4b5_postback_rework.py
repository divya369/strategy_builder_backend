"""Rename tables, add postback columns, drop unused columns.

Revision ID: a7e1f2c3d4b5
Revises: 2ee80bd36713
Create Date: 2026-06-08

Changes:
- Rename all 8 strategy_builder_* tables (remove prefix)
- Add publisher_tag, broker_order_id, broker_status, broker_status_message,
  broker_raw_postback to buy_stock, sell_stock, circuit_stock
- Drop triggered_price from buy_stock
- Drop backtest_run_id, end_date, last_rebalance_date from automate_equity
- Drop broker_meta from broker_account
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "a7e1f2c3d4b5"
down_revision = "2ee80bd36713"
branch_labels = None
depends_on = None

# Table renames: old_name -> new_name
TABLE_RENAMES = [
    ("strategy_builder_broker_account", "broker_account"),
    ("strategy_builder_automate_equity", "automate_equity"),
    ("strategy_builder_buy_stock", "buy_stock"),
    ("strategy_builder_sell_stock", "sell_stock"),
    ("strategy_builder_circuit_stock", "circuit_stock"),
    ("strategy_builder_tradelog_automate_equity", "tradelog_automate_equity"),
    ("strategy_builder_equitycurve_automate_equity", "equitycurve_automate_equity"),
    ("strategy_builder_publisher_basket", "publisher_basket"),
]


def upgrade() -> None:
    # ── 1. Rename tables ─────────────────────────────────────────────────
    for old_name, new_name in TABLE_RENAMES:
        op.rename_table(old_name, new_name)

    # ── 2. Rename constraints ────────────────────────────────────────────
    # UniqueConstraint on broker_account
    op.execute(
        "ALTER TABLE broker_account RENAME CONSTRAINT "
        "uq_strategy_builder_broker_account_user_broker_label "
        "TO uq_broker_account_user_broker_label"
    )
    # UniqueConstraint on publisher_basket
    op.execute(
        "ALTER TABLE publisher_basket RENAME CONSTRAINT "
        "uq_strategy_builder_publisher_basket_key "
        "TO uq_publisher_basket_key"
    )

    # ── 3. Drop unused columns ───────────────────────────────────────────
    op.drop_column("buy_stock", "triggered_price")
    op.drop_column("automate_equity", "backtest_run_id")
    op.drop_column("automate_equity", "end_date")
    op.drop_column("automate_equity", "last_rebalance_date")
    op.drop_column("broker_account", "broker_meta")

    # ── 4. Add postback columns to buy_stock ─────────────────────────────
    op.add_column("buy_stock", sa.Column("publisher_tag", sa.String(8), nullable=True))
    op.add_column("buy_stock", sa.Column("broker_order_id", sa.String(50), nullable=True))
    op.add_column("buy_stock", sa.Column("broker_status", sa.String(30), nullable=True))
    op.add_column("buy_stock", sa.Column("broker_status_message", sa.Text(), nullable=True))
    op.add_column("buy_stock", sa.Column("broker_raw_postback", postgresql.JSONB(), nullable=True))
    op.create_unique_constraint("uq_buy_stock_publisher_tag", "buy_stock", ["publisher_tag"])
    op.create_index("ix_buy_stock_publisher_tag", "buy_stock", ["publisher_tag"])
    op.create_index("ix_buy_stock_broker_order_id", "buy_stock", ["broker_order_id"])

    # ── 5. Add postback columns to sell_stock ────────────────────────────
    op.add_column("sell_stock", sa.Column("publisher_tag", sa.String(8), nullable=True))
    op.add_column("sell_stock", sa.Column("broker_order_id", sa.String(50), nullable=True))
    op.add_column("sell_stock", sa.Column("broker_status", sa.String(30), nullable=True))
    op.add_column("sell_stock", sa.Column("broker_status_message", sa.Text(), nullable=True))
    op.add_column("sell_stock", sa.Column("broker_raw_postback", postgresql.JSONB(), nullable=True))
    op.create_unique_constraint("uq_sell_stock_publisher_tag", "sell_stock", ["publisher_tag"])
    op.create_index("ix_sell_stock_publisher_tag", "sell_stock", ["publisher_tag"])
    op.create_index("ix_sell_stock_broker_order_id", "sell_stock", ["broker_order_id"])

    # ── 6. Add postback columns to circuit_stock ─────────────────────────
    op.add_column("circuit_stock", sa.Column("publisher_tag", sa.String(8), nullable=True))
    op.add_column("circuit_stock", sa.Column("broker_order_id", sa.String(50), nullable=True))
    op.add_column("circuit_stock", sa.Column("broker_status", sa.String(30), nullable=True))
    op.add_column("circuit_stock", sa.Column("broker_status_message", sa.Text(), nullable=True))
    op.add_column("circuit_stock", sa.Column("broker_raw_postback", postgresql.JSONB(), nullable=True))
    op.create_unique_constraint("uq_circuit_stock_publisher_tag", "circuit_stock", ["publisher_tag"])
    op.create_index("ix_circuit_stock_publisher_tag", "circuit_stock", ["publisher_tag"])
    op.create_index("ix_circuit_stock_broker_order_id", "circuit_stock", ["broker_order_id"])


def downgrade() -> None:
    # ── Remove postback columns ──────────────────────────────────────────
    for table in ("buy_stock", "sell_stock", "circuit_stock"):
        op.drop_index(f"ix_{table}_broker_order_id", table_name=table)
        op.drop_index(f"ix_{table}_publisher_tag", table_name=table)
        op.drop_constraint(f"uq_{table}_publisher_tag", table, type_="unique")
        op.drop_column(table, "broker_raw_postback")
        op.drop_column(table, "broker_status_message")
        op.drop_column(table, "broker_status")
        op.drop_column(table, "broker_order_id")
        op.drop_column(table, "publisher_tag")

    # ── Restore dropped columns ──────────────────────────────────────────
    op.add_column("broker_account", sa.Column("broker_meta", postgresql.JSONB(), nullable=True))
    op.add_column("automate_equity", sa.Column("last_rebalance_date", sa.Date(), nullable=True))
    op.add_column("automate_equity", sa.Column("end_date", sa.Date(), nullable=True))
    op.add_column("automate_equity", sa.Column("backtest_run_id", postgresql.UUID(), nullable=True))
    op.add_column("buy_stock", sa.Column("triggered_price", sa.Float(), nullable=True))

    # ── Rename constraints back ──────────────────────────────────────────
    op.execute(
        "ALTER TABLE publisher_basket RENAME CONSTRAINT "
        "uq_publisher_basket_key "
        "TO uq_strategy_builder_publisher_basket_key"
    )
    op.execute(
        "ALTER TABLE broker_account RENAME CONSTRAINT "
        "uq_broker_account_user_broker_label "
        "TO uq_strategy_builder_broker_account_user_broker_label"
    )

    # ── Rename tables back ───────────────────────────────────────────────
    for old_name, new_name in reversed(TABLE_RENAMES):
        op.rename_table(new_name, old_name)
