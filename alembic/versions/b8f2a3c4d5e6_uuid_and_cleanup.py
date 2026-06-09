"""Drop broker_order_id, Integer-to-UUID PKs, add ALL_REJECTED status.

Revision ID: b8f2a3c4d5e6
Revises: a7e1f2c3d4b5
Create Date: 2026-06-09

Changes:
- Drop broker_order_id column + index from buy_stock, sell_stock, circuit_stock
- Convert Integer PK to UUID PK for buy_stock, sell_stock, circuit_stock,
  tradelog_automate_equity, equitycurve_automate_equity
- Add ALL_REJECTED to livestatus enum
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "b8f2a3c4d5e6"
down_revision = "a7e1f2c3d4b5"
branch_labels = None
depends_on = None

# Tables that need Integer->UUID PK migration
UUID_TABLES = [
    "buy_stock",
    "sell_stock",
    "circuit_stock",
    "tradelog_automate_equity",
    "equitycurve_automate_equity",
]


def upgrade() -> None:
    # ── 1. Drop broker_order_id columns + indexes ────────────────────────
    for table in ("buy_stock", "sell_stock", "circuit_stock"):
        op.drop_index(f"ix_{table}_broker_order_id", table_name=table)
        op.drop_column(table, "broker_order_id")

    # ── 2. Add ALL_REJECTED to livestatus enum ───────────────────────────
    # PostgreSQL enums need ALTER TYPE to add new values
    op.execute("ALTER TYPE strategy_builder_live_status_enum ADD VALUE IF NOT EXISTS 'ALL_REJECTED'")

    # ── 3. Convert Integer PK to UUID PK ─────────────────────────────────
    # PK constraint names still have old strategy_builder_ prefix from table renames
    pk_names = {
        "buy_stock": "strategy_builder_buy_stock_pkey",
        "sell_stock": "strategy_builder_sell_stock_pkey",
        "circuit_stock": "strategy_builder_circuit_stock_pkey",
        "tradelog_automate_equity": "strategy_builder_tradelog_automate_equity_pkey",
        "equitycurve_automate_equity": "strategy_builder_equitycurve_automate_equity_pkey",
    }
    for table in UUID_TABLES:
        old_pk = pk_names[table]
        # Step 1: Add a new UUID column with default
        op.execute(f"ALTER TABLE {table} ADD COLUMN uuid_id UUID DEFAULT gen_random_uuid() NOT NULL")
        # Step 2: Drop old integer PK constraint
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {old_pk}")
        # Step 3: Drop old integer id column
        op.drop_column(table, "id")
        # Step 4: Rename uuid_id to id
        op.alter_column(table, "uuid_id", new_column_name="id")
        # Step 5: Create new PK constraint
        op.create_primary_key(f"{table}_pkey", table, ["id"])


def downgrade() -> None:
    # ── Reverse UUID to Integer ──────────────────────────────────────────
    for table in reversed(UUID_TABLES):
        op.drop_constraint(f"{table}_pkey", table, type_="primary")
        op.drop_column(table, "id")
        op.add_column(table, sa.Column(
            "id", sa.Integer(), autoincrement=True, nullable=False,
        ))
        op.create_primary_key(f"{table}_pkey", table, ["id"])

    # Note: Cannot easily remove enum value in PostgreSQL
    # ALL_REJECTED will remain in the enum type

    # ── Restore broker_order_id columns ──────────────────────────────────
    for table in ("buy_stock", "sell_stock", "circuit_stock"):
        op.add_column(table, sa.Column("broker_order_id", sa.String(50), nullable=True))
        op.create_index(f"ix_{table}_broker_order_id", table, ["broker_order_id"])
