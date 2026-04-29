"""Initial schema — all 17 tables at final production state.

Revision ID: 0001
Create Date: 2026-04-22

This is the single source-of-truth migration.
On a fresh server: alembic upgrade head
On existing DB:    alembic stamp head  (marks DB as up to date without running)
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:

    # ── 1. users ──────────────────────────────────────────────────────────────
    op.create_table(
        "users",
        sa.Column("id",              sa.Integer(),     primary_key=True),
        sa.Column("email",           sa.String(255),   nullable=False, unique=True, index=True),
        sa.Column("hashed_password", sa.String(255),   nullable=False),
        sa.Column("full_name",       sa.String(255),   nullable=True),
        sa.Column("is_active",       sa.Boolean(),     nullable=False, default=True),
        sa.Column("created_at",      sa.DateTime(),    nullable=True),
    )

    # ── 2. screeners ──────────────────────────────────────────────────────────
    op.create_table(
        "screeners",
        sa.Column("id",          sa.Integer(),   primary_key=True, index=True),
        sa.Column("user_id",     sa.Integer(),   sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("name",        sa.String(255), nullable=False),
        sa.Column("description", sa.Text(),      nullable=True),
        sa.Column("is_active",   sa.Boolean(),   nullable=False, default=True),
        sa.Column("created_at",  sa.DateTime(),  nullable=False),
        sa.Column("updated_at",  sa.DateTime(),  nullable=False),
        sa.Column("deleted_at",  sa.DateTime(),  nullable=True),
    )

    # ── 3. screener_versions ──────────────────────────────────────────────────
    op.create_table(
        "screener_versions",
        sa.Column("id",             sa.Integer(), primary_key=True, index=True),
        sa.Column("screener_id",    sa.Integer(), sa.ForeignKey("screeners.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("filters_json",   sa.JSON(),    nullable=False),
        sa.Column("universe_json",  sa.JSON(),    nullable=False),
        sa.Column("ranking_json",   sa.JSON(),    nullable=True),
        sa.Column("is_current",     sa.Boolean(), nullable=False, default=False),
        sa.Column("created_at",     sa.DateTime(), nullable=False),
    )

    # ── 4. index_constituents ─────────────────────────────────────────────────
    op.create_table(
        "index_constituents",
        sa.Column("id",            sa.Integer(),   primary_key=True, index=True),
        sa.Column("date",          sa.Date(),      nullable=False, index=True),
        sa.Column("index_name",    sa.String(100), nullable=False, index=True),
        sa.Column("tradingsymbol", sa.String(50),  nullable=False, index=True),
        sa.UniqueConstraint("date", "index_name", "tradingsymbol", name="uq_index_constituent"),
    )

    # ── 5. daily_screener_data ────────────────────────────────────────────────
    op.create_table(
        "daily_screener_data",
        sa.Column("id",            sa.Integer(),                         primary_key=True, index=True),
        sa.Column("date",          sa.Date(),                            nullable=False, index=True),
        sa.Column("tradingsymbol", sa.String(50),                        nullable=False, index=True),
        sa.Column("close",         sa.Numeric(18, 6),                    nullable=True),
        sa.Column("volume",        sa.BigInteger(),                      nullable=True),
        sa.Column("indicators",    postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default="{}"),
        sa.Index("idx_daily_screener_date_symbol", "date", "tradingsymbol", unique=True),
    )

    # ── 6. daily_stock_prices ─────────────────────────────────────────────────
    op.create_table(
        "daily_stock_prices",
        sa.Column("id",     sa.Integer(),      primary_key=True, index=True),
        sa.Column("date",   sa.Date(),         nullable=False, index=True),
        sa.Column("symbol", sa.String(50),     nullable=False, index=True),
        sa.Column("open",   sa.Numeric(18, 6), nullable=True),
        sa.Column("high",   sa.Numeric(18, 6), nullable=True),
        sa.Column("low",    sa.Numeric(18, 6), nullable=True),
        sa.Column("close",  sa.Numeric(18, 6), nullable=True),
        sa.Column("volume", sa.BigInteger(),   nullable=True),
        sa.Index("idx_daily_prices_date_symbol", "date", "symbol", unique=True),
    )

    # ── 7. benchmark_prices_daily ─────────────────────────────────────────────
    op.create_table(
        "benchmark_prices_daily",
        sa.Column("id",             sa.Integer(),      primary_key=True, index=True),
        sa.Column("date",           sa.Date(),         nullable=False, index=True),
        sa.Column("benchmark_name", sa.String(50),     nullable=False, index=True),
        sa.Column("open",           sa.Numeric(18, 6), nullable=True),
        sa.Column("close",          sa.Numeric(18, 6), nullable=True),
        sa.Index("idx_benchmark_date_name", "date", "benchmark_name", unique=True),
    )

    # ── 8. rebalance_calendar ─────────────────────────────────────────────────
    op.create_table(
        "rebalance_calendar",
        sa.Column("id",             sa.Integer(),  primary_key=True, index=True),
        sa.Column("frequency",      sa.String(50), nullable=False, index=True),
        sa.Column("rebalance_date", sa.Date(),     nullable=False, index=True),
        sa.Index("idx_rebalance_freq_date", "frequency", "rebalance_date", unique=True),
    )

    # ── 9. backtest_batches ───────────────────────────────────────────────────
    op.create_table(
        "backtest_batches",
        sa.Column("id",                  sa.Integer(),  primary_key=True, index=True),
        sa.Column("user_id",             sa.Integer(),  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("screener_id",         sa.Integer(),  nullable=True),
        sa.Column("screener_version_id", sa.Integer(),  nullable=True),
        sa.Column("status",              sa.String(50), nullable=False, default="PENDING"),
        sa.Column("created_at",          sa.DateTime(), nullable=False),
    )

    # ── 10. backtest_run ──────────────────────────────────────────────────────
    op.create_table(
        "backtest_run",
        sa.Column("id",                   sa.Integer(),       primary_key=True, index=True),
        sa.Column("batch_id",             sa.Integer(),       sa.ForeignKey("backtest_batches.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("request_hash",         sa.String(64),      nullable=False, index=True),
        sa.Column("screener_version_id",  sa.Integer(),       nullable=True, index=True),
        sa.Column("user_id",              sa.Integer(),       sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("screener_id",          sa.Integer(),       nullable=True),
        sa.Column("run_name",             sa.String(255),     nullable=True),
        sa.Column("benchmark_symbol",     sa.String(100),     nullable=True),
        sa.Column("from_date",            sa.Date(),          nullable=False),
        sa.Column("to_date",              sa.Date(),          nullable=False),
        sa.Column("rebalance_frequency",  sa.String(50),      nullable=False),
        sa.Column("portfolio_size",       sa.Integer(),       nullable=False),
        sa.Column("top_rank",             sa.Integer(),       nullable=True),
        sa.Column("wrh",                  sa.Integer(),       nullable=False),
        sa.Column("transaction_cost_bps", sa.Numeric(10, 4),  nullable=False, default=20.0),
        sa.Column("slippage_bps",         sa.Numeric(10, 4),  nullable=False, default=10.0),
        sa.Column("status",               sa.String(50),      nullable=False, default="QUEUED"),
        sa.Column("progress_pct",         sa.Numeric(5, 2),   nullable=True, default=0),
        sa.Column("error_message",        sa.Text(),          nullable=True),
        sa.Column("config_snapshot_json", sa.JSON(),          nullable=True),
        sa.Column("created_at",           sa.DateTime(),      nullable=False),
        sa.Column("started_at",           sa.DateTime(),      nullable=True),
        sa.Column("completed_at",         sa.DateTime(),      nullable=True),
    )

    # ── 11. backtest_daily_nav ────────────────────────────────────────────────
    op.create_table(
        "backtest_daily_nav",
        sa.Column("id",                     sa.BigInteger(), primary_key=True),
        sa.Column("backtest_run_id",         sa.BigInteger(), sa.ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("trade_date",              sa.Date(),       nullable=False, index=True),
        sa.Column("portfolio_return_gross",  sa.Numeric(18, 8), nullable=False),
        sa.Column("portfolio_return_net",    sa.Numeric(18, 8), nullable=False),
        sa.Column("portfolio_nav_gross",     sa.Numeric(18, 8), nullable=False),
        sa.Column("portfolio_nav_net",       sa.Numeric(18, 8), nullable=False),
        sa.Column("benchmark_return",        sa.Numeric(18, 8), nullable=True),
        sa.Column("benchmark_nav",           sa.Numeric(18, 8), nullable=True),
        sa.Column("running_peak_nav",        sa.Numeric(18, 8), nullable=True),
        sa.Column("drawdown",                sa.Numeric(18, 8), nullable=True),
        sa.Column("daily_turnover",          sa.Numeric(18, 8), nullable=True, default=0),
        sa.Column("daily_cost",              sa.Numeric(18, 8), nullable=True, default=0),
        sa.UniqueConstraint("backtest_run_id", "trade_date", name="uq_backtest_daily_nav_date"),
    )

    # ── 12. backtest_rebalance_event ──────────────────────────────────────────
    op.create_table(
        "backtest_rebalance_event",
        sa.Column("id",                    sa.BigInteger(), primary_key=True),
        sa.Column("backtest_run_id",        sa.BigInteger(), sa.ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("rebalance_date",         sa.Date(),       nullable=False, index=True),
        sa.Column("portfolio_value_before", sa.Numeric(18, 8), nullable=False),
        sa.Column("portfolio_value_after",  sa.Numeric(18, 8), nullable=False),
        sa.Column("turnover",               sa.Numeric(18, 8), nullable=False),
        sa.Column("transaction_cost",       sa.Numeric(18, 8), nullable=False, default=0),
        sa.Column("positions_before",       sa.Integer(),    nullable=False),
        sa.Column("positions_after",        sa.Integer(),    nullable=False),
        sa.Column("added_count",            sa.Integer(),    nullable=False, default=0),
        sa.Column("dropped_count",          sa.Integer(),    nullable=False, default=0),
        sa.Column("retained_count",         sa.Integer(),    nullable=False, default=0),
        sa.UniqueConstraint("backtest_run_id", "rebalance_date", name="uq_backtest_rebalance_event_date"),
    )

    # ── 13. backtest_rebalance_constituent ────────────────────────────────────
    op.create_table(
        "backtest_rebalance_constituent",
        sa.Column("id",              sa.BigInteger(), primary_key=True),
        sa.Column("backtest_run_id", sa.BigInteger(), sa.ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("rebalance_date",  sa.Date(),       nullable=False, index=True),
        sa.Column("symbol",          sa.String(50),   nullable=False),
        sa.Column("rank_position",   sa.Integer(),    nullable=True),
        sa.Column("action",          sa.String(20),   nullable=False),
        sa.Column("target_weight",   sa.Numeric(10, 6), nullable=True),
        sa.Column("is_new_entry",    sa.Boolean(),    nullable=True),
        sa.Column("is_retained",     sa.Boolean(),    nullable=True),
        sa.Column("is_exited",       sa.Boolean(),    nullable=True),
    )

    # ── 14. backtest_holding_period ───────────────────────────────────────────
    op.create_table(
        "backtest_holding_period",
        sa.Column("id",              sa.BigInteger(), primary_key=True),
        sa.Column("backtest_run_id", sa.BigInteger(), sa.ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("symbol",          sa.String(50),   nullable=False),
        sa.Column("entry_date",      sa.Date(),       nullable=False),
        sa.Column("exit_date",       sa.Date(),       nullable=True),
        sa.Column("entry_rank",      sa.Integer(),    nullable=True),
        sa.Column("entry_price",     sa.Numeric(18, 6), nullable=True),
        sa.Column("exit_price",      sa.Numeric(18, 6), nullable=True),
        sa.Column("entry_weight",    sa.Numeric(10, 6), nullable=True),
        sa.Column("exit_weight",     sa.Numeric(10, 6), nullable=True),
        sa.Column("holding_days",    sa.Integer(),    nullable=True),
        sa.Column("gross_return",    sa.Numeric(18, 8), nullable=True),
        sa.Column("net_return",      sa.Numeric(18, 8), nullable=True),
        sa.Column("exit_reason",     sa.String(50),   nullable=True),
    )

    # ── 15. backtest_drawdown_episode ─────────────────────────────────────────
    op.create_table(
        "backtest_drawdown_episode",
        sa.Column("id",                    sa.BigInteger(), primary_key=True),
        sa.Column("backtest_run_id",        sa.BigInteger(), sa.ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("peak_date",              sa.Date(),       nullable=False),
        sa.Column("trough_date",            sa.Date(),       nullable=False),
        sa.Column("recovery_date",          sa.Date(),       nullable=True),
        sa.Column("drawdown_pct",           sa.Numeric(18, 8), nullable=False),
        sa.Column("peak_to_trough_days",    sa.Integer(),    nullable=False),
        sa.Column("trough_to_recovery_days",sa.Integer(),    nullable=True),
        sa.Column("total_recovery_days",    sa.Integer(),    nullable=True),
    )

    # ── 16. backtest_monthly_return ───────────────────────────────────────────
    op.create_table(
        "backtest_monthly_return",
        sa.Column("id",                      sa.BigInteger(), primary_key=True),
        sa.Column("backtest_run_id",          sa.BigInteger(), sa.ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("year",                     sa.Integer(),    nullable=False),
        sa.Column("month",                    sa.Integer(),    nullable=False),
        sa.Column("monthly_return",           sa.Numeric(18, 8), nullable=False),
        sa.Column("benchmark_monthly_return", sa.Numeric(18, 8), nullable=True),
        sa.Column("excess_monthly_return",    sa.Numeric(18, 8), nullable=True),
        sa.UniqueConstraint("backtest_run_id", "year", "month", name="uq_backtest_monthly_return_ym"),
    )

    # ── 17. backtest_summary ──────────────────────────────────────────────────
    op.create_table(
        "backtest_summary",
        sa.Column("id",                   sa.BigInteger(), primary_key=True),
        sa.Column("backtest_run_id",       sa.BigInteger(), sa.ForeignKey("backtest_run.id", ondelete="CASCADE"), nullable=False, unique=True, index=True),
        sa.Column("total_return",          sa.Numeric(18, 8), nullable=False),
        sa.Column("cagr",                  sa.Numeric(18, 8), nullable=False),
        sa.Column("volatility",            sa.Numeric(18, 8), nullable=False),
        sa.Column("sharpe",                sa.Numeric(18, 8), nullable=False),
        sa.Column("sortino",               sa.Numeric(18, 8), nullable=False),
        sa.Column("calmar",                sa.Numeric(18, 8), nullable=False),
        sa.Column("max_drawdown",          sa.Numeric(18, 8), nullable=False),
        sa.Column("benchmark_total_return",sa.Numeric(18, 8), nullable=True),
        sa.Column("benchmark_cagr",        sa.Numeric(18, 8), nullable=True),
        sa.Column("excess_cagr",           sa.Numeric(18, 8), nullable=True),
        sa.Column("hit_ratio_vs_benchmark",sa.Numeric(18, 8), nullable=True),
        sa.Column("upside_capture",        sa.Numeric(18, 8), nullable=True),
        sa.Column("downside_capture",      sa.Numeric(18, 8), nullable=True),
        sa.Column("positive_month_pct",    sa.Numeric(18, 8), nullable=True),
        sa.Column("best_month",            sa.Numeric(18, 8), nullable=True),
        sa.Column("worst_month",           sa.Numeric(18, 8), nullable=True),
        sa.Column("avg_month",             sa.Numeric(18, 8), nullable=True),
        sa.Column("total_rebalances",      sa.Integer(),    nullable=False),
        sa.Column("avg_turnover",          sa.Numeric(18, 8), nullable=True),
        sa.Column("annualized_turnover",   sa.Numeric(18, 8), nullable=True),
        sa.Column("total_cost_drag",       sa.Numeric(18, 8), nullable=True),
        sa.Column("avg_holding_days",      sa.Numeric(18, 8), nullable=True),
        sa.Column("median_holding_days",   sa.Numeric(18, 8), nullable=True),
        sa.Column("avg_retention_pct",     sa.Numeric(18, 8), nullable=True),
        sa.Column("avg_churn_pct",         sa.Numeric(18, 8), nullable=True),
    )


def downgrade() -> None:
    # Drop in reverse order (children before parents)
    op.drop_table("backtest_summary")
    op.drop_table("backtest_monthly_return")
    op.drop_table("backtest_drawdown_episode")
    op.drop_table("backtest_holding_period")
    op.drop_table("backtest_rebalance_constituent")
    op.drop_table("backtest_rebalance_event")
    op.drop_table("backtest_daily_nav")
    op.drop_table("backtest_run")
    op.drop_table("backtest_batches")
    op.drop_table("rebalance_calendar")
    op.drop_table("benchmark_prices_daily")
    op.drop_table("daily_stock_prices")
    op.drop_table("daily_screener_data")
    op.drop_table("index_constituents")
    op.drop_table("screener_versions")
    op.drop_table("screeners")
    op.drop_table("users")
