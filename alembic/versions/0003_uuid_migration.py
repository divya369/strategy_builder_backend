"""
UUID migration + dead table cleanup.

Changes:
  1. Drop legacy market data tables (daily_stock_prices, benchmark_prices_daily,
     daily_screener_data, rebalance_calendar, index_constituents)
  2. Drop all existing app tables (cascade-safe order)
  3. Recreate all app tables with UUID primary keys (gen_random_uuid())
  
Depends on: 0002_drop_backtest_batches
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0003_uuid_migration"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    # Enable pgcrypto for gen_random_uuid() (idempotent)
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # ── 1. Drop legacy market data tables ─────────────────────────────────────
    for table in ("daily_screener_data", "daily_stock_prices", "benchmark_prices_daily",
                  "rebalance_calendar", "index_constituents"):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    # ── 2. Drop existing app tables (children first) ──────────────────────────
    for table in (
        "backtest_monthly_return", "backtest_drawdown_episode", "backtest_summary",
        "backtest_rebalance_event", "backtest_daily_nav",
        "backtest_holding_period", "backtest_rebalance_constituent",
        "backtest_run", "screener_versions", "screeners", "users",
    ):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    # ── 3. Recreate with UUID PKs ─────────────────────────────────────────────
    op.execute("""
        CREATE TABLE users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) NOT NULL UNIQUE,
            hashed_password VARCHAR(255) NOT NULL,
            full_name VARCHAR(255),
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE TABLE screeners (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            deleted_at TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX ix_screeners_user_id ON screeners(user_id)")

    op.execute("""
        CREATE TABLE screener_versions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            screener_id UUID NOT NULL REFERENCES screeners(id) ON DELETE CASCADE,
            version_number INTEGER NOT NULL,
            filters_json JSONB NOT NULL,
            universe_json JSONB NOT NULL,
            ranking_json JSONB,
            is_current BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL
        )
    """)
    op.execute("CREATE INDEX ix_screener_versions_screener_id ON screener_versions(screener_id)")

    op.execute("""
        CREATE TABLE backtest_run (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            request_hash VARCHAR(64) UNIQUE,
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            screener_id UUID REFERENCES screeners(id) ON DELETE SET NULL,
            screener_version_id UUID REFERENCES screener_versions(id) ON DELETE SET NULL,
            run_name VARCHAR(255),
            benchmark_symbol VARCHAR(50),
            from_date DATE NOT NULL,
            to_date DATE NOT NULL,
            rebalance_frequency VARCHAR(50) NOT NULL DEFAULT 'WEEKLY',
            portfolio_size INTEGER NOT NULL DEFAULT 30,
            top_rank INTEGER NOT NULL DEFAULT 30,
            wrh INTEGER NOT NULL DEFAULT 40,
            transaction_cost_bps NUMERIC(10,4) NOT NULL DEFAULT 0,
            slippage_bps NUMERIC(10,4) NOT NULL DEFAULT 0,
            initial_capital NUMERIC(20,4) NOT NULL DEFAULT 1000000,
            status VARCHAR(50) NOT NULL DEFAULT 'QUEUED',
            progress_pct INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP,
            completed_at TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX ix_backtest_run_user_id ON backtest_run(user_id)")
    op.execute("CREATE INDEX ix_backtest_run_screener_id ON backtest_run(screener_id)")
    op.execute("CREATE INDEX ix_backtest_run_screener_version_id ON backtest_run(screener_version_id)")

    op.execute("""
        CREATE TABLE backtest_rebalance_constituent (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
            rebalance_date DATE NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            rank_position INTEGER,
            action VARCHAR(10) NOT NULL,
            target_weight NUMERIC(18,8) NOT NULL,
            is_new_entry BOOLEAN NOT NULL DEFAULT FALSE,
            is_retained BOOLEAN NOT NULL DEFAULT FALSE,
            is_exited BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)
    op.execute("CREATE INDEX ix_brc_run_id ON backtest_rebalance_constituent(backtest_run_id)")
    op.execute("CREATE INDEX ix_brc_date ON backtest_rebalance_constituent(rebalance_date)")

    op.execute("""
        CREATE TABLE backtest_holding_period (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
            symbol VARCHAR(50) NOT NULL,
            entry_date DATE NOT NULL,
            exit_date DATE,
            entry_rank INTEGER,
            entry_price NUMERIC(18,8) NOT NULL,
            exit_price NUMERIC(18,8),
            entry_weight NUMERIC(18,8) NOT NULL,
            exit_weight NUMERIC(18,8),
            holding_days INTEGER,
            gross_return NUMERIC(18,8),
            net_return NUMERIC(18,8),
            exit_reason VARCHAR(50)
        )
    """)
    op.execute("CREATE INDEX ix_bhp_run_id ON backtest_holding_period(backtest_run_id)")

    op.execute("""
        CREATE TABLE backtest_daily_nav (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
            trade_date DATE NOT NULL,
            portfolio_return_gross NUMERIC(18,8) NOT NULL,
            portfolio_return_net NUMERIC(18,8) NOT NULL,
            portfolio_nav_gross NUMERIC(18,8) NOT NULL,
            portfolio_nav_net NUMERIC(18,8) NOT NULL,
            benchmark_return NUMERIC(18,8),
            benchmark_nav NUMERIC(18,8),
            running_peak_nav NUMERIC(18,8),
            drawdown NUMERIC(18,8),
            daily_turnover NUMERIC(18,8) DEFAULT 0,
            daily_cost NUMERIC(18,8) DEFAULT 0,
            UNIQUE(backtest_run_id, trade_date)
        )
    """)
    op.execute("CREATE INDEX ix_bdn_run_id ON backtest_daily_nav(backtest_run_id)")

    op.execute("""
        CREATE TABLE backtest_rebalance_event (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
            rebalance_date DATE NOT NULL,
            portfolio_value_before NUMERIC(18,8) NOT NULL,
            portfolio_value_after NUMERIC(18,8) NOT NULL,
            turnover NUMERIC(18,8) NOT NULL,
            transaction_cost NUMERIC(18,8) NOT NULL DEFAULT 0,
            positions_before INTEGER NOT NULL,
            positions_after INTEGER NOT NULL,
            added_count INTEGER NOT NULL DEFAULT 0,
            dropped_count INTEGER NOT NULL DEFAULT 0,
            retained_count INTEGER NOT NULL DEFAULT 0,
            UNIQUE(backtest_run_id, rebalance_date)
        )
    """)
    op.execute("CREATE INDEX ix_bre_run_id ON backtest_rebalance_event(backtest_run_id)")

    op.execute("""
        CREATE TABLE backtest_summary (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            backtest_run_id UUID NOT NULL UNIQUE REFERENCES backtest_run(id) ON DELETE CASCADE,
            total_return NUMERIC(18,8) NOT NULL,
            cagr NUMERIC(18,8) NOT NULL,
            volatility NUMERIC(18,8) NOT NULL,
            sharpe NUMERIC(18,8) NOT NULL,
            sortino NUMERIC(18,8) NOT NULL,
            calmar NUMERIC(18,8) NOT NULL,
            max_drawdown NUMERIC(18,8) NOT NULL,
            benchmark_total_return NUMERIC(18,8),
            benchmark_cagr NUMERIC(18,8),
            excess_cagr NUMERIC(18,8),
            hit_ratio_vs_benchmark NUMERIC(18,8),
            upside_capture NUMERIC(18,8),
            downside_capture NUMERIC(18,8),
            positive_month_pct NUMERIC(18,8),
            best_month NUMERIC(18,8),
            worst_month NUMERIC(18,8),
            avg_month NUMERIC(18,8),
            total_rebalances INTEGER NOT NULL,
            avg_turnover NUMERIC(18,8),
            annualized_turnover NUMERIC(18,8),
            total_cost_drag NUMERIC(18,8),
            avg_holding_days NUMERIC(18,8),
            median_holding_days NUMERIC(18,8),
            avg_retention_pct NUMERIC(18,8),
            avg_churn_pct NUMERIC(18,8)
        )
    """)

    op.execute("""
        CREATE TABLE backtest_drawdown_episode (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
            peak_date DATE NOT NULL,
            trough_date DATE NOT NULL,
            recovery_date DATE,
            drawdown_pct NUMERIC(18,8) NOT NULL,
            peak_to_trough_days INTEGER NOT NULL,
            trough_to_recovery_days INTEGER,
            total_recovery_days INTEGER
        )
    """)
    op.execute("CREATE INDEX ix_bde_run_id ON backtest_drawdown_episode(backtest_run_id)")

    op.execute("""
        CREATE TABLE backtest_monthly_return (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            monthly_return NUMERIC(18,8) NOT NULL,
            benchmark_monthly_return NUMERIC(18,8),
            excess_monthly_return NUMERIC(18,8),
            UNIQUE(backtest_run_id, year, month)
        )
    """)
    op.execute("CREATE INDEX ix_bmr_run_id ON backtest_monthly_return(backtest_run_id)")


def downgrade():
    # Drop all UUID tables — no recovery of old integer-keyed data
    for table in (
        "backtest_monthly_return", "backtest_drawdown_episode", "backtest_summary",
        "backtest_rebalance_event", "backtest_daily_nav",
        "backtest_holding_period", "backtest_rebalance_constituent",
        "backtest_run", "screener_versions", "screeners", "users",
    ):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

