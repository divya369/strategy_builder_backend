"""Add platform paper-trading tables.

Revision ID: r2s3t4u5v6w7
Revises: q1r2s3t4u5v6
Create Date: 2026-07-23

Changes (all additive — no existing table is touched):
- platform_paper_portfolio   (paper equivalent of automate_equity)
- platform_paper_buy_stock   (paper equivalent of buy_stock, no broker cols)
- platform_paper_sell_stock  (paper equivalent of sell_stock, no broker cols)
- platform_paper_tradelog    (paper equivalent of tradelog_automate_equity)
- platform_paper_equitycurve (paper equivalent of equitycurve_automate_equity)

Column names intentionally mirror the live tables (incl. automate_equity_ra_id
as the FK name) so shared live_investment_service functions work unchanged.
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from alembic import op

revision = 'r2s3t4u5v6w7'
down_revision = 'q1r2s3t4u5v6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'platform_paper_portfolio',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('screener_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('screeners.id', ondelete='SET NULL'), nullable=True, index=True),
        sa.Column('screener_version_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('screener_versions.id', ondelete='SET NULL'), nullable=True, index=True),
        sa.Column('backtest_run_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('backtest_run.id', ondelete='SET NULL'), nullable=True, index=True),
        sa.Column('strategy_name', sa.String(255), nullable=True),
        sa.Column('portfolio_size', sa.Integer(), nullable=False),
        sa.Column('worst_hold_rank', sa.Integer(), nullable=False),
        sa.Column('rebalance_frequency', sa.String(20), nullable=False),
        sa.Column('initial_aum', sa.Float(), nullable=False, server_default='0'),
        sa.Column('cash', sa.Float(), nullable=False, server_default='0'),
        sa.Column('stock_value', sa.Float(), nullable=False, server_default='0'),
        sa.Column('final_aum', sa.Float(), nullable=False, server_default='0'),
        sa.Column('pnl', sa.Float(), nullable=False, server_default='0'),
        sa.Column('todays_pnl', sa.Float(), nullable=False, server_default='0'),
        sa.Column('status', sa.String(20), nullable=False, server_default='ACTIVE', index=True),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('backfill_end_date', sa.Date(), nullable=True),
        sa.Column('last_updated_date', sa.Date(), nullable=True),
        sa.Column('next_rebalance_date', sa.Date(), nullable=True),
        sa.Column('filters_json', postgresql.JSONB(), nullable=True),
        sa.Column('universe_json', postgresql.JSONB(), nullable=True),
        sa.Column('ranking_json', postgresql.JSONB(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        'platform_paper_buy_stock',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('automate_equity_ra_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('platform_paper_portfolio.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('tradingsymbol', sa.String(), nullable=False),
        sa.Column('isin', sa.String(), nullable=False, server_default=''),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('qty', sa.Integer(), nullable=False),
        sa.Column('price', sa.Float(), nullable=False),
        sa.Column('amount', sa.Float(), nullable=False),
        sa.Column('weightage', sa.Float(), nullable=True),
        sa.Column('actual_qty', sa.Integer(), nullable=True),
        sa.Column('actual_price', sa.Float(), nullable=True),
        sa.Column('actual_amount', sa.Float(), nullable=True),
        sa.Column('stoploss', sa.Float(), nullable=True),
        sa.Column('volatility', sa.Float(), nullable=True),
        sa.Column('order_id', sa.String(), nullable=True, index=True),
        sa.Column('circuit', sa.Boolean(), nullable=True, server_default='false'),
        sa.Column('updated_in_tradelog', sa.Boolean(), nullable=True, server_default='false'),
    )

    op.create_table(
        'platform_paper_sell_stock',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('automate_equity_ra_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('platform_paper_portfolio.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('tradingsymbol', sa.String(), nullable=False),
        sa.Column('isin', sa.String(), nullable=False, server_default=''),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('qty', sa.Integer(), nullable=False),
        sa.Column('price', sa.Float(), nullable=False),
        sa.Column('amount', sa.Float(), nullable=False),
        sa.Column('actual_qty', sa.Integer(), nullable=True),
        sa.Column('actual_price', sa.Float(), nullable=True),
        sa.Column('actual_amount', sa.Float(), nullable=True),
        sa.Column('order_id', sa.String(), nullable=True, index=True),
        sa.Column('method', sa.String(), nullable=True),
        sa.Column('circuit', sa.Boolean(), nullable=True, server_default='false'),
        sa.Column('updated_in_tradelog', sa.Boolean(), nullable=True, server_default='false'),
    )

    op.create_table(
        'platform_paper_tradelog',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('automate_equity_ra_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('platform_paper_portfolio.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('tradingsymbol', sa.String(), nullable=False),
        sa.Column('isin', sa.String(), nullable=False, server_default=''),
        sa.Column('buy_date', sa.Date(), nullable=False),
        sa.Column('sell_date', sa.Date(), nullable=True),
        sa.Column('hold', sa.Integer(), nullable=False),
        sa.Column('weightage', sa.Float(), nullable=True),
        sa.Column('buy_qty', sa.Integer(), nullable=False),
        sa.Column('buy_price', sa.Float(), nullable=False),
        sa.Column('buy_amount', sa.Float(), nullable=False),
        sa.Column('sell_qty', sa.Integer(), nullable=True),
        sa.Column('sell_price', sa.Float(), nullable=True),
        sa.Column('sell_amount', sa.Float(), nullable=True),
        sa.Column('pyramiding', sa.Integer(), nullable=True),
        sa.Column('volatility', sa.Float(), nullable=True),
        sa.Column('ltp', sa.Float(), nullable=True),
        sa.Column('stoploss', sa.Float(), nullable=True),
        sa.Column('risk', sa.Float(), nullable=True),
        sa.Column('risk_percent', sa.Float(), nullable=True),
        sa.Column('current_value', sa.Float(), nullable=True),
        sa.Column('unrealised_pnl', sa.Float(), nullable=True),
        sa.Column('realised_pnl', sa.Float(), nullable=True),
        sa.Column('profit_percent', sa.Float(), nullable=True),
        sa.Column('buy_charges', sa.Float(), nullable=True),
        sa.Column('sell_charges', sa.Float(), nullable=True),
        sa.Column('active', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('buy_order_id', sa.String(), nullable=True, index=True),
        sa.Column('sell_order_id', sa.String(), nullable=True, index=True),
        sa.Column('pyramiding_data', sa.String(), nullable=True),
        sa.Column('profit_booking_data', sa.String(), nullable=True),
    )

    op.create_table(
        'platform_paper_equitycurve',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('automate_equity_ra_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('platform_paper_portfolio.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('total_days', sa.Integer(), nullable=False),
        sa.Column('portfolio_size', sa.Integer(), nullable=True),
        sa.Column('stocks_value', sa.Float(), nullable=True),
        sa.Column('cash', sa.Float(), nullable=True),
        sa.Column('aum', sa.Float(), nullable=True),
        sa.Column('index_price', sa.Float(), nullable=True),
        sa.Column('strategy_roc', sa.Float(), nullable=True),
        sa.Column('index_roc', sa.Float(), nullable=True),
        sa.Column('strategy_daily_return', sa.Float(), nullable=True),
        sa.Column('index_daily_return', sa.Float(), nullable=True),
        sa.Column('strategy_daily_performance', sa.Float(), nullable=True),
        sa.Column('index_daily_performance', sa.Float(), nullable=True),
        sa.Column('unrealised_pnl', sa.Float(), nullable=True),
        sa.Column('realised_pnl', sa.Float(), nullable=True),
        sa.Column('total_pnl', sa.Float(), nullable=True),
        sa.Column('winning_trades', sa.Integer(), nullable=True),
        sa.Column('losing_trades', sa.Integer(), nullable=True),
        sa.Column('total_trades', sa.Integer(), nullable=True),
        sa.Column('winning_percent', sa.Float(), nullable=True),
        sa.Column('losing_percent', sa.Float(), nullable=True),
        sa.Column('avg_win', sa.Float(), nullable=True),
        sa.Column('avg_loss', sa.Float(), nullable=True),
        sa.Column('rr', sa.Float(), nullable=True),
        sa.Column('profit_factor', sa.Float(), nullable=True),
        sa.Column('biggest_winning_trade', sa.Float(), nullable=True),
        sa.Column('biggest_losing_trade', sa.Float(), nullable=True),
        sa.Column('expectancy', sa.Float(), nullable=True),
        sa.Column('avg_profit_per_day', sa.Float(), nullable=True),
        sa.Column('max_dd_percent', sa.Float(), nullable=True),
        sa.Column('max_dd_absolute', sa.Float(), nullable=True),
        sa.Column('current_dd_percent', sa.Float(), nullable=True),
        sa.Column('sqn', sa.Float(), nullable=True),
        sa.Column('k_multiple', sa.Float(), nullable=True),
        sa.Column('sharpe', sa.Float(), nullable=True),
        sa.Column('calmar', sa.Float(), nullable=True),
        sa.Column('sortino_ratio', sa.Float(), nullable=True),
        sa.Column('equitycurve_percent', sa.Float(), nullable=True),
        sa.Column('cagr_percent', sa.Float(), nullable=True),
        sa.Column('neg_2sd', sa.Float(), nullable=True),
        sa.Column('equitycurve_avg', sa.Float(), nullable=True),
        sa.Column('pos_2sd', sa.Float(), nullable=True),
        sa.Column('total_charges', sa.Float(), nullable=True),
        sa.Column('rebalance', sa.Boolean(), nullable=True, server_default='false'),
        sa.Column('weekly_return', sa.Float(), nullable=True),
        sa.Column('monthly_return', sa.Float(), nullable=True),
        sa.Column('quarterly_return', sa.Float(), nullable=True),
        sa.Column('yearly_return', sa.Float(), nullable=True),
        sa.Column('benchmark_price', sa.Float(), nullable=True),
        sa.Column('benchmark_roc', sa.Float(), nullable=True),
        sa.Column('benchmark_daily_return', sa.Float(), nullable=True),
        sa.Column('benchmark_daily_performance', sa.Float(), nullable=True),
    )
    op.create_index('ix_platform_paper_equitycurve_date', 'platform_paper_equitycurve', ['date'])


def downgrade() -> None:
    op.drop_table('platform_paper_equitycurve')
    op.drop_table('platform_paper_tradelog')
    op.drop_table('platform_paper_sell_stock')
    op.drop_table('platform_paper_buy_stock')
    op.drop_table('platform_paper_portfolio')
