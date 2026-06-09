from datetime import date
from celery import shared_task

from app.core.database import SessionLocal
from app.services.live_investment_service import LiveInvestmentService


def prepare_strategy_builder_live_rebalances(today: date | None = None) -> int:
    """
    Run from 16:45 cron/manual job.

    Equitycase standard:
    - WEEKLY: prepare only on week_last_trading_day
    - MONTHLY: prepare only on month_last_trading_day
    Execution still waits for user to click Trade Now and approve Publisher basket.
    """
    db = SessionLocal()
    try:
        return LiveInvestmentService.prepare_due_rebalances(db, today or date.today())
    finally:
        db.close()


@shared_task(name="strategy_builder.prepare_live_rebalances")
def prepare_strategy_builder_live_rebalances_task(today_iso: str | None = None) -> int:
    today = date.fromisoformat(today_iso) if today_iso else date.today()
    return prepare_strategy_builder_live_rebalances(today)


def daily_equity_curve_update(today: date | None = None) -> int:
    """
    Run from 16:30 cron/manual job.

    Equitycase standard: daily MTM refresh for all active strategies.
    Refreshes LTP for active holdings and appends new equity curve row.
    """
    db = SessionLocal()
    try:
        return LiveInvestmentService.daily_equity_curve_update(db, today or date.today())
    finally:
        db.close()


@shared_task(name="strategy_builder.daily_equity_curve_update")
def daily_equity_curve_update_task(today_iso: str | None = None) -> int:
    today = date.fromisoformat(today_iso) if today_iso else date.today()
    return daily_equity_curve_update(today)
