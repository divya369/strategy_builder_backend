import logging
from datetime import date
from celery import shared_task

from app.core.database import SessionLocal
from app.core.logging_config import setup_logging
from app.core.trading_calendar import is_trading_day
from app.services.live_investment_service import LiveInvestmentService

# Dedicated loggers → write to daily_mtm.log / rebalance.log respectively
mtm_logger = logging.getLogger("daily_mtm")
rebal_logger = logging.getLogger("rebalance")


def prepare_strategy_builder_live_rebalances(today: date | None = None) -> int:
    """
    Run from 16:45 cron/manual job.

    Equitycase standard:
    - WEEKLY: prepare only on week_last_trading_day
    - MONTHLY: prepare only on month_last_trading_day
    Execution still waits for user to click Trade Now and approve Publisher basket.
    """
    setup_logging()  # ensure file handlers for cron (no-op if already called)
    today = today or date.today()

    if not is_trading_day(today):
        rebal_logger.info("[Rebalance] %s is a non-trading day, skipping rebalance preparation", today)
        return 0

    rebal_logger.info("[Rebalance] Starting prepare_strategy_builder_live_rebalances for %s", today)
    db = SessionLocal()
    try:
        count = LiveInvestmentService.prepare_due_rebalances(db, today)
        rebal_logger.info("[Rebalance] Completed — prepared %d strategies for %s", count, today)
        return count
    except Exception:
        rebal_logger.exception("[Rebalance] Fatal error in prepare_strategy_builder_live_rebalances")
        raise
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
    setup_logging()  # ensure file handlers for cron (no-op if already called)
    today = today or date.today()

    if not is_trading_day(today):
        mtm_logger.info("[DailyMTM] %s is a non-trading day, skipping daily equity curve update", today)
        return 0

    mtm_logger.info("[DailyMTM] Starting daily_equity_curve_update for %s", today)
    db = SessionLocal()
    try:
        count = LiveInvestmentService.daily_equity_curve_update(db, today)
        mtm_logger.info("[DailyMTM] Completed — updated %d strategies for %s", count, today)
        return count
    except Exception:
        mtm_logger.exception("[DailyMTM] Fatal error in daily_equity_curve_update")
        raise
    finally:
        db.close()


@shared_task(name="strategy_builder.daily_equity_curve_update")
def daily_equity_curve_update_task(today_iso: str | None = None) -> int:
    # today = date.fromisoformat(today_iso) if today_iso else date.today()
    today = date.fromisoformat("2026-06-10")
    return daily_equity_curve_update(today)
