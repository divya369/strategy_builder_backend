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


def send_rebalance_reminder_emails(today: date | None = None) -> int:
    """
    Run from ~8:30 AM IST cronjob on trading days.

    Re-sends the rebalance notification email to users whose strategies
    are still in REBALANCE_READY (they haven't executed yet).
    """
    setup_logging()
    today = today or date.today()

    if not is_trading_day(today):
        rebal_logger.info("[Reminder] %s is a non-trading day, skipping reminders", today)
        return 0

    rebal_logger.info("[Reminder] Starting send_rebalance_reminder_emails for %s", today)
    db = SessionLocal()
    try:
        count = LiveInvestmentService.send_pending_rebalance_reminders(db, today)
        rebal_logger.info("[Reminder] Completed — sent %d reminders for %s", count, today)
        return count
    except Exception:
        rebal_logger.exception("[Reminder] Fatal error in send_rebalance_reminder_emails")
        raise
    finally:
        db.close()


@shared_task(name="strategy_builder.send_rebalance_reminders")
def send_rebalance_reminder_emails_task(today_iso: str | None = None) -> int:
    today = date.fromisoformat(today_iso) if today_iso else date.today()
    return send_rebalance_reminder_emails(today)


def daily_equity_curve_update(today: date | None = None) -> int:
    """
    Run from 16:30 cron/manual job.

    Equitycase standard: daily MTM refresh for all active strategies.
    Refreshes LTP for active holdings and appends new equity curve row.
    """
    setup_logging()  # ensure file handlers for cron (no-op if already called)
    # today = today or date.today()
    today = date.fromisoformat("2026-07-01")

    if not is_trading_day(today):
        mtm_logger.info("[DailyMTM] %s is a non-trading day, skipping daily equity curve update", today)
        return 0

    mtm_logger.info("[DailyMTM] Starting daily_equity_curve_update for %s", today)
    db = SessionLocal()
    try:
        count = LiveInvestmentService.daily_equity_curve_update(db, today)
        mtm_logger.info("[DailyMTM] Completed — updated %d strategies for %s", count, today)

        # Safety net: verify strategies still stuck in pending states via kite.orders()
        # This catches missed postbacks and failed orderbook verification tasks.
        # Must run BEFORE auto_timeout to give stuck strategies a chance to resolve.
        try:
            safety_count = LiveInvestmentService.safety_net_verify_pending_strategies(db, today)
            if safety_count:
                mtm_logger.info("[DailyMTM] Safety net resolved %d stuck strategies", safety_count)
        except Exception:
            mtm_logger.exception("[DailyMTM] Safety net failed — non-blocking")

        # Store daily orderbook backup (tag-filtered JSON per broker_account)
        try:
            backup_count = LiveInvestmentService.store_daily_orderbook_backup(db, today)
            if backup_count:
                mtm_logger.info("[DailyMTM] Stored orderbook backups for %d broker_accounts", backup_count)
        except Exception:
            mtm_logger.exception("[DailyMTM] Orderbook backup failed — non-blocking")

        # Auto-cancel strategies stuck in PENDING_USER_APPROVAL with no fills
        timeout_count = LiveInvestmentService.auto_timeout_stale_strategies(db, today)
        if timeout_count:
            mtm_logger.info("[DailyMTM] Auto-timed-out %d stale PENDING strategies", timeout_count)

        return count
    except Exception:
        mtm_logger.exception("[DailyMTM] Fatal error in daily_equity_curve_update")
        raise
    finally:
        db.close()


@shared_task(name="strategy_builder.daily_equity_curve_update")
def daily_equity_curve_update_task(today_iso: str | None = None) -> int:
    today = date.fromisoformat(today_iso) if today_iso else date.today()
    # today = date.fromisoformat("2026-06-10")
    return daily_equity_curve_update(today)


def auto_timeout_stale_strategies(today: date | None = None) -> int:
    """
    Run from daily cron (e.g., after daily_equity_curve_update at 16:30).

    Auto-cancels strategies stuck in PENDING_USER_APPROVAL since before today
    with zero filled orders. This means the user opened Kite but never
    completed/cancelled the order flow.
    """
    setup_logging()
    today = today or date.today()

    if not is_trading_day(today):
        mtm_logger.info("[AutoTimeout] %s is a non-trading day, skipping auto-timeout", today)
        return 0

    mtm_logger.info("[AutoTimeout] Starting auto_timeout_stale_strategies for %s", today)
    db = SessionLocal()
    try:
        count = LiveInvestmentService.auto_timeout_stale_strategies(db, today)
        mtm_logger.info("[AutoTimeout] Completed — timed out %d strategies for %s", count, today)
        return count
    except Exception:
        mtm_logger.exception("[AutoTimeout] Fatal error in auto_timeout_stale_strategies")
        raise
    finally:
        db.close()
