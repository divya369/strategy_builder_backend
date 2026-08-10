"""Daily paper-trading update for platform (ready-to-use) strategies.

No Celery — this is a plain function invoked from the same 16:30 cronjob as the
live investment update (scripts/cron_daily_equity_update.sh), AFTER
daily_equity_curve_update() so the day's fills/LTP flow is consistent:

    from app.tasks.live_rebalance_tasks import daily_equity_curve_update
    daily_equity_curve_update()
    from app.tasks.platform_paper_tasks import daily_paper_update
    daily_paper_update()

Touches only platform_paper_* tables. Trading-day check happens inside the service.
"""
import logging
from datetime import date

from app.core.database import SessionLocal
from app.core.logging_config import setup_logging
from app.services.platform_paper_service import PlatformPaperService

paper_logger = logging.getLogger("paper_trading")


def daily_paper_update(today: date | None = None) -> int:
    """Run from the 16:30 cron (after live daily_equity_curve_update) or manually."""
    setup_logging()  # ensure file handlers for cron (no-op if already called)
    today = today or date.today()
    paper_logger.info("[Paper] Starting daily_paper_update for %s", today)
    db = SessionLocal()
    try:
        count = PlatformPaperService.run_daily_update(db, today)
        paper_logger.info("[Paper] Completed — updated %d paper portfolios for %s", count, today)
        return count
    except Exception:
        paper_logger.exception("[Paper] Fatal error in daily_paper_update")
        raise
    finally:
        db.close()
