"""
Celery task for running backtests in an isolated worker process.

This task wraps backtest_engine_service.execute_backtest_background()
with proper lifecycle management:
  - Marks RUNNING with Celery task ID
  - Catches SoftTimeLimitExceeded (2h limit)
  - Catches all exceptions and marks FAILED with full traceback
  - Never leaves a run stuck in RUNNING status
"""
import logging
import traceback

from celery.exceptions import SoftTimeLimitExceeded

from app.core.celery_app import celery_app
from app.core.database import SessionLocal
from app.models.backtest import BacktestRun
from app.services.backtest_job_service import BacktestJobService
from app.services.backtest_engine import backtest_engine_service

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name="backtests.run_backtest",
    max_retries=0,
)
def run_backtest_task(self, run_id: str):
    """
    Celery task entry point for backtest execution.

    Args:
        run_id: UUID string of the BacktestRun record.
    """
    db = SessionLocal()

    try:
        run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()

        if not run:
            logger.error("BacktestRun %s not found in Celery task", run_id)
            return {"status": "FAILED", "reason": "BacktestRun not found"}

        if run.status == "COMPLETED":
            logger.info("BacktestRun %s already COMPLETED — skipping", run_id)
            return {"status": "SKIPPED", "reason": "Already completed"}

        # Mark RUNNING with Celery task ID for tracking
        BacktestJobService.mark_running(
            db=db,
            run_id=run_id,
            celery_task_id=self.request.id,
        )

        # ── Main execution ────────────────────────────────────────────────
        # execute_backtest_background manages its own DB sessions internally
        # and sets status to COMPLETED on success / FAILED on error.
        backtest_engine_service.execute_backtest_background(run_id)

        # Double-check: if engine forgot to mark COMPLETED, do it here
        db.expire_all()
        run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
        if run and run.status == "RUNNING":
            BacktestJobService.mark_completed(db, run_id)

        logger.info("Celery task for backtest %s finished successfully", run_id)
        return {"status": "COMPLETED", "run_id": run_id}

    except SoftTimeLimitExceeded:
        logger.error("Backtest %s hit soft time limit (2h)", run_id)
        db.rollback()
        try:
            BacktestJobService.mark_failed(
                db, run_id,
                "Backtest stopped: exceeded the 2-hour soft time limit.",
            )
        except Exception:
            pass
        return {"status": "FAILED", "reason": "Soft time limit exceeded"}

    except Exception as e:
        logger.error("Backtest %s FAILED in Celery task: %s", run_id, e, exc_info=True)
        db.rollback()
        error_message = str(e) + "\n\n" + traceback.format_exc()
        try:
            BacktestJobService.mark_failed(db, run_id, error_message)
        except Exception as inner_e:
            logger.error("Failed to update run %s status to FAILED: %s", run_id, inner_e)
        return {"status": "FAILED", "run_id": run_id, "error": str(e)}

    finally:
        db.close()
