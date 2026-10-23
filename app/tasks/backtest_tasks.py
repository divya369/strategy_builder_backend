"""
Celery task for running backtests in an isolated worker process.

This task wraps backtest_engine_service.execute_backtest_background()
with proper lifecycle management:
  - Marks RUNNING with Celery task ID
  - Catches SoftTimeLimitExceeded (2h limit)
  - Catches all exceptions and marks FAILED with full traceback
  - Never leaves a run stuck in RUNNING status
  - Worker process killed (out of memory, hard time limit) → run marked
    FAILED by _on_task_failure; the task is NOT re-queued, since the same
    backtest would most likely be killed again
  - Skips outdated messages: only the task whose id is stored on the run executes
"""
import logging
import traceback
from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import task_failure
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
    # Global default re-queues a task whose worker died. For a backtest that
    # means: killed for memory → re-run → killed again, in a loop. Fail instead.
    reject_on_worker_lost=False,
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

        # The API stores the task id before sending. A different id means this
        # run was re-submitted after this message was queued — the newer task runs it.
        if run.celery_task_id and run.celery_task_id != self.request.id:
            logger.info("BacktestRun %s belongs to task %s — skipping outdated task %s",
                        run_id, run.celery_task_id, self.request.id)
            return {"status": "SKIPPED", "reason": "Superseded by a newer task"}

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


@task_failure.connect
def _on_task_failure(sender=None, task_id=None, exception=None, args=None, **kwargs):
    """
    The task body catches its own errors, so this only fires when the process
    running it died — killed for memory (WorkerLostError) or by the hard time
    limit. It runs in the worker's main process; mark the run FAILED right away
    instead of leaving it RUNNING until its heartbeat goes stale.
    """
    if getattr(sender, "name", None) != run_backtest_task.name or not args:
        return
    run_id = args[0]
    db = SessionLocal()
    try:
        run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
        if run and run.status in ("RUNNING", "QUEUED") and run.celery_task_id == task_id:
            BacktestJobService.mark_failed(
                db, run_id,
                f"Backtest stopped: the worker process ended before finishing "
                f"({type(exception).__name__}: {exception}). This usually means the "
                f"server ran out of memory or the 3-hour limit was hit. It was not "
                f"retried automatically.",
            )
            logger.error("Backtest %s FAILED — worker process lost: %r", run_id, exception)
    except Exception:
        logger.exception("Could not mark backtest %s FAILED after worker loss", run_id)
    finally:
        db.close()
