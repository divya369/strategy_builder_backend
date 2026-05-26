"""
Backtest Job Service — lifecycle helpers for BacktestRun status management.

Provides:
  - mark_running / mark_completed / mark_failed — status transitions
  - heartbeat — periodic liveness update during backtest execution
  - recover_stale_running_jobs — startup cleanup for orphaned RUNNING jobs

All methods are stateless and take an explicit DB session.
"""
import logging
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.models.backtest import BacktestRun
from app.core.config import settings

logger = logging.getLogger(__name__)


class BacktestJobService:

    @staticmethod
    def mark_running(db: Session, run_id, celery_task_id: str = None):
        """Set status to RUNNING and record Celery task ID."""
        run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
        if not run:
            return None

        now = datetime.utcnow()
        run.status = "RUNNING"
        run.started_at = run.started_at or now
        run.last_heartbeat_at = now

        if celery_task_id:
            run.celery_task_id = celery_task_id

        db.commit()
        db.refresh(run)
        return run

    @staticmethod
    def heartbeat(db: Session, run_id):
        """Update last_heartbeat_at to signal the worker is still alive."""
        run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
        if not run:
            return
        run.last_heartbeat_at = datetime.utcnow()
        db.commit()

    @staticmethod
    def mark_completed(db: Session, run_id):
        """Mark run as COMPLETED with timestamp."""
        run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
        if not run:
            return

        now = datetime.utcnow()
        run.status = "COMPLETED"
        run.completed_at = now
        run.last_heartbeat_at = now
        run.error_message = None
        db.commit()

    @staticmethod
    def mark_failed(db: Session, run_id, error_message: str):
        """Mark run as FAILED with error details (truncated to 10k chars)."""
        run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
        if not run:
            return

        now = datetime.utcnow()
        run.status = "FAILED"
        run.completed_at = now
        run.last_heartbeat_at = now
        run.error_message = error_message[:10000]
        db.commit()

    @staticmethod
    def recover_stale_running_jobs(db: Session) -> int:
        """
        Find RUNNING/QUEUED jobs whose heartbeat is older than
        BACKTEST_STALE_MINUTES and mark them FAILED.

        Called on FastAPI startup and can also be called on-demand.
        Returns the number of jobs recovered.
        """
        stale_before = datetime.utcnow() - timedelta(
            minutes=settings.BACKTEST_STALE_MINUTES
        )

        # Jobs with a stale heartbeat
        stale_runs = db.query(BacktestRun).filter(
            BacktestRun.status.in_(["RUNNING", "QUEUED"]),
            BacktestRun.last_heartbeat_at.isnot(None),
            BacktestRun.last_heartbeat_at < stale_before,
        ).all()

        # Jobs with NO heartbeat at all (old records from before this feature)
        null_heartbeat_runs = db.query(BacktestRun).filter(
            BacktestRun.status.in_(["RUNNING", "QUEUED"]),
            BacktestRun.last_heartbeat_at.is_(None),
        ).all()

        all_stale = stale_runs + null_heartbeat_runs

        for run in all_stale:
            run.status = "FAILED"
            run.completed_at = datetime.utcnow()
            run.error_message = (
                "Marked FAILED automatically: worker/server stopped or "
                "heartbeat became stale."
            )
            logger.warning(
                "Recovered stale backtest run %s (was %s)",
                run.id, "RUNNING/QUEUED"
            )

        if all_stale:
            db.commit()

        return len(all_stale)
