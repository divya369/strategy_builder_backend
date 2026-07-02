"""
Celery application configuration.

Connects to Redis as broker and result backend.
All backtest tasks run in a separate worker process,
isolating heavy computation from the FastAPI server.

Start the worker:
    celery -A app.core.celery_app.celery_app worker --loglevel=info --concurrency=1
"""
from celery import Celery
from celery.schedules import crontab
from app.core.config import settings
from celery.signals import after_setup_logger 

celery_app = Celery(
    "strategy_builder",
    broker=settings.REDIS_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.tasks.backtest_tasks",
        "app.tasks.live_rebalance_tasks",
        "app.tasks.orderbook_sync_tasks",
    ],
)

celery_app.conf.update(
    # ── Timezone ──────────────────────────────────────────────────────────
    timezone="Asia/Kolkata",
    enable_utc=False,

    # ── Task behaviour ────────────────────────────────────────────────────
    task_track_started=True,
    worker_prefetch_multiplier=1,       # fetch one task at a time

    task_acks_late=True,                # ack AFTER task finishes (crash-safe)
    task_reject_on_worker_lost=True,    # re-queue if worker is killed

    # ── Serialization ─────────────────────────────────────────────────────
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # ── Time limits ───────────────────────────────────────────────────────
    task_time_limit=60 * 60 * 3,        # hard kill after 3 hours
    task_soft_time_limit=60 * 60 * 2,   # SoftTimeLimitExceeded after 2 hours

    # ── Broker ────────────────────────────────────────────────────────────
    broker_connection_retry_on_startup=True,

    # ── Beat schedule (cron) ──────────────────────────────────────────────
    # Start the beat scheduler:
    #   celery -A app.core.celery_app.celery_app beat --loglevel=info
    beat_schedule={
        # 16:30 IST — Daily MTM: refresh LTP and equity curve for all active strategies
        "daily-equity-curve-update-1630": {
            "task": "strategy_builder.daily_equity_curve_update",
            "schedule": crontab(hour=16, minute=30, day_of_week="mon-fri"),
        },
        # 16:45 IST — Rebalance preparation (weekly last / monthly last trading day)
        "prepare-live-rebalances-1645": {
            "task": "strategy_builder.prepare_live_rebalances",
            "schedule": crontab(hour=16, minute=45, day_of_week="mon-fri"),
        },
    },
)


# ── File-based logging for Celery workers ─────────────────────────────────
# after_setup_logger fires once per worker process, AFTER Celery sets up
# its own console/file handlers. setup_logging() only ADDS our daily-folder
# file handlers — it does not touch Celery's existing handlers.
# noqa: E402


@after_setup_logger.connect
def _on_after_setup_logger(**kwargs):
    from app.core.logging_config import setup_logging
    setup_logging()

