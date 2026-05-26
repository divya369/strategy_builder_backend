"""
Celery application configuration.

Connects to Redis as broker and result backend.
All backtest tasks run in a separate worker process,
isolating heavy computation from the FastAPI server.

Start the worker:
    celery -A app.core.celery_app.celery_app worker --loglevel=info --concurrency=1
"""
from celery import Celery
from app.core.config import settings

celery_app = Celery(
    "strategy_builder",
    broker=settings.REDIS_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.tasks.backtest_tasks",
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
)
