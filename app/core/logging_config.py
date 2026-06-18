"""
Daily-folder file logging for Strategy Builder.

Structure:
    logs/
    └── 2026-06-10/
        ├── 2026-06-10_main.log              ← catch-all (root logger)
        ├── 2026-06-10_backtest.log           ← backtest tasks & engine
        ├── 2026-06-10_live_investment.log    ← go-live, orders, postback
        ├── 2026-06-10_daily_mtm.log          ← 16:30 daily equity curve update
        ├── 2026-06-10_rebalance.log          ← 16:45 rebalance preparation
        └── 2026-06-10_celery.log             ← celery worker internals

Usage:
    from app.core.logging_config import setup_logging
    setup_logging()              # call once per process

Safe to call multiple times — only the first call takes effect.
"""
from __future__ import annotations
import logging
from datetime import date
from pathlib import Path

# Base log directory: <project_root>/logs/
BASE_LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"


# ---------------------------------------------------------------------------
# Custom handler: date-folder + date-prefixed file
# ---------------------------------------------------------------------------

class DailyFolderFileHandler(logging.Handler):
    """Writes to ``{base_dir}/{YYYY-MM-DD}/{YYYY-MM-DD}_{component}.log``.

    Automatically rolls to a new date folder at midnight.
    Thread-safe: ``logging.Handler.handle()`` acquires the lock before
    calling ``emit()``.
    """

    def __init__(self, component: str, base_dir: Path | None = None):
        super().__init__()
        self.component = component
        self.base_dir = base_dir or BASE_LOG_DIR
        self._current_date: date | None = None
        self._stream = None

    # -- internal helpers ---------------------------------------------------

    def _open_stream(self, today: date) -> None:
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
        date_str = today.isoformat()
        folder = self.base_dir / date_str
        folder.mkdir(parents=True, exist_ok=True)
        filepath = folder / f"{date_str}_{self.component}.log"
        self._stream = open(filepath, "a", encoding="utf-8")
        self._current_date = today

    # -- Handler API --------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        try:
            today = date.today()
            if today != self._current_date:
                self._open_stream(today)
            msg = self.format(record)
            self._stream.write(msg + "\n")
            self._stream.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
        super().close()


# ---------------------------------------------------------------------------
# Logger → file mapping
# ---------------------------------------------------------------------------

# Component-specific loggers. Each list entry is a Python logger name
# whose output (in addition to main.log via root) goes to a dedicated file.

COMPONENT_LOGGERS: dict[str, list[str]] = {
    "backtest": [
        "app.tasks.backtest_tasks",
        "app.services.backtest_job_service",
        "app.services.backtest_engine",
    ],
    "live_investment": [
        "app.services.live_investment_service",
        "app.api.v1.live_investment",
        "app.services.broker_publishers",
        "app.services.screener_execution_service",
    ],
    "daily_mtm": [
        "daily_mtm",
    ],
    "rebalance": [
        "rebalance",
    ],
    "broker_token": [
        "broker_token",
    ],
    "celery": [
        "celery",
        "celery.app.trace",
        "celery.worker",
    ],
}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_setup_done = False


def setup_logging(level: int = logging.INFO) -> None:
    """Configure daily-folder file logging for all components.

    * Adds a ``_main.log`` handler on the **root** logger (catch-all).
    * Adds component-specific file handlers so each subsystem also writes
      to its own dedicated log file.
    * Does **not** remove or replace existing handlers (console output from
      uvicorn / celery stays intact).
    * Idempotent — only the first call per process takes effect.
    """
    global _setup_done
    if _setup_done:
        return
    _setup_done = True

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Root logger: catch-all → main.log ─────────────────────────────────
    root = logging.getLogger()
    main_handler = DailyFolderFileHandler("main")
    main_handler.setFormatter(formatter)
    main_handler.setLevel(level)
    root.addHandler(main_handler)
    if root.level == logging.NOTSET or root.level > level:
        root.setLevel(level)

    # ── Component-specific file handlers ──────────────────────────────────
    for component, logger_names in COMPONENT_LOGGERS.items():
        handler = DailyFolderFileHandler(component)
        handler.setFormatter(formatter)
        handler.setLevel(level)
        for name in logger_names:
            lgr = logging.getLogger(name)
            lgr.addHandler(handler)
            if lgr.level == logging.NOTSET or lgr.level > level:
                lgr.setLevel(level)
            # propagate=True (default) so logs also appear in main.log
