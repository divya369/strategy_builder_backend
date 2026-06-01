import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.v1.router import api_router
from app.core.config import settings

logger = logging.getLogger(__name__)


# ── Lifespan: startup + graceful shutdown ─────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── STARTUP ───────────────────────────────────────────────────────────────
    logger.info("[Startup] %s is starting …", settings.PROJECT_NAME)

    # Seed system user
    from app.core.database import SessionLocal
    from app.models.user import User
    from app.api.deps import SYSTEM_USER_ID

    db = SessionLocal()
    try:
        if not db.query(User).filter(User.id == SYSTEM_USER_ID).first():
            db.add(User(
                id=SYSTEM_USER_ID,
                email="system@local",
                hashed_password="nologin",
                full_name="System",
            ))
            db.commit()
            logger.info("[Startup] Seeded system user")
    finally:
        db.close()

    # Recover stale backtest jobs
    from app.services.backtest_job_service import BacktestJobService

    db = SessionLocal()
    try:
        count = BacktestJobService.recover_stale_running_jobs(db)
        if count:
            logger.warning("[Startup Recovery] Marked %d stale backtest jobs as FAILED", count)
        else:
            logger.info("[Startup Recovery] No stale backtest jobs found")
    finally:
        db.close()

    logger.info("[Startup] %s is ready", settings.PROJECT_NAME)

    # ── Hand control to the running application ──────────────────────────────
    yield

    # ── SHUTDOWN (runs when the server receives SIGTERM / SIGINT / Ctrl+C) ───
    logger.info("[Shutdown] Graceful shutdown initiated …")

    # 1. Dispose SQLAlchemy connection pools
    from app.core.database import engine as app_engine
    try:
        app_engine.dispose()
        logger.info("[Shutdown] App DB engine disposed")
    except Exception:
        logger.exception("[Shutdown] Error disposing app DB engine")

    try:
        from app.core.database import equity_engine
        equity_engine.dispose()
        logger.info("[Shutdown] Equity OHLC DB engine disposed")
    except Exception:
        logger.exception("[Shutdown] Error disposing equity DB engine")

    # 2. Close Celery app (flushes pending results, closes broker connection)
    try:
        from app.core.celery_app import celery_app
        celery_app.close()
        logger.info("[Shutdown] Celery app closed")
    except Exception:
        logger.exception("[Shutdown] Error closing Celery app")

    logger.info("[Shutdown] %s has stopped cleanly ✔", settings.PROJECT_NAME)


# ── App instance ──────────────────────────────────────────────────────────────
app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)


# ── CORS ──────────────────────────────────────────────────────────────────────
# Allowed origins are driven by the ALLOWED_ORIGINS env variable.
# During development the default includes localhost:3000 and localhost:8000.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["Authorization", "Content-Type"],  # Restrict to only required headers
)

# ── API-key auth ──────────────────────────────────────────────────────────────
from app.core.auth_middleware import APIKeyMiddleware
app.add_middleware(APIKeyMiddleware)

app.include_router(api_router, prefix="/api/v1")

# ── Static files (if public/ dir exists) ──────────────────────────────────────
public_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "public")
if os.path.exists(public_dir):
    app.mount("/", StaticFiles(directory=public_dir, html=True), name="public")
else:
    @app.get("/")
    def read_root():
        return {"message": f"Welcome to {settings.PROJECT_NAME} API"}
