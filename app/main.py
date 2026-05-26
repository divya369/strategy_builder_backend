from fastapi import FastAPI
from app.api.v1.router import api_router
from app.core.config import settings
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title=settings.PROJECT_NAME)


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

app.include_router(api_router, prefix="/api/v1")

# ── Ensure system user exists (public mode — no auth) ────────────────────────
@app.on_event("startup")
def _seed_system_user():
    from app.core.database import SessionLocal
    from app.models.user import User
    from app.api.deps import SYSTEM_USER_ID
    db = SessionLocal()
    try:
        if not db.query(User).filter(User.id == SYSTEM_USER_ID).first():
            db.add(User(id=SYSTEM_USER_ID, email="system@local", hashed_password="nologin", full_name="System"))
            db.commit()
    finally:
        db.close()

# ── Recover stale RUNNING/QUEUED backtests from previous crashes ──────────────
@app.on_event("startup")
def _recover_stale_backtests():
    from app.core.database import SessionLocal
    from app.services.backtest_job_service import BacktestJobService
    import logging
    logger = logging.getLogger(__name__)
    db = SessionLocal()
    try:
        count = BacktestJobService.recover_stale_running_jobs(db)
        if count:
            logger.warning("[Startup Recovery] Marked %d stale backtest jobs as FAILED", count)
        else:
            logger.info("[Startup Recovery] No stale backtest jobs found")
    finally:
        db.close()

import os
from fastapi.staticfiles import StaticFiles

public_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "public")
if os.path.exists(public_dir):
    app.mount("/", StaticFiles(directory=public_dir, html=True), name="public")
else:
    @app.get("/")
    def read_root():
        return {"message": f"Welcome to {settings.PROJECT_NAME} API"}
