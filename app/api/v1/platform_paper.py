"""
Platform paper-trading API.

Admin flow (via main backend, behind the service API key):
    POST /paper-trading/toggle         — start or stop paper tracking (action=start|stop)
    POST /paper-trading/run-daily      — manual trigger of the daily update (testing/recovery)

User/admin read:
    GET  /paper-trading/{screener_id}  — summary + equity curve + tradelog
"""
import uuid
from datetime import date
from typing import Literal, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.platform_paper_service import PlatformPaperService

router = APIRouter()


class TogglePaperRequest(BaseModel):
    screener_id: uuid.UUID
    action: Literal["start", "stop"]


class RunDailyRequest(BaseModel):
    date: Optional[date] = None


@router.post("/toggle")
def toggle_paper_tracking(req: TogglePaperRequest, db: Session = Depends(get_db)):
    """Start or stop paper tracking for a platform screener.

    action="start":
        Bootstrap a fresh paper portfolio from the latest COMPLETED backtest.
        Errors: 404 screener/backtest not found · 400 not platform / already
        active / no NAV data.

    action="stop":
        Stop paper tracking AND permanently delete ALL backtest + paper data for
        the screener (one step — clean slate for re-backtesting). The screener +
        its versions are kept. The strategy must be deactivated first.
        Errors: 404 screener not found · 400 strategy still active / nothing to stop.
    """
    if req.action == "start":
        PlatformPaperService.start_paper_tracking(db, req.screener_id)
        return {"success": True, "action": "start", "message": "Paper trading started"}

    result = PlatformPaperService.stop_paper_tracking(db, req.screener_id)
    return {"success": True, "action": "stop",
            "message": "Paper trading stopped and data deleted", "data": result}


@router.post("/run-daily")
def run_daily_update(req: RunDailyRequest, db: Session = Depends(get_db)):
    count = PlatformPaperService.run_daily_update(db, req.date)
    return {"success": True, "updated_portfolios": count, "date": str(req.date or date.today())}


@router.get("/{screener_id}")
def get_paper_performance(screener_id: uuid.UUID, db: Session = Depends(get_db)):
    """User-facing detail: backtest-standard metrics + equity/drawdown graphs +
    monthly returns + drawdown history + current portfolio. Same response shape
    as the backtest results API (bottom section = current portfolio)."""
    return PlatformPaperService.build_detail(db, screener_id)
