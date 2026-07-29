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
from app.core.trading_calendar import require_market_open
from app.schemas.live_investment import (
    GoLiveRequest,
    LiveStrategyResponse,
    PlatformInvestRequest,
)
from app.services.platform_paper_service import PlatformPaperService
from app.services.screener_service import screener_service

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


@router.post("/invest", response_model=LiveStrategyResponse)
def invest_in_platform_strategy(
    req: PlatformInvestRequest,
    db: Session = Depends(get_db),
    _mkt=Depends(require_market_open),
):
    """User clicks 'Invest Now' on a platform (ready-to-use) strategy.

    The user's own screener is NOT created during the in-between stages
    (draft / preview / pending). Instead:
      - If the user has ALREADY adopted this platform strategy (a prior invest
        went ACTIVE), this runs on that existing user screener — a normal Go Live.
      - Otherwise it runs on the PLATFORM version directly and records the source
        on the live strategy; the user's own screener is materialised only when the
        strategy first goes ACTIVE (in process_orders), then this strategy is
        re-pointed to it. So an abandoned preview never creates a throwaway screener.

    From there it is an ordinary live strategy, so the standard Go Live flow and
    ALL its restrictions apply unchanged (same-broker duplicate guard, stale-PENDING
    auto-cancel, different-broker allowed).

    The response carries `screener_id` for routing — note it is the PLATFORM
    screener until the strategy activates, then the user's own clone.
    """
    # Imported here to avoid any import-order coupling between the two routers.
    from app.api.v1.live_investment import go_live
    from app.models.live_investment import LiveStrategy

    # Decide the version WITHOUT creating a user screener yet.
    version_id, defer_source = screener_service.resolve_platform_invest_version(
        db, req.platform_screener_id, uuid.UUID(req.user_id),
    )

    payload = GoLiveRequest(
        user_id=req.user_id,
        screener_version_id=version_id,
        strategy_name=req.strategy_name,
        portfolio_size=req.portfolio_size,
        wrh=req.wrh,
        rebalance_frequency=req.rebalance_frequency,
        aum=req.aum,
        broker=req.broker,
        broker_user_id=req.broker_user_id,
        broker_account_label=req.broker_account_label,
    )
    result = go_live(payload, db=db, _mkt=_mkt)

    # Deferred adoption: tag the live strategy so it clones its user screener on
    # first activation. (Nothing to tag when reusing an already-adopted screener.)
    if defer_source is not None:
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == result.id).first()
        if strategy:
            strategy.source_platform_screener_id = defer_source
            db.commit()
    return result


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
