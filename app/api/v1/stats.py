"""
Public landing-page statistics API.

One endpoint, four aggregate counters, single round-trip:
    GET /stats/landing

Numbers are cached in-process for a short TTL because the landing page is hit
by every visitor while these counters move at most a few times a day. Each
uvicorn worker keeps its own copy — that is fine for display counters, and it
avoids adding a Redis dependency for four cheap aggregates.

Auth: this route sits behind APIKeyMiddleware like every other route. The
frontend already sends `Authorization: Basic <API_KEY>`, so no bypass is needed.
"""
import time
import logging
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.models.backtest import BacktestRun
from app.models.live_investment import LiveStatus, LiveStrategy
from app.models.screener import Screener

logger = logging.getLogger(__name__)

router = APIRouter()

# ── "Currently running" definition ────────────────────────────────────────────
# Positive allowlist rather than excluding terminal states, so a LiveStatus added
# later is left OUT of the public counters until someone deliberately adds it.
#
# Paired with subscription_active below. Both are required: the exit path sets
# status=EXITED and subscription_active=False together, but rows exist where the
# two have drifted apart, and a landing-page counter must not advertise an exited
# strategy as live on the strength of one stale flag.
RUNNING_STATUSES = (
    LiveStatus.ACTIVE,
    LiveStatus.REBALANCE_READY,
    LiveStatus.REBALANCE_PENDING_USER_APPROVAL,
    LiveStatus.REBALANCE_PROCESSING,
    LiveStatus.REBALANCE_SELL_COMPLETE,
    LiveStatus.EXIT_PENDING_USER_APPROVAL,
    LiveStatus.EXIT_PROCESSING,
)

# ── In-process cache ──────────────────────────────────────────────────────────
CACHE_TTL_SECONDS = 300  # 5 min — counters change slowly, page is hit constantly

_cached_stats: Optional["LandingStatsResponse"] = None
_cached_at: float = 0.0


class LandingStatsResponse(BaseModel):
    total_aum_deployed: float
    total_backtests: int
    total_live_strategies: int
    total_ready_to_use_strategies: int


def _compute_landing_stats(db: Session) -> LandingStatsResponse:
    """Run the four aggregates. Each is a single indexed COUNT/SUM."""

    # Committed capital across strategies that are currently running.
    # initial_aum (not final_aum): what users actually put in, not today's MTM.
    # The running filter excludes DRAFT/PREVIEW_READY/PENDING_USER_APPROVAL rows,
    # which carry an initial_aum that was never executed, and EXITED/CANCELLED
    # rows, where the money has been withdrawn.
    total_aum_deployed = db.query(
        func.coalesce(func.sum(LiveStrategy.initial_aum), 0.0)
    ).filter(
        LiveStrategy.subscription_active == True,  # noqa: E712 — SQL comparison
        LiveStrategy.status.in_(RUNNING_STATUSES),
    ).scalar()

    # Backtests that produced results. Note: backtest_run.request_hash is unique,
    # so identical configs are deduped — this counts distinct backtests, not the
    # number of times the run button was pressed.
    total_backtests = db.query(func.count(BacktestRun.id)).filter(
        BacktestRun.status == "COMPLETED"
    ).scalar()

    # Strategies with real money live right now (same filter as the AUM sum, so
    # the two numbers always describe the same set of strategies).
    total_live_strategies = db.query(func.count(LiveStrategy.id)).filter(
        LiveStrategy.subscription_active == True,  # noqa: E712 — SQL comparison
        LiveStrategy.status.in_(RUNNING_STATUSES),
    ).scalar()

    # Ready-to-use (platform) strategies visible to users. Mirrors the filter in
    # GET /screeners/platform-screeners: inactive platform screeners are admin
    # drafts with no paper-trading data behind them.
    total_ready_to_use_strategies = db.query(func.count(Screener.id)).filter(
        Screener.role == "platform",
        Screener.is_active == True,  # noqa: E712 — SQL comparison
    ).scalar()

    return LandingStatsResponse(
        total_aum_deployed=round(float(total_aum_deployed or 0.0), 2),
        total_backtests=int(total_backtests or 0),
        total_live_strategies=int(total_live_strategies or 0),
        total_ready_to_use_strategies=int(total_ready_to_use_strategies or 0),
    )


@router.get("/landing", response_model=LandingStatsResponse)
def get_landing_stats(refresh: bool = False, db: Session = Depends(get_db)):
    """Aggregate counters for the marketing landing page.

    - `total_aum_deployed`            — SUM(initial_aum) of running strategies (₹)
    - `total_backtests`               — COMPLETED backtest runs
    - `total_live_strategies`         — strategies currently running
    - `total_ready_to_use_strategies` — active platform strategies

    Pass `?refresh=true` to bypass the 5-minute cache (admin/debug).
    """
    global _cached_stats, _cached_at

    now = time.monotonic()
    if not refresh and _cached_stats is not None and (now - _cached_at) < CACHE_TTL_SECONDS:
        return _cached_stats

    stats = _compute_landing_stats(db)
    _cached_stats = stats
    _cached_at = now
    logger.info(
        "[Stats] Landing stats refreshed | aum=%.2f backtests=%d live=%d ready=%d",
        stats.total_aum_deployed, stats.total_backtests,
        stats.total_live_strategies, stats.total_ready_to_use_strategies,
    )
    return stats
