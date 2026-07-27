"""
Screeners API — all universe data comes from CSV files (no DB for market data).
Public endpoints — no authentication required.
"""
import uuid
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session
from app.core.rate_limiter import rate_limit   
from datetime import datetime, timezone
from app.core.database import get_db
from app.core.filter_registry import FILTER_CONFIG_MAP, EXTRA_SORT_FIELDS
from app.core.backtest_metric_formatter import format_metric_value
from app.schemas.screener import ScreenerCreate, ScreenerVersionResponse, ScreenerVersionCreate, FilterConfig,ScreenerNewVersionResponse
from app.services.screener_service import screener_service
from app.services.screener_version_service import screener_version_service
from app.services.screener_execution_service import screener_execution_service
from app.services import csv_data_service
from app.models.screener import ScreenerVersion, Screener
from app.models.backtest import BacktestRun
from app.models.result import BacktestSummary
from app.core.backtest_error_classifier import classify_error
from zoneinfo import ZoneInfo

router = APIRouter()

IST = ZoneInfo("Asia/Kolkata")

# ── Filter config & sort options come from app.core.filter_registry ──────────

@router.get("/config/filters")
def get_filter_config():
    return FILTER_CONFIG_MAP

@router.get("/config/sort-options")
def get_sort_options():
    dynamic = []
    for key, conf in FILTER_CONFIG_MAP.items():
        if not conf.get("sortable"):
            continue
        base_key = conf.get("dbKey", key)
        desc = conf.get("description", "")
        if conf.get("periods") and conf.get("periodValues"):
            for i, p_label in enumerate(conf["periods"]):
                p_value = conf["periodValues"][i]
                label = conf["label"].replace(" (%)", "")
                sort_label = f"{label} {p_label}"
                # Replace the ## heading in description with the actual sort label
                sort_desc = desc
                first_break = desc.find("\n\n")
                if first_break != -1 and desc.startswith("## "):
                    sort_desc = f"## {sort_label}" + desc[first_break:]
                dynamic.append({"value": f"{p_value}_{base_key}", "label": sort_label, "group": conf.get("sortGroup","Filter-based"), "description": sort_desc})
        else:
            dynamic.append({"value": base_key, "label": conf["label"], "group": conf.get("sortGroup","Filter-based"), "description": desc})
    return dynamic + EXTRA_SORT_FIELDS

@router.get("/universes")
def get_universes():
    """
    Dynamically lists all available index universes from CSV files.
    Adding a new index CSV to the folder auto-appears here with zero code changes.
    """
    indices = csv_data_service.list_available_indices()
    result = [{"type": "ALL", "value": "ALL", "label": "All Stocks"}]
    for name in indices:
        result.append({"type": "index", "value": name, "label": name.replace("_", " ")})
    return result

@router.get("/data-range")
def get_data_range():
    """Returns min/max dates available in screener CSV data (used by frontend date pickers)."""
    dates = csv_data_service.get_available_screener_dates()
    if not dates:
        raise HTTPException(status_code=404, detail="No screener data available.")
    return {"min_date": str(dates[0]), "max_date": str(dates[-1])}

@router.get("/my-screeners/{user_id}")
def get_my_screeners(user_id: str, db: Session = Depends(get_db)):
    latest_v_sq = (
        db.query(ScreenerVersion.screener_id, sa_func.max(ScreenerVersion.version_number).label("max_v"))
        .group_by(ScreenerVersion.screener_id).subquery()
    )
    rows = (
        db.query(Screener, latest_v_sq.c.max_v)
        .outerjoin(latest_v_sq, Screener.id == latest_v_sq.c.screener_id)
        .filter(
            Screener.user_id == user_id,
            Screener.is_active == True
        )
        .order_by(Screener.created_at.desc())
        .all()
    )
    return [{"id": str(s.id), "name": s.name, "description": s.description, "version_number": max_v or 0} for s, max_v in rows]

@router.get("/platform-screeners")
def get_platform_strategies(role: str = None, db: Session = Depends(get_db)):
    """
    List platform (ready-to-use) strategies.
      - ?role=admin → all platform strategies incl. inactive (admin panel view)
      - no role     → only active ones (user browse view)
    """
    q = db.query(Screener).filter(Screener.role == "platform")
    if role != "ADMIN":
        q = q.filter(Screener.is_active == True)
    rows = q.order_by(Screener.created_at.desc()).all()

    def base(s):
        return {
            "id": str(s.id), "name": s.name, "description": s.description, "is_active": s.is_active,
            "created_at": s.created_at.replace(tzinfo=timezone.utc).astimezone(IST).isoformat() if s.created_at else None,
        }

    # Admin view: lean management list.
    if role == "ADMIN":
        return [base(s) for s in rows]

    # User view: enrich each card with a live snapshot from the latest paper
    # equity-curve row (formula-agnostic fields → always match the detail page).
    from app.models.platform_paper import PaperPortfolio, PaperEquityCurve
    screener_ids = [s.id for s in rows]
    ports = db.query(PaperPortfolio).filter(
        PaperPortfolio.screener_id.in_(screener_ids),
        PaperPortfolio.status == "ACTIVE",
    ).all() if screener_ids else []
    port_by_screener = {p.screener_id: p for p in ports}
    # Latest curve row per portfolio (few active portfolios → cheap).
    latest_by_port = {
        p.id: (
            db.query(PaperEquityCurve)
            .filter(PaperEquityCurve.automate_equity_ra_id == p.id)
            .order_by(PaperEquityCurve.date.desc(), PaperEquityCurve.total_days.desc())
            .first()
        )
        for p in ports
    }

    from app.services.platform_paper_service import PlatformPaperService
    result = []
    for s in rows:
        item = base(s)
        p = port_by_screener.get(s.id)
        row = latest_by_port.get(p.id) if p else None
        # Card metrics (CAGR, Max DD, Sharpe, NAV) read straight from the latest
        # paper equity-curve row — cheap single-row fetch, like live investment.
        item["live"] = PlatformPaperService.card_metrics(p, row) if (p and row) else None
        result.append(item)
    return result

@router.post("", response_model=ScreenerVersionResponse)
def create_screener(screener_in: ScreenerCreate, db: Session = Depends(get_db)):
    screener = screener_service.create_screener(db, screener_in, screener_in.user_id)
    version_in = ScreenerVersionCreate(description="Initial version", universe=screener_in.universe, filters=screener_in.filters, ranking=screener_in.ranking, rebalance=screener_in.rebalance)
    version = screener_version_service.create_version(db, screener.id, version_in, 1)
    return {"screener_id": screener.id, "name": screener.name, "version_id": version.id, "version_number": version.version_number, "message": "Screener created successfully"}

@router.post("/{screener_id}/versions", response_model=ScreenerNewVersionResponse)
def create_screener_version(screener_id: uuid.UUID, version_in: ScreenerVersionCreate, db: Session = Depends(get_db)):
    screener = db.query(Screener).filter(Screener.id == screener_id).first()
    if not screener:
        raise HTTPException(status_code=404, detail="Screener not found")
    last_v = db.query(ScreenerVersion).filter(ScreenerVersion.screener_id == screener_id).order_by(ScreenerVersion.version_number.desc()).first()
    version = screener_version_service.create_version(db, screener_id, version_in, (last_v.version_number + 1) if last_v else 1)
    return {"screener_id": screener_id, "version_id": version.id, "version_number": version.version_number, "message": "New version created successfully"}

@router.delete("/{screener_id}")
def delete_screener(screener_id: uuid.UUID, db: Session = Depends(get_db)):
    """Toggle: active screener → deactivated (soft delete); inactive screener → reactivated.

    Guard: a platform strategy can only be ACTIVATED when it has ACTIVE paper
    trading — otherwise users would see a strategy with no (or deleted) data.
    So the admin must always complete backtest → start paper before activating,
    both first time and after a stop (which deletes the data). Deactivation is
    always allowed.
    """
    screener = screener_service.get_screener(db, screener_id)
    if not screener:
        raise HTTPException(status_code=404, detail="Screener not found")

    activating = not screener.is_active  # inactive → active on this toggle
    if activating and screener.role == "platform":
        from app.models.platform_paper import PaperPortfolio
        has_active_paper = db.query(PaperPortfolio).filter(
            PaperPortfolio.screener_id == screener_id,
            PaperPortfolio.status == "ACTIVE",
        ).first() is not None
        if not has_active_paper:
            raise HTTPException(status_code=400, detail="Start paper trading before activating this strategy")

    screener = screener_service.toggle_screener_active(db, screener_id)
    action = "deactivated" if not screener.is_active else "reactivated"
    return {"success": True, "message": f"Screener {action} successfully", "data": {"id": str(screener.id), "is_active": screener.is_active, "deleted_at": screener.deleted_at.isoformat() if screener.deleted_at else None}}

@router.get("/{screener_id}/versions")
def get_screener_versions(screener_id: uuid.UUID, db: Session = Depends(get_db)):
    versions = db.query(ScreenerVersion).filter(ScreenerVersion.screener_id == screener_id).order_by(ScreenerVersion.version_number.desc()).all()
    return [{"id": str(v.id), "version_number": v.version_number, "created_at":  v.created_at
                .replace(tzinfo=timezone.utc)   # if DB time is UTC naive
                .astimezone(IST)
                .isoformat()} for v in versions]

@router.get("/{screener_id}/versions/{version_id}/backtests")
def get_version_backtests(screener_id: uuid.UUID, version_id: uuid.UUID, role: str = None, db: Session = Depends(get_db)):
    runs = db.query(BacktestRun).filter(BacktestRun.screener_version_id == version_id).order_by(BacktestRun.created_at.desc()).all()

    # ?role=ADMIN only: paper_started flag so the admin panel can
    # disable/enable the "Start Paper Trading" button on this page.
    active_paper = None
    if role == "ADMIN":
        from app.models.platform_paper import PaperPortfolio
        active_paper = db.query(PaperPortfolio).filter(
            PaperPortfolio.screener_id == screener_id,
            PaperPortfolio.status == "ACTIVE",
        ).first()

    result = []
    for run in runs:
        res = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run.id).first()
        error_type, error_message = classify_error(run.error_message) if run.status == "FAILED" else (None, None)
        item = {
            "run_id": str(run.id), "run_name": run.run_name or f"Run {run.id}",
            "period": f"{run.from_date} to {run.to_date}", "rebalance": run.rebalance_frequency,
            "portfolio_size": run.portfolio_size, "wrh": run.wrh,
            "cagr": format_metric_value(res.metrics_json.get("cagr"), "%") if res and res.metrics_json else None,
            "total_return": format_metric_value(res.metrics_json.get("total_return"), '%') if res and res.metrics_json else None,
            "status": run.status, "created_at": run.created_at,
            "error_type": error_type, "error_message": error_message
        }
        if role == "ADMIN":
            # paper_started for the screener; paper_run marks the exact run the
            # active paper portfolio was started from.
            item["paper_started"] = active_paper is not None
            item["paper_run"] = bool(active_paper and active_paper.backtest_run_id == run.id)
        result.append(item)
    return result

@router.get("/{screener_id}")
def get_screener_detail(screener_id: uuid.UUID, user_id: str, vid: uuid.UUID = None, db: Session = Depends(get_db)):
    """
    Get screener with version config.
      - ?vid=<uuid>  → returns that specific version
      - no vid       → returns the latest version
    """
    screener = screener_service.get_screener(db, screener_id)
    if not screener:
        raise HTTPException(status_code=404, detail="Screener not found.")
    if str(screener.user_id) != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to access this resource")

    if vid:
        version = db.query(ScreenerVersion).filter(
            ScreenerVersion.id == vid,
            ScreenerVersion.screener_id == screener_id,
        ).first()
        if not version:
            raise HTTPException(status_code=404, detail="Version not found.")
        latest = screener_version_service.get_latest_version(db, screener_id)
        is_latest = latest and latest.id == version.id
    else:
        version = screener_version_service.get_latest_version(db, screener_id)
        if not version:
            raise HTTPException(status_code=404, detail="No version found.")
        is_latest = True

    # Schema-driven cleanup: FilterConfig handles "null" → None, exclude_none strips them
    clean_filters = [
        FilterConfig(**f).model_dump(exclude_none=True)
        for f in (version.filters_json or [])
    ]

    # Enrich universe with start_date from CSV
    universe = dict(version.universe_json) if version.universe_json else {}
    uni_type = (universe.get("type") or "ALL").upper()
    if uni_type == "ALL":
        screener_dates = csv_data_service.get_available_screener_dates()
        universe["start_date"] = str(screener_dates[0]) if screener_dates else None
    else:
        oldest = csv_data_service.get_index_start_date(universe.get("value", ""))
        universe["start_date"] = str(oldest) if oldest else None

    return {
        "id": str(screener.id),
        "name": screener.name,
        "description": screener.description,
        "version_number": version.version_number,
        "version_id": str(version.id),
        "is_latest": is_latest,
        "filters": clean_filters,
        "universe": universe,
        "ranking": version.ranking_json,
    }

@router.post("/run-adhoc", dependencies=[Depends(rate_limit(max_requests=10, window_seconds=60))])
def run_screener_adhoc(payload: ScreenerVersionCreate, limit: int = None, offset: int = 0, db: Session = Depends(get_db)):
    return screener_execution_service.execute_adhoc(
        universe=payload.universe.model_dump(),
        filters=[f.model_dump() for f in payload.filters],
        ranking=payload.ranking.model_dump() if payload.ranking else None,
        limit=limit, offset=offset,
    )
