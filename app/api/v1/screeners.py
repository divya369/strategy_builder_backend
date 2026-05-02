"""
Screeners API — all universe data comes from CSV files (no DB for market data).
Public endpoints — no authentication required.
"""
import uuid
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.filter_registry import FILTER_CONFIG_MAP, EXTRA_SORT_FIELDS
from app.core.backtest_metric_formatter import format_metric_value
from app.schemas.screener import ScreenerCreate, ScreenerVersionResponse, ScreenerVersionCreate, FilterConfig
from app.services.screener_service import screener_service
from app.services.screener_version_service import screener_version_service
from app.services.screener_execution_service import screener_execution_service
from app.services import csv_data_service
from app.models.screener import ScreenerVersion, Screener
from app.models.backtest import BacktestRun
from app.models.result import BacktestSummary
from app.api.deps import SYSTEM_USER_ID

router = APIRouter()

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
        if conf.get("periods") and conf.get("periodValues"):
            for i, p_label in enumerate(conf["periods"]):
                p_value = conf["periodValues"][i]
                label = conf["label"].replace(" (%)", "")
                dynamic.append({"value": f"{p_value}_{base_key}", "label": f"{label} {p_label}", "group": conf.get("sortGroup","Filter-based")})
        else:
            dynamic.append({"value": base_key, "label": conf["label"], "group": conf.get("sortGroup","Filter-based")})
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

@router.post("", response_model=ScreenerVersionResponse)
def create_screener(screener_in: ScreenerCreate, db: Session = Depends(get_db)):
    screener = screener_service.create_screener(db, screener_in, screener_in.user_id)
    version_in = ScreenerVersionCreate(description="Initial version", universe=screener_in.universe, filters=screener_in.filters, ranking=screener_in.ranking, rebalance=screener_in.rebalance)
    version = screener_version_service.create_version(db, screener.id, version_in, 1)
    return {"screener_id": screener.id, "name": screener.name, "version_id": version.id, "version_number": version.version_number, "message": "Screener created successfully"}

@router.post("/{screener_id}/versions", response_model=ScreenerVersionResponse)
def create_screener_version(screener_id: uuid.UUID, version_in: ScreenerVersionCreate, db: Session = Depends(get_db)):
    screener = db.query(Screener).filter(Screener.id == screener_id).first()
    if not screener:
        raise HTTPException(status_code=404, detail="Screener not found")
    last_v = db.query(ScreenerVersion).filter(ScreenerVersion.screener_id == screener_id).order_by(ScreenerVersion.version_number.desc()).first()
    version = screener_version_service.create_version(db, screener_id, version_in, (last_v.version_number + 1) if last_v else 1)
    return {"screener_id": screener_id, "version_id": version.id, "version_number": version.version_number, "message": "New version created successfully"}

@router.delete("/{screener_id}")
def delete_screener(screener_id: uuid.UUID, db: Session = Depends(get_db)):
    screener = screener_service.soft_delete_screener(db, screener_id, SYSTEM_USER_ID)
    if not screener:
        raise HTTPException(status_code=404, detail="Screener not found")
    return {"success": True, "message": "Screener deleted successfully", "data": {"id": str(screener.id), "is_active": screener.is_active, "deleted_at": screener.deleted_at.isoformat() if screener.deleted_at else None}}

@router.get("/{screener_id}/versions")
def get_screener_versions(screener_id: uuid.UUID, db: Session = Depends(get_db)):
    versions = db.query(ScreenerVersion).filter(ScreenerVersion.screener_id == screener_id).order_by(ScreenerVersion.version_number.desc()).all()
    return [{"id": str(v.id), "version_number": v.version_number, "created_at": v.created_at} for v in versions]

@router.get("/{screener_id}/versions/{version_id}/backtests")
def get_version_backtests(screener_id: uuid.UUID, version_id: uuid.UUID, db: Session = Depends(get_db)):
    runs = db.query(BacktestRun).filter(BacktestRun.screener_version_id == version_id).order_by(BacktestRun.created_at.desc()).all()
    result = []
    for run in runs:
        res = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run.id).first()
        result.append({
            "run_id": str(run.id), "run_name": run.run_name or f"Run {run.id}",
            "period": f"{run.from_date} to {run.to_date}", "rebalance": run.rebalance_frequency,
            "portfolio_size": run.portfolio_size, "wrh": run.wrh,
            "cagr": format_metric_value(res.metrics_json.get("cagr"), "%") if res and res.metrics_json else None,
            "total_return": format_metric_value(res.metrics_json.get("total_return"), '%') if res and res.metrics_json else None,
            "status": run.status, "created_at": run.created_at
        })
    return result

@router.get("/{screener_id}")
def get_screener_detail(screener_id: uuid.UUID, vid: uuid.UUID = None, db: Session = Depends(get_db)):
    """
    Get screener with version config.
      - ?vid=<uuid>  → returns that specific version
      - no vid       → returns the latest version
    """
    screener = screener_service.get_screener(db, screener_id)
    if not screener:
        raise HTTPException(status_code=404, detail="Screener not found.")

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

    return {
        "id": str(screener.id),
        "name": screener.name,
        "description": screener.description,
        "version_number": version.version_number,
        "version_id": str(version.id),
        "is_latest": is_latest,
        "filters": clean_filters,
        "universe": version.universe_json,
        "ranking": version.ranking_json,
    }

@router.post("/run-adhoc")
def run_screener_adhoc(payload: ScreenerVersionCreate, limit: int = None, offset: int = 0, db: Session = Depends(get_db)):
    return screener_execution_service.execute_adhoc(
        universe=payload.universe.model_dump(),
        filters=[f.model_dump() for f in payload.filters],
        ranking=payload.ranking.model_dump() if payload.ranking else None,
        limit=limit, offset=offset,
    )
