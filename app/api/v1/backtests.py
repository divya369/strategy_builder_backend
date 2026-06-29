import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.backtest_metric_formatter import format_metric_value

from app.schemas.backtest import CustomBacktestRequest
from app.services.backtest_engine import backtest_engine_service
from app.tasks.backtest_tasks import run_backtest_task
from app.models.backtest import BacktestRun
from app.models.result import BacktestSummary
from app.models.screener import Screener, ScreenerVersion
from app.core.rate_limiter import rate_limit  


router = APIRouter()

@router.post("/custom-run", dependencies=[Depends(rate_limit(max_requests=5, window_seconds=60))])
def run_custom_backtest(req: CustomBacktestRequest, db: Session = Depends(get_db)):
    user_id = req.user_id
    screener_id = req.screener_id
    screener_version_id = req.screener_version_id

    if screener_id is not None:
        screener = db.query(Screener).filter(Screener.id == screener_id, Screener.is_active == True).first()
        if not screener:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Screener not found.")

    if screener_version_id is not None and screener_id is not None:
        version = db.query(ScreenerVersion).filter(ScreenerVersion.id == screener_version_id, ScreenerVersion.screener_id == screener_id).first()
        if not version:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Screener version not found or does not belong to this screener.")

    req_dict = req.model_dump()
    req_dict.setdefault("transaction_cost_bps", 20.0)
    req_dict.setdefault("slippage_bps", 10.0)

    # submit_backtest now returns a BacktestRun ORM object (not just UUID)
    run_record = backtest_engine_service.submit_backtest(
        db=db, request_data=req_dict, user_id=user_id,
        screener_id=screener_id, screener_version_id=screener_version_id
    )

    # ── Smart dispatch: only enqueue to Celery if not already assigned ────
    if run_record.status == "COMPLETED":
        return {
            "status": "success",
            "run_id": str(run_record.id),
            "message": "Existing completed backtest returned.",
        }

    if run_record.status in ("QUEUED", "RUNNING"):
        # Only submit to Celery if no task is already assigned
        if not run_record.celery_task_id:
            task = run_backtest_task.delay(str(run_record.id))
            run_record.celery_task_id = task.id
            db.commit()

        return {
            "status": "success",
            "run_id": str(run_record.id),
            "message": "Backtest submitted.",
        }

    # Fallback (should not normally reach here)
    return {"status": "success", "run_id": str(run_record.id)}

@router.get("/{run_id}")
def get_backtest_result(run_id: uuid.UUID, db: Session = Depends(get_db)):
    run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Backtest run not found.")

    summary = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run_id).first()

    # Read chart data from JSONB instead of querying backtest_daily_nav table
    daily_nav_data = summary.daily_nav_json or [] if summary else []

    chart_data = [{"time": e["trade_date"], "value": round(e["portfolio_nav_net"], 2), "drawdown": round(e["drawdown"] * 100, 2)} for e in daily_nav_data]

    benchmark_curve = []
    if daily_nav_data and any(e.get("benchmark_nav") is not None for e in daily_nav_data):
        benchmark_curve = [{"time": e["trade_date"], "value": round(e["benchmark_nav"], 2)} for e in daily_nav_data if e.get("benchmark_nav") is not None]

    metrics = {}
    if summary and summary.metrics_json:
        m = summary.metrics_json

        metrics = {
            "cagr": format_metric_value(m.get("cagr"), "%"),
            "total_return": format_metric_value(m.get("total_return"), "%"),
            "volatility": format_metric_value(m.get("volatility"), "%"),
            "sharpe": format_metric_value(m.get("sharpe"), ""),
            "max_drawdown": format_metric_value(m.get("max_drawdown"), "%"),
            "final_nav": round(daily_nav_data[-1]["portfolio_nav_net"], 2) if daily_nav_data else 0.0,
        }

    return {"run_name": run.run_name, "status": run.status, "initial_capital": float(run.initial_capital), "metrics": metrics, "equity_curve": chart_data, "benchmark_curve": benchmark_curve, "benchmark_label": run.benchmark_symbol or "NIFTY 50"}
