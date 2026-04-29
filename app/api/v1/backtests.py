import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.core.database import get_db, get_equity_db
from app.core.backtest_metric_formatter import format_metric_value
from app.schemas.backtest import CustomBacktestRequest
from app.services.backtest_engine import backtest_engine_service, _backtest_executor
from app.models.backtest import BacktestRun
from app.models.result import BacktestSummary, BacktestDailyNav
from app.models.screener import Screener, ScreenerVersion


router = APIRouter()

@router.post("/custom-run")
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

    run_id = backtest_engine_service.submit_backtest(db=db, request_data=req_dict, user_id=user_id, screener_id=screener_id, screener_version_id=screener_version_id)

    run_record = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
    if run_record and run_record.status == "RUNNING":
        _backtest_executor.submit(backtest_engine_service.execute_backtest_background, run_id)

    return {"status": "success", "run_id": str(run_id)}

@router.get("/{run_id}")
def get_backtest_result(run_id: uuid.UUID, db: Session = Depends(get_db)):
    run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Backtest run not found.")

    summary = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run_id).first()

    equity_curve = db.query(BacktestDailyNav).filter(BacktestDailyNav.backtest_run_id == run_id).order_by(BacktestDailyNav.trade_date).all()

    chart_data = [{"time": str(e.trade_date), "value": round(float(e.portfolio_nav_net),2), "drawdown": round(float(e.drawdown) * 100, 2)} for e in equity_curve] if equity_curve else []

    benchmark_curve = []
    if equity_curve and any(e.benchmark_nav is not None for e in equity_curve):
        benchmark_curve = [{"time": str(e.trade_date), "value": round(float(e.benchmark_nav), 2)} for e in equity_curve if e.benchmark_nav is not None]

    metrics = {}
    if summary and summary.metrics_json:
        m = summary.metrics_json

        metrics = {
            "cagr": format_metric_value(m.get("cagr"), "%"),
            "total_return": format_metric_value(m.get("total_return"), "%"),
            "volatility": format_metric_value(m.get("volatility"), "%"),
            "sharpe": format_metric_value(m.get("sharpe"), ""),
            "max_drawdown": format_metric_value(m.get("max_drawdown"), "%"),
            "final_nav": round(float(equity_curve[-1].portfolio_nav_net), 2) if equity_curve else 0.0,
        }

    return {"run_name": run.run_name, "status": run.status, "initial_capital": float(run.initial_capital), "metrics": metrics, "equity_curve": chart_data, "benchmark_curve": benchmark_curve}
