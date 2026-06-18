import uuid
from typing import List
from sqlalchemy.orm import Session
from app.core.database import get_db
from collections import OrderedDict
from fastapi import APIRouter, Depends, HTTPException
from app.models.result import BacktestSummary
from app.core.backtest_metric_formatter import OVERVIEW_METRICS_CONFIG, format_metric_value
from app.models.backtest import BacktestRun, BacktestHoldingPeriod

router = APIRouter()

# ── Overview metrics config ────────────────────────────────────────────────
# Single source of truth: (json_key, label, unit, section)
# To add a new metric: compute it in engine → add ONE tuple here. Done.
# OVERVIEW_METRICS_CONFIG = [
#     # Performance
#     ("cagr",           "CAGR",           "%",    "Performance"),
#     ("total_return",   "Total Return",   "%",    "Performance"),
#     ("max_drawdown",   "Max Drawdown",   "%",    "Performance"),
#     ("volatility",     "Volatility",     "%",    "Performance"),
#     ("sharpe",         "Sharpe Ratio",   "x",    "Performance"),
#     ("sortino",        "Sortino Ratio",  "x",    "Performance"),
#     ("calmar",         "Calmar Ratio",   "x",    "Performance"),
#     # Monthly
#     ("best_month",         "Best Month",       "%", "Monthly"),
#     ("worst_month",        "Worst Month",      "%", "Monthly"),
#     ("avg_month",          "Avg Month",        "%", "Monthly"),
#     ("positive_month_pct", "Positive Month %", "%", "Monthly"),
#     # Benchmark
#     ("benchmark_cagr",         "Benchmark CAGR",     "%", "Benchmark"),
#     ("excess_cagr",            "Excess CAGR (α)",    "%", "Benchmark"),
#     ("hit_ratio_vs_benchmark", "Hit Ratio vs Bench", "%", "Benchmark"),
#     ("upside_capture",         "Upside Capture",     "x", "Benchmark"),
#     ("downside_capture",       "Downside Capture",   "x", "Benchmark"),
#     # Turnover & Cost
#     ("total_rebalances",    "Total Rebalances",    "#",    "Turnover & Cost"),
#     ("avg_turnover",        "Avg Turnover",        "%",    "Turnover & Cost"),
#     ("annualized_turnover", "Annualized Turnover", "%",    "Turnover & Cost"),
#     ("total_cost_drag",     "Total Cost Drag",     "%",    "Turnover & Cost"),
#     # Holding
#     ("avg_holding_days",    "Avg Holding Days",    "days", "Holding"),
#     ("median_holding_days", "Median Holding Days", "days", "Holding"),
#     ("avg_retention_pct",   "Avg Retention %",     "%",    "Holding"),
#     ("avg_churn_pct",       "Avg Churn %",         "%",    "Holding"),
# ]


# def format_metric_value(value, unit: str):
#     if value is None:
#         return None

#     value = float(value)

#     if unit == "%":
#         return round(value * 100, 2)

#     if unit == "x":
#         return round(value, 2)

#     if unit == "#":
#         return int(value)

#     if unit == "days":
#         return round(value, 1)

#     return value

def get_run_or_404(run_id: uuid.UUID, db: Session):
    run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Backtest run not found.")
    return run

@router.get("/overview/{run_id}")
def get_overview(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    s = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run_id).first()
    if not s:
        raise HTTPException(status_code=404, detail="Summary not found.")

    m = s.metrics_json or {}

    # Group by section preserving config order
    sections = OrderedDict()
    for key, label, unit, section in OVERVIEW_METRICS_CONFIG:
        raw_value = m.get(key)
        sections.setdefault(section, []).append({
            "key": key,
            "label": label,
            "raw_value": raw_value,
            "value": format_metric_value(raw_value, unit),
            "unit": unit,
        })

    return [{"section": sec, "metrics": metrics} for sec, metrics in sections.items()]

@router.get("/dd-history/{run_id}")
def get_drawdowns(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    summary = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run_id).first()
    if not summary:
        raise HTTPException(status_code=404, detail="Summary not found.")
    return summary.drawdowns_json or []

@router.get("/monthly-returns/{run_id}")
def get_monthly_returns(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    summary = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run_id).first()
    if not summary:
        raise HTTPException(status_code=404, detail="Summary not found.")
    rows = summary.monthly_returns_json or []

    # Group by year and compute YTD (same logic as before, iterating dicts)
    years = OrderedDict()
    for r in rows:
        years.setdefault(r["year"], []).append({
            "month": r["month"],
            "monthly_return": r["monthly_return"],
            "benchmark_monthly_return": r.get("benchmark_monthly_return"),
            "excess_monthly_return": r.get("excess_monthly_return"),
        })

    result = []
    for year, months in years.items():
        ytd = 1.0
        for m in months:
            ret = m["monthly_return"]
            if ret is not None:
                ytd *= (1.0 + ret)
        result.append({
            "year": year,
            "months": months,
            "ytd": ytd - 1.0,
        })

    return result

@router.get("/rebalance-history/{run_id}")
def get_rebalance_history(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    summary = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run_id).first()
    if not summary:
        raise HTTPException(status_code=404, detail="Summary not found.")
    return summary.rebalance_events_json or []

@router.get("/baskets/{run_id}")
def get_baskets(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    summary = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run_id).first()
    if not summary:
        raise HTTPException(status_code=404, detail="Summary not found.")
    return summary.constituents_json or []

@router.get("/tradelog/{run_id}")
def get_tradelog_data(run_id: uuid.UUID, db: Session = Depends(get_db)):
    run = get_run_or_404(run_id, db)
    periods = db.query(BacktestHoldingPeriod).filter(BacktestHoldingPeriod.backtest_run_id == run_id).order_by(BacktestHoldingPeriod.entry_date).all()
    response = []
    for p in periods:
        entry_p = float(p.entry_price) if p.entry_price else 0.0
        exit_p = float(p.exit_price) if p.exit_price else 0.0
        response.append({
            "symbol": p.symbol,
            "entry_date": str(p.entry_date),
            "exit_date": str(p.exit_date) if p.exit_date else None,
            "holding_days": p.holding_days,
            "entry_price": entry_p,
            "exit_price": exit_p,
            "gross_return": float(p.gross_return) if p.gross_return is not None else None,
            "net_return": float(p.net_return) if p.net_return is not None else None,
            "qty": p.qty,
            "charges": float(p.cost_drag) if p.cost_drag is not None else None,
            "pnl_abs": float(p.pnl_abs) if p.pnl_abs is not None else None,
            "exit_reason": p.exit_reason,
        })
    return response
