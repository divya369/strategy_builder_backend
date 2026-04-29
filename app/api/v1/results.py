import uuid
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.core.backtest_metric_formatter import OVERVIEW_METRICS_CONFIG, format_metric_value
from app.models.backtest import BacktestRun, BacktestRebalanceConstituent, BacktestHoldingPeriod
from app.models.result import BacktestDailyNav, BacktestRebalanceEvent, BacktestSummary, BacktestDrawdownEpisode, BacktestMonthlyReturn
from app.schemas.result import BacktestDrawdownEpisodeResponse, BacktestRebalanceEventResponse

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
    from collections import OrderedDict
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

@router.get("/dd-history/{run_id}", response_model=List[BacktestDrawdownEpisodeResponse])
def get_drawdowns(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    return db.query(BacktestDrawdownEpisode).filter(BacktestDrawdownEpisode.backtest_run_id == run_id).order_by(BacktestDrawdownEpisode.drawdown_pct.asc()).all()

@router.get("/monthly-returns/{run_id}")
def get_monthly_returns(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    rows = db.query(BacktestMonthlyReturn).filter(
        BacktestMonthlyReturn.backtest_run_id == run_id
    ).order_by(BacktestMonthlyReturn.year, BacktestMonthlyReturn.month).all()

    # Group by year and compute YTD
    from collections import OrderedDict
    years = OrderedDict()
    for r in rows:
        years.setdefault(r.year, []).append({
            "month": r.month,
            "monthly_return": float(r.monthly_return) if r.monthly_return is not None else None,
            "benchmark_monthly_return": float(r.benchmark_monthly_return) if r.benchmark_monthly_return is not None else None,
            "excess_monthly_return": float(r.excess_monthly_return) if r.excess_monthly_return is not None else None,
        })

    result = []
    for year, months in years.items():
        # Compound YTD: (1+r1) * (1+r2) * ... - 1
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

@router.get("/rebalance-history/{run_id}", response_model=List[BacktestRebalanceEventResponse])
def get_rebalance_history(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    return db.query(BacktestRebalanceEvent).filter(BacktestRebalanceEvent.backtest_run_id == run_id).order_by(BacktestRebalanceEvent.rebalance_date).all()

@router.get("/baskets/{run_id}")
def get_baskets(run_id: uuid.UUID, db: Session = Depends(get_db)):
    get_run_or_404(run_id, db)
    constituents = db.query(BacktestRebalanceConstituent).filter(
        BacktestRebalanceConstituent.backtest_run_id == run_id
    ).order_by(BacktestRebalanceConstituent.rebalance_date, BacktestRebalanceConstituent.rank_position).all()

    # Group by date → action
    from collections import OrderedDict
    dates = OrderedDict()
    for c in constituents:
        d = str(c.rebalance_date)
        if d not in dates:
            dates[d] = {"buy": [], "sell": [], "retain": []}

        item = c.symbol

        action = (c.action or "").upper()
        if action == "BUY":
            dates[d]["buy"].append(item)
        elif action == "SELL":
            dates[d]["sell"].append(item)
        else:
            dates[d]["retain"].append(item)

    return [{"date": d, **actions} for d, actions in dates.items()]

@router.get("/tradelog/{run_id}")
def get_tradelog_data(run_id: uuid.UUID, db: Session = Depends(get_db)):
    run = get_run_or_404(run_id, db)
    periods = db.query(BacktestHoldingPeriod).filter(BacktestHoldingPeriod.backtest_run_id == run_id).order_by(BacktestHoldingPeriod.entry_date).all()
    total_cost_perc = (float(run.transaction_cost_bps) + float(run.slippage_bps)) / 10000.0
    nominal_aum = float(run.initial_capital)
    response = []
    for p in periods:
        entry_p = float(p.entry_price) if p.entry_price else 0.0
        exit_p = float(p.exit_price) if p.exit_price else 0.0
        e_weight = float(p.entry_weight) if p.entry_weight else 0.0
        qty = int((nominal_aum * e_weight) / entry_p) if entry_p > 0 else 0
        gross_pnl_pct = float(p.gross_return) if p.gross_return is not None else ((exit_p / entry_p) - 1 if (exit_p and entry_p) else None)
        net_pnl_pct = float(p.net_return) if p.net_return is not None else ((gross_pnl_pct - total_cost_perc) if gross_pnl_pct is not None else None)
        charges = pnl_abs = None
        if qty > 0 and exit_p > 0:
            charges = (qty * entry_p + qty * exit_p) * (total_cost_perc / 2.0)
            pnl_abs = (qty * exit_p) - (qty * entry_p) - charges
        response.append({"symbol": p.symbol, "entry_date": str(p.entry_date), "exit_date": str(p.exit_date) if p.exit_date else None, "holding_days": p.holding_days, "entry_price": entry_p, "exit_price": exit_p, "entry_weight": e_weight, "exit_weight": float(p.exit_weight) if p.exit_weight else 0.0, "gross_return": gross_pnl_pct, "net_return": net_pnl_pct, "qty": qty, "charges": charges, "pnl_abs": pnl_abs, "exit_reason": p.exit_reason})
    return response
