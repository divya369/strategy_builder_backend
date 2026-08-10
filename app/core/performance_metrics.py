"""
Shared performance-metrics computation over a NAV series.

This is a faithful, standalone replica of the metric formulas used by the
backtest engine (app/services/backtest_engine.py, summary block). It is used by
the platform paper-trading detail endpoint so that paper metrics are computed
with the SAME institutional methodology as the backtest report — CAGR, total
return, annualized volatility/Sharpe/Sortino, max drawdown, Calmar, monthly
returns, and drawdown episodes.

The backtest engine is intentionally NOT refactored to call this (zero risk to
the running backtest path); instead a parity test feeds a backtest's own
daily_nav_json here and asserts the output equals the stored metrics_json.

Input NAV is a base-100 series (100.0 = strategy start). Daily returns are
derived from the NAV series so no separate return column is required.
"""
from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd


def make_drawdown_episodes(nav_series: pd.Series) -> List[dict]:
    """Peak/trough/recovery episodes — identical logic to the backtest engine."""
    episodes: List[dict] = []
    if nav_series.empty:
        return episodes
    running_peak = nav_series.cummax()
    dd = (nav_series - running_peak) / running_peak
    in_dd = False
    start_dt = trough_dt = None
    trough_dd = 0.0
    peak_nav = 0.0
    for dt, dd_val in dd.items():
        if dd_val < 0 and not in_dd:
            in_dd = True
            start_dt = dt
            trough_dt = dt
            trough_dd = float(dd_val)
            peak_nav = float(running_peak.loc[dt])
        elif dd_val < 0 and in_dd:
            if dd_val < trough_dd:
                trough_dd = float(dd_val)
                trough_dt = dt
        elif dd_val >= 0 and in_dd:
            episodes.append({
                "start_date": start_dt.date(), "trough_date": trough_dt.date(),
                "recovery_date": dt.date(), "drawdown": trough_dd,
                "peak_nav": peak_nav, "trough_nav": float(nav_series.loc[trough_dt]),
                "duration_days": int((dt - start_dt).days),
            })
            in_dd = False
            start_dt = trough_dt = None
            trough_dd = 0.0
            peak_nav = 0.0
    if in_dd and start_dt and trough_dt:
        episodes.append({
            "start_date": start_dt.date(), "trough_date": trough_dt.date(),
            "recovery_date": None, "drawdown": trough_dd,
            "peak_nav": peak_nav, "trough_nav": float(nav_series.loc[trough_dt]),
            "duration_days": int((nav_series.index[-1] - start_dt).days),
        })
    return episodes


def compute_summary_from_nav(
    nav: pd.Series,
    benchmark_nav_by_date: Optional[Dict[pd.Timestamp, float]] = None,
    risk_free_rate: float = 0.0,
    daily_returns: Optional[pd.Series] = None,
) -> dict:
    """Compute the institutional performance summary from a base-100 NAV series.

    Args:
        nav: base-100 NAV, indexed by Timestamp (sorted ascending).
        benchmark_nav_by_date: optional base-100 benchmark NAV keyed by Timestamp.
        risk_free_rate: annual risk-free rate (fraction).
        daily_returns: optional explicit daily-return series (fraction), indexed
            by Timestamp. When given, vol/Sharpe/Sortino use it verbatim (exact
            parity with the backtest's stored returns for the historical era);
            otherwise returns are derived from the NAV series. NAV-derived
            metrics (total_return, CAGR, drawdown, monthly) always use `nav`.

    Returns dict with the same keys/semantics as backtest metrics_json (the
    NAV-derivable subset) plus:
        drawdown_series  — per-date drawdown (fraction) for the drawdown graph
        monthly_returns  — [{year, month, monthly_return, benchmark_monthly_return, excess_monthly_return}]
        drawdown_episodes — [{peak_date, trough_date, recovery_date, drawdown_pct, ...}]
    """
    nav = nav.astype(float).sort_index()
    if daily_returns is not None:
        # Use the caller's explicit return series (exact parity path).
        ret = daily_returns.astype(float).sort_index()
    else:
        # Derive returns from NAV. Prepend a 100.0 anchor one period before the
        # first point so the FIRST day's move (relative to the 100.0 inception
        # base) is included — matching the engine's stored day-0 return.
        nav_anchored = pd.concat([
            pd.Series([100.0], index=[nav.index[0] - pd.Timedelta(days=1)]),
            nav,
        ])
        ret = nav_anchored.pct_change().dropna()

    total_return = float(nav.iloc[-1] / 100.0 - 1.0)
    elapsed_days = max(1, (nav.index[-1] - nav.index[0]).days)
    cagr = float((nav.iloc[-1] / nav.iloc[0]) ** (365.25 / elapsed_days) - 1.0) if nav.iloc[0] > 0 else 0.0

    annual_vol = float(ret.std(ddof=1) * np.sqrt(252)) if len(ret) > 1 else 0.0
    daily_rf = (1 + risk_free_rate) ** (1 / 252) - 1
    excess_daily = ret - daily_rf
    sharpe = (
        float(excess_daily.mean() / ret.std(ddof=1) * np.sqrt(252))
        if len(ret) > 1 and ret.std(ddof=1) > 0 else 0.0
    )
    downside_returns = np.minimum(excess_daily, 0.0)
    downside_dev = float(np.sqrt(np.mean(downside_returns ** 2)) * np.sqrt(252)) if len(excess_daily) else 0.0
    annualized_excess_return = float(excess_daily.mean() * 252) if len(excess_daily) else 0.0
    sortino = float(annualized_excess_return / downside_dev) if downside_dev > 0 else 0.0

    running_peak = nav.cummax()
    drawdown_series = (nav - running_peak) / running_peak
    max_dd = float(drawdown_series.min()) if not drawdown_series.empty else 0.0
    calmar = float(cagr / abs(max_dd)) if max_dd != 0 else 0.0

    # ── Monthly returns (pad a base-100 point at the prior month-end) ──────
    monthly_nav = nav.resample("ME").last()
    if not monthly_nav.empty:
        monthly_nav_padded = pd.concat([
            pd.Series([100.0], index=[monthly_nav.index[0] - pd.offsets.MonthEnd(1)]),
            monthly_nav,
        ])
        monthly_rets = monthly_nav_padded.pct_change().dropna()
    else:
        monthly_rets = pd.Series(dtype=float)

    # ── Benchmark ─────────────────────────────────────────────────────────
    bm_monthly_rets = None
    benchmark_total_return = benchmark_cagr = excess_cagr = None
    hit_ratio = upside_cap = downside_cap = None
    if benchmark_nav_by_date:
        bm_nav = pd.Series(benchmark_nav_by_date).sort_index().astype(float)
        bm_nav = bm_nav[bm_nav > 0]
        if not bm_nav.empty and bm_nav.iloc[0] > 0:
            benchmark_total_return = float(bm_nav.iloc[-1] / bm_nav.iloc[0] - 1.0)
            bm_elapsed = max(1, (bm_nav.index[-1] - bm_nav.index[0]).days)
            benchmark_cagr = float((bm_nav.iloc[-1] / bm_nav.iloc[0]) ** (365.25 / bm_elapsed) - 1.0)
            excess_cagr = cagr - benchmark_cagr

            bm_ret = bm_nav.pct_change().fillna(0.0)
            port_ret = ret.reindex(bm_ret.index).fillna(0.0)
            both_nonzero = (port_ret != 0) | (bm_ret != 0)
            if both_nonzero.sum() > 0:
                hit_ratio = float((port_ret[both_nonzero] > bm_ret[both_nonzero]).mean())
            up_days = bm_ret > 0
            if up_days.sum() > 0:
                bm_up = float((1 + bm_ret[up_days]).prod() - 1)
                port_up = float((1 + port_ret[up_days]).prod() - 1)
                upside_cap = port_up / bm_up if bm_up != 0 else None
            down_days = bm_ret < 0
            if down_days.sum() > 0:
                bm_down = float((1 + bm_ret[down_days]).prod() - 1)
                port_down = float((1 + port_ret[down_days]).prod() - 1)
                downside_cap = port_down / bm_down if bm_down != 0 else None

            # Benchmark monthly returns (same padding convention)
            bm_monthly_nav = bm_nav.resample("ME").last()
            if not bm_monthly_nav.empty:
                bm_monthly_padded = pd.concat([
                    pd.Series([bm_monthly_nav.iloc[0]], index=[bm_monthly_nav.index[0] - pd.offsets.MonthEnd(1)]),
                    bm_monthly_nav,
                ])
                bm_monthly_rets = bm_monthly_padded.pct_change().dropna()

    monthly_returns_list = []
    for dt, mret in monthly_rets.items():
        bm_mret = float(bm_monthly_rets.get(dt, 0.0)) if bm_monthly_rets is not None else None
        excess_mret = (float(mret) - bm_mret) if bm_mret is not None else None
        monthly_returns_list.append({
            "year": int(dt.year), "month": int(dt.month),
            "monthly_return": float(mret),
            "benchmark_monthly_return": bm_mret,
            "excess_monthly_return": excess_mret,
        })

    positive_month_pct = float((monthly_rets > 0).mean()) if len(monthly_rets) > 0 else 0.0
    best_month = float(monthly_rets.max()) if len(monthly_rets) > 0 else 0.0
    worst_month = float(monthly_rets.min()) if len(monthly_rets) > 0 else 0.0
    avg_month = float(monthly_rets.mean()) if len(monthly_rets) > 0 else 0.0

    # ── Drawdown episodes ─────────────────────────────────────────────────
    episodes = make_drawdown_episodes(nav)
    drawdown_episodes = [{
        "peak_date": str(ep["start_date"]), "trough_date": str(ep["trough_date"]),
        "recovery_date": str(ep["recovery_date"]) if ep["recovery_date"] else None,
        "drawdown_pct": float(ep["drawdown"]),
        "peak_to_trough_days": (ep["trough_date"] - ep["start_date"]).days,
        "trough_to_recovery_days": (ep["recovery_date"] - ep["trough_date"]).days if ep["recovery_date"] else None,
        "total_recovery_days": (ep["recovery_date"] - ep["start_date"]).days if ep["recovery_date"] else None,
    } for ep in episodes]

    return {
        "metrics": {
            "total_return": total_return,
            "cagr": cagr,
            "volatility": annual_vol,
            "sharpe": sharpe,
            "sortino": sortino,
            "calmar": calmar,
            "max_drawdown": max_dd,
            "benchmark_total_return": benchmark_total_return,
            "benchmark_cagr": benchmark_cagr,
            "excess_cagr": excess_cagr,
            "hit_ratio_vs_benchmark": hit_ratio,
            "upside_capture": upside_cap,
            "downside_capture": downside_cap,
            "positive_month_pct": positive_month_pct,
            "best_month": best_month,
            "worst_month": worst_month,
            "avg_month": avg_month,
            "final_nav": float(nav.iloc[-1]),
        },
        "drawdown_series": drawdown_series,
        "monthly_returns": monthly_returns_list,
        "drawdown_episodes": drawdown_episodes,
    }
