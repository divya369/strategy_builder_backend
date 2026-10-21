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

from datetime import date
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from app.core.trading_calendar import next_trading_day

# Below one year a return is reported as absolute, never annualised (GIPS /
# Indian MF factsheet convention) — annualising a few days turns -0.05% into
# +51% CAGR.
MIN_DAYS_FOR_CAGR = 365


def annualised_return(growth: float, days: int) -> Optional[float]:
    """CAGR for a growth multiple (end value / start value) over `days`
    calendar days. None when the period is under one year or growth <= 0."""
    if days < MIN_DAYS_FOR_CAGR or not growth or growth <= 0:
        return None
    return float(growth ** (365.25 / days) - 1.0)


# Fewer up (down) benchmark months than this and the upside (downside) capture
# is reported as None — a ratio over one or two months is noise, not a trait.
MIN_MONTHS_FOR_CAPTURE = 3


def capture_ratios(port_monthly: pd.Series, bm_monthly: Optional[pd.Series]) -> tuple:
    """(upside_capture, downside_capture) on MONTHLY returns — Morningstar method.

    Months are split by the sign of the benchmark's monthly return. For each
    side: geometric mean monthly return of the strategy / of the benchmark,
    where geometric mean = (prod(1 + r)) ** (1 / n) - 1. Returned as fractions
    (1.0 = 100%). A side with fewer than MIN_MONTHS_FOR_CAPTURE months is None.

    Monthly rather than daily: compounding every benchmark-up (or -down) DAY of
    a multi-year history compounds thousands of days into one number, which
    drifts downside capture towards ~100% and makes upside capture erratic.
    """
    if bm_monthly is None or port_monthly is None or port_monthly.empty or bm_monthly.empty:
        return None, None
    both = pd.concat([port_monthly.rename("p"), bm_monthly.rename("b")], axis=1, join="inner").dropna()

    def _side(mask) -> Optional[float]:
        side = both[mask]
        n = len(side)
        if n < MIN_MONTHS_FOR_CAPTURE:
            return None
        p_growth = float((1.0 + side["p"]).prod())
        b_growth = float((1.0 + side["b"]).prod())
        if p_growth <= 0 or b_growth <= 0:
            return None
        bm_geo = b_growth ** (1.0 / n) - 1.0
        if bm_geo == 0:
            return None
        return float((p_growth ** (1.0 / n) - 1.0) / bm_geo)

    return _side(both["b"] > 0), _side(both["b"] < 0)


def month_complete_on(last_date: date, next_market_date: Optional[date] = None) -> bool:
    """True when `last_date` is the final NSE trading day of its month, i.e.
    the month is complete once that day's EOD data is in.

    next_market_date: the actual next trading date from market data, when the
    caller knows it (historical periods — holidays.json covers recent years
    only). Otherwise the NSE trading calendar decides. A missing holiday can
    only make a complete month look incomplete (never the reverse), and that
    self-corrects with the next month's first trading day.
    """
    nd = next_market_date if next_market_date and next_market_date > last_date else next_trading_day(last_date)
    return (nd.year, nd.month) != (last_date.year, last_date.month)


def official_monthly_returns(
    nav: pd.Series,
    bm_nav: pd.Series,
    next_market_date: Optional[date] = None,
) -> tuple:
    """(strategy, benchmark) monthly returns used for the OFFICIAL capture ratios.

    - Both series are cut to their common dates, so every month-end value of
      the strategy and of the benchmark is taken on the same day — identical
      periods; a missing benchmark day is never treated as a 0% return.
    - The first month is excluded: it has no complete prior month-end (a
      mid-month start, and the strategy's base-100 is the start of day 1 while
      the benchmark's is day 1's close).
    - The last month is excluded until it is complete (month_complete_on), so
      daily NAV within a running month never moves the official ratios. That
      running month stays in the monthly returns table as MTD.
    """
    empty = pd.Series(dtype=float)
    if nav is None or bm_nav is None or nav.empty or bm_nav.empty:
        return empty, empty
    both = pd.concat([nav.astype(float).rename("p"), bm_nav.astype(float).rename("b")],
                     axis=1, join="inner").dropna().sort_index()
    both = both[(both["p"] > 0) & (both["b"] > 0)]
    if both.empty:
        return empty, empty
    month_end = both.resample("ME").last().dropna()
    rets = month_end.pct_change().iloc[1:]
    if len(rets) and not month_complete_on(both.index[-1].date(), next_market_date):
        rets = rets.iloc[:-1]
    return rets["p"], rets["b"]


def official_capture(
    nav: pd.Series,
    bm_nav: pd.Series,
    next_market_date: Optional[date] = None,
) -> tuple:
    """(upside_capture, downside_capture) over complete months only — the one
    entry point shared by the backtest engine and the paper detail."""
    port_m, bm_m = official_monthly_returns(nav, bm_nav, next_market_date)
    return capture_ratios(port_m, bm_m)


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
    next_market_date: Optional[date] = None,
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
        next_market_date: optional actual next trading date after the last NAV
            date (see month_complete_on); default is the NSE trading calendar.

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
    # Same base as total_return (100 = capital at the start of day 1), so the
    # period runs from the start of the first NAV day: +1 calendar day.
    elapsed_days = (nav.index[-1] - nav.index[0]).days + 1
    cagr = annualised_return(nav.iloc[-1] / 100.0, elapsed_days)

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
    calmar = (float(cagr / abs(max_dd)) if max_dd != 0 else 0.0) if cagr is not None else None

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
    hit_ratio = upside_cap = downside_cap = capture_through = None
    if benchmark_nav_by_date:
        bm_nav = pd.Series(benchmark_nav_by_date).sort_index().astype(float)
        bm_nav = bm_nav[bm_nav > 0]
        if not bm_nav.empty and bm_nav.iloc[0] > 0:
            benchmark_total_return = float(bm_nav.iloc[-1] / bm_nav.iloc[0] - 1.0)
            bm_elapsed = (bm_nav.index[-1] - bm_nav.index[0]).days
            benchmark_cagr = annualised_return(bm_nav.iloc[-1] / bm_nav.iloc[0], bm_elapsed)
            excess_cagr = cagr - benchmark_cagr if cagr is not None and benchmark_cagr is not None else None

            bm_ret = bm_nav.pct_change().fillna(0.0)
            port_ret = ret.reindex(bm_ret.index).fillna(0.0)
            both_nonzero = (port_ret != 0) | (bm_ret != 0)
            if both_nonzero.sum() > 0:
                hit_ratio = float((port_ret[both_nonzero] > bm_ret[both_nonzero]).mean())

            # Benchmark monthly returns. The prior month-end is padded with the
            # benchmark's day-1 value (the base-100 start), as the strategy is
            # padded with 100 — padding with the first month-END value would
            # force the first benchmark month to 0%.
            # dropna: a month without benchmark data is absent (shown as None),
            # not forward-filled into a 0% month.
            bm_monthly_nav = bm_nav.resample("ME").last().dropna()
            if not bm_monthly_nav.empty:
                bm_monthly_padded = pd.concat([
                    pd.Series([float(bm_nav.iloc[0])], index=[bm_monthly_nav.index[0] - pd.offsets.MonthEnd(1)]),
                    bm_monthly_nav,
                ])
                bm_monthly_rets = bm_monthly_padded.pct_change().dropna()

            # Official capture: complete months only, identical periods.
            port_m, bm_m = official_monthly_returns(nav, bm_nav, next_market_date)
            upside_cap, downside_cap = capture_ratios(port_m, bm_m)
            capture_through = port_m.index[-1].strftime("%Y-%m") if len(port_m) else None

    # The running (incomplete) month is listed as MTD: is_complete=False.
    last_month_complete = month_complete_on(nav.index[-1].date(), next_market_date)
    monthly_returns_list = []
    for i, (dt, mret) in enumerate(monthly_rets.items()):
        # A month with no benchmark data is shown as None, never as 0%.
        bm_val = bm_monthly_rets.get(dt) if bm_monthly_rets is not None else None
        bm_mret = float(bm_val) if bm_val is not None else None
        excess_mret = (float(mret) - bm_mret) if bm_mret is not None else None
        monthly_returns_list.append({
            "year": int(dt.year), "month": int(dt.month),
            "monthly_return": float(mret),
            "benchmark_monthly_return": bm_mret,
            "excess_monthly_return": excess_mret,
            "is_complete": bool(last_month_complete or i < len(monthly_rets) - 1),
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
            "capture_through": capture_through,  # last month-end in the capture (YYYY-MM)
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
