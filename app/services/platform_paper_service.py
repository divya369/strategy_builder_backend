"""
Platform paper-trading service for ready-to-use (role="platform") strategies.

Flow:
  1. start_paper_tracking(): one-time bootstrap after admin runs a backtest.
     - Backfills platform_paper_equitycurve from backtest_summary.daily_nav_json
       (gives update_equitycurve its required "first date data" + full history).
     - Backfills platform_paper_tradelog from backtest_holding_period:
         NOT_IN_TOP_N     -> closed rows (realised pnl)
         END_OF_BACKTEST  -> OPEN rows (active=True) — the backtest's synthetic
                             final sell is ignored; the paper portfolio keeps
                             holding those exact stocks and continues forward.
       (Backtest tables themselves are never modified.)
  2. run_daily_update(): daily job (after market close), catching up any
     missed trading days in order.
     - On rebalance-due days: synthetic fills (order_id = "PAPER-...") with the
       SAME rules as the backtest engine — previous-day screener signal, fills
       at the day's open, the backtest's per-side cost and cash rule — so the
       paper portfolio is the backtest continued forward.
     - Every trading day: run the SAME update_tradelog() + update_equitycurve()
       functions as the live 16:30 job (MTM at close). No broker, no approval.

This module only IMPORTS from live_investment_service — it never modifies it.
"""
from __future__ import annotations

import logging
import uuid
from bisect import bisect_left, bisect_right
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import EquitySessionLocal, equity_engine
from app.core.symbol_registry import resolve_stock_symbol
from app.services import csv_data_service, equity_data_service
from app.services.screener_execution_service import screener_execution_service
from app.core.trading_calendar import (
    is_trading_day,
    next_rebalance_prepare_date,
    next_trading_day,
)
from app.models.backtest import BacktestHoldingPeriod, BacktestRun
from app.models.platform_paper import (
    PaperBuyStock,
    PaperEquityCurve,
    PaperPortfolio,
    PaperSellStock,
    PaperTradelog,
)
from app.models.result import BacktestSummary
from app.models.screener import Screener, ScreenerVersion
from app.core.config import settings
from app.core.backtest_metric_formatter import format_metric_value
from app.core.filter_registry import get_filter_label, get_sort_label
from app.core.performance_metrics import annualised_return, compute_summary_from_nav
from app.schemas.live_investment import EquityCurvePointResponse
# Shared equitycase-style machinery — imported, NEVER modified.
from app.services.live_investment_service import (
    _get_benchmark_index_name,
    insert_df_to_db,
    insert_single_row_to_db,
    make_buy_table,
    make_circuit_table,
    make_sell_table,
    model_df,
    trade_stats,
    update_df_to_db,
    update_equitycurve,
    update_tradelog,
    upsert_df_to_db,
)

logger = logging.getLogger("paper_trading")


def _paper_order_id() -> str:
    return f"PAPER-{uuid.uuid4().hex[:8]}"


PAPER_DEFAULT_COST_RATE = 0.003  # 0.30%/side, used only if the source backtest is gone
PAPER_MAX_START_GAP_TRADING_DAYS = 5  # a backtest older than this must be re-run before paper starts

# Bump when the stored detail payload's formulas or shape change: stored
# payloads with another version are recomputed on their next page open.
DETAIL_JSON_VERSION = 2  # 2: capture over complete months only + MTD flag


def _json_safe(obj):
    """Deep copy with numpy scalars -> Python and NaN/inf -> None (JSONB rejects NaN)."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, np.generic):
        obj = obj.item()
    if isinstance(obj, float) and not np.isfinite(obj):
        return None
    return obj


def _next_rebalance_after(day: date, frequency: str) -> date:
    """First rebalance (trade) day strictly after `day`, for a portfolio that is
    up to date as of `day` — the engine's "first trading day of the next ISO
    week / month". The prepare date is searched from `day` itself: from day+1,
    a `day` that is the week's (month's) last trading day skips a whole period."""
    return next_trading_day(next_rebalance_prepare_date(day, frequency))


def _cost_rate(run: BacktestRun) -> float:
    """Per-side cost of the source backtest (transaction + slippage), as a fraction."""
    return (float(run.transaction_cost_bps or 0.0) + float(run.slippage_bps or 0.0)) / 10000.0


def _index_close_map(index_table: str = "NIFTY 50") -> dict:
    """date -> close map for backfilling index_price. Empty dict on failure."""
    try:
        df = pd.read_sql_table(index_table, equity_engine)
        if df.empty or "close" not in df.columns or "date" not in df.columns:
            return {}
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return dict(zip(df["date"], df["close"].astype(float)))
    except Exception:
        return {}


class PlatformPaperService:

    # ── Bootstrap ─────────────────────────────────────────────────────────

    @staticmethod
    def start_paper_tracking(
        db: Session,
        screener_id,
        backtest_run_id=None,
        strategy_name: Optional[str] = None,
    ) -> PaperPortfolio:
        """Create the paper portfolio for a platform screener and backfill history
        from its completed backtest. One ACTIVE portfolio per screener."""
        screener = db.query(Screener).filter(Screener.id == screener_id).first()
        if not screener:
            raise HTTPException(status_code=404, detail="Screener not found")
        if screener.role != "platform":
            raise HTTPException(status_code=400, detail="Paper trading is only for platform screeners")

        existing = db.query(PaperPortfolio).filter(
            PaperPortfolio.screener_id == screener_id,
            PaperPortfolio.status == "ACTIVE",
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="An ACTIVE paper portfolio already exists for this screener")

        # Resolve the source backtest run (given id, or latest COMPLETED for screener)
        q = db.query(BacktestRun).filter(BacktestRun.screener_id == screener_id, BacktestRun.status == "COMPLETED")
        if backtest_run_id:
            run = q.filter(BacktestRun.id == backtest_run_id).first()
            if not run:
                raise HTTPException(status_code=404, detail="COMPLETED backtest run not found for this screener")
        else:
            run = q.order_by(BacktestRun.created_at.desc()).first()
            if not run:
                raise HTTPException(status_code=400, detail="No COMPLETED backtest found — run a backtest first")

        summary = db.query(BacktestSummary).filter(BacktestSummary.backtest_run_id == run.id).first()
        if not summary or not summary.daily_nav_json:
            raise HTTPException(status_code=400, detail="Backtest has no daily NAV data")

        # Paper continues the backtest from its last NAV day and catches up the
        # gap. A long gap would be simulated in hindsight yet stored as paper
        # tracking (and could be cherry-picked), so it must be re-backtested.
        today = date.today()
        last_nav_day = pd.to_datetime(summary.daily_nav_json[-1]["trade_date"]).date()
        gap = _trading_days_after(last_nav_day, today)
        if gap > PAPER_MAX_START_GAP_TRADING_DAYS:
            raise HTTPException(status_code=400, detail=(
                f"Backtest ends on {last_nav_day:%d-%m-%Y}, {gap} trading days ago. Paper trading can continue "
                f"a backtest at most {PAPER_MAX_START_GAP_TRADING_DAYS} trading days old — re-run the backtest "
                f"up to the latest date, then start paper trading."))

        version = db.query(ScreenerVersion).filter(ScreenerVersion.id == run.screener_version_id).first()

        initial_capital = float(run.initial_capital)
        portfolio = PaperPortfolio(
            screener_id=screener.id,
            screener_version_id=run.screener_version_id,
            backtest_run_id=run.id,
            strategy_name=strategy_name or screener.name,
            portfolio_size=run.portfolio_size,
            worst_hold_rank=run.wrh,
            rebalance_frequency=run.rebalance_frequency,
            initial_aum=initial_capital,
            status="ACTIVE",
            start_date=run.from_date,
            backfill_end_date=run.to_date,
            filters_json=version.filters_json if version else None,
            universe_json=version.universe_json if version else None,
            ranking_json=version.ranking_json if version else None,
        )
        db.add(portfolio)
        db.flush()  # portfolio.id needed for child rows

        n_curve, n_trades = PlatformPaperService._bootstrap_from_backtest(db, portfolio, run, summary)
        db.commit()
        logger.info("[Paper] Started portfolio %s for screener %s — %d curve rows, %d trades backfilled",
                    portfolio.id, screener_id, n_curve, n_trades)

        # Fill the gap since the backtest's last day right away (days whose EOD
        # data is loaded); anything not ready yet is picked up by the 23:15 cron.
        try:
            PlatformPaperService.catch_up(db, portfolio, today)
        except Exception:
            db.rollback()
            logger.exception("[Paper] Immediate catch-up failed for %s — the daily cron will retry", portfolio.id)
        db.refresh(portfolio)
        return portfolio

    @staticmethod
    def _bootstrap_from_backtest(db: Session, portfolio: PaperPortfolio, run: BacktestRun, summary: BacktestSummary) -> tuple[int, int]:
        """Seed an (empty) paper portfolio with its backtest's history: tradelog,
        equity curve, cash/holdings and next rebalance date. Does not commit.
        Shared by start_paper_tracking and scripts/rebuild_paper_portfolios.py.
        Returns (curve_rows, tradelog_rows)."""
        initial_capital = float(run.initial_capital)
        cost_rate = _cost_rate(run)
        n_trades, open_value, ledger = PlatformPaperService._backfill_tradelog(db, portfolio, run, summary, cost_rate)
        # The backtest deducts a hypothetical sell of every END_OF_BACKTEST
        # holding from its final NAV (backtest_engine liquidation step). Paper
        # keeps holding them, so that cost is added back at handover.
        eob_sell_cost = round(open_value * cost_rate, 2)
        n_curve = PlatformPaperService._backfill_equitycurve(db, portfolio, run, summary, ledger, eob_sell_cost)

        # Reconcile last curve row's cash/stocks split with the seeded open holdings
        last_curve = db.query(PaperEquityCurve).filter(
            PaperEquityCurve.automate_equity_ra_id == portfolio.id,
        ).order_by(PaperEquityCurve.date.desc(), PaperEquityCurve.total_days.desc()).first()
        if last_curve is not None:
            last_aum = float(last_curve.aum or initial_capital)
            last_curve.stocks_value = round(open_value, 2)
            last_curve.cash = round(last_aum - open_value, 2)
            portfolio.cash = last_curve.cash
            portfolio.stock_value = last_curve.stocks_value
            portfolio.final_aum = last_aum
            portfolio.pnl = float(last_curve.total_pnl or (last_aum - initial_capital))
            portfolio.todays_pnl = float(last_curve.strategy_daily_return or 0.0)
            portfolio.last_updated_date = last_curve.date
            # From the backtest's last trading day (to_date may be a weekend/holiday)
            portfolio.next_rebalance_date = _next_rebalance_after(last_curve.date, portfolio.rebalance_frequency)

        # Store institutional headline ratios on the latest row so the card
        # matches the detail page from the very first (backfill-only) render.
        db.flush()
        PlatformPaperService._store_institutional_on_latest_row(db, portfolio)
        return n_curve, n_trades

    @staticmethod
    def stop_paper_tracking(db: Session, screener_id) -> dict:
        """Stop paper tracking AND permanently delete all data for the screener.

        Stopping is a hard reset in one step: removes every paper portfolio
        (→ equity curve, tradelog, buy/sell) and every backtest_run (→ summary,
        holding_period) for the screener. Child rows cascade via DB
        ondelete=CASCADE. The screener row and its versions are KEPT so the
        admin can re-backtest and start again on a clean slate.

        The strategy must already be deactivated (is_active=False) — otherwise
        users would keep seeing a strategy whose data just got deleted.
        Irreversible.
        """
        screener = db.query(Screener).filter(Screener.id == screener_id).first()
        if not screener:
            raise HTTPException(status_code=404, detail="Screener not found")

        active = db.query(PaperPortfolio).filter(
            PaperPortfolio.screener_id == screener_id,
            PaperPortfolio.status == "ACTIVE",
        ).first()
        if not active:
            raise HTTPException(status_code=400, detail="No active paper trading to stop for this screener")

        if screener.is_active:
            raise HTTPException(status_code=400, detail="Deactivate the strategy before stopping paper trading")

        # Delete portfolios first (they FK->backtest_run with SET NULL), then runs.
        n_paper = db.query(PaperPortfolio).filter(
            PaperPortfolio.screener_id == screener_id,
        ).delete(synchronize_session=False)
        n_backtest = db.query(BacktestRun).filter(
            BacktestRun.screener_id == screener_id,
        ).delete(synchronize_session=False)
        db.commit()
        logger.info("[Paper] Stopped + purged screener %s — deleted %d paper portfolios, %d backtest runs",
                    screener_id, n_paper, n_backtest)
        return {"paper_portfolios_deleted": n_paper, "backtest_runs_deleted": n_backtest}

    # ── Detail (backtest-style results summary, backtest-standard metrics) ──

    @staticmethod
    def _stitched_summary(db: Session, portfolio: PaperPortfolio):
        """Stitch backtest history + live paper days into one NAV series and
        compute the institutional summary (backtest methodology).

        Returns (out, nav, bench_by_date) where `out` is the
        compute_summary_from_nav() result. Shared by build_detail() and
        card_metrics() so cards and the detail page always agree.
        """
        # Only the column used — the summary row's other JSONB columns
        # (rebalance events, constituents, drawdowns) are large and not needed.
        summary_row = db.query(BacktestSummary.daily_nav_json).filter(
            BacktestSummary.backtest_run_id == portfolio.backtest_run_id,
        ).first()
        daily_nav_json = summary_row[0] if summary_row else None
        backfill_end = portfolio.backfill_end_date

        # Historical era: backtest daily_nav (precise nav + stored returns)
        hist_nav, hist_ret = {}, {}
        if daily_nav_json:
            for e in daily_nav_json:
                d = pd.Timestamp(e["trade_date"])
                hist_nav[d] = float(e["portfolio_nav_net"])
                hist_ret[d] = float(e.get("portfolio_return_net") or 0.0)

        # Live era + continuous raw benchmark from paper equity curve.
        # The handover day (last backtest day) is taken from the paper curve too:
        # its row has the backtest's hypothetical liquidation cost added back
        # (paper keeps the stocks), unlike daily_nav_json.
        handover = max(hist_nav) if hist_nav else None
        # Only the 3 columns used, not the full ~50-column ORM rows.
        curve_rows = db.query(
            PaperEquityCurve.date, PaperEquityCurve.benchmark_price, PaperEquityCurve.equitycurve_percent,
        ).filter(
            PaperEquityCurve.automate_equity_ra_id == portfolio.id,
        ).order_by(PaperEquityCurve.date.asc(), PaperEquityCurve.total_days.asc()).all()
        live_nav, bench_raw = {}, {}
        for r in curve_rows:
            d = pd.Timestamp(r.date)
            if r.benchmark_price:
                bench_raw[d] = float(r.benchmark_price)
            if (backfill_end and r.date > backfill_end) or (handover is not None and d == handover):
                live_nav[d] = float(r.equitycurve_percent or 0.0)

        nav_map = dict(hist_nav)
        nav_map.update(live_nav)
        nav = pd.Series(nav_map).sort_index()
        if nav.empty:
            raise HTTPException(status_code=400, detail="No NAV data available for this strategy")

        # Returns: stored (exact) for backtest dates, nav-derived for paper days
        # (anchored at the 100.0 inception base so a first-day value has one).
        derived = pd.concat([pd.Series([100.0], index=[nav.index[0] - pd.Timedelta(days=1)]), nav]).pct_change()
        combined_ret = pd.Series(hist_ret, dtype=float).reindex(nav.index)
        use_derived = ~nav.index.isin(list(hist_ret)) | nav.index.isin(list(live_nav))
        combined_ret = combined_ret.where(~use_derived, derived.reindex(nav.index))
        combined_ret = combined_ret.dropna()

        # Benchmark: raw index close (continuous hist+live) rebased to 100
        bench_by_date = None
        if bench_raw:
            bs = pd.Series(bench_raw).sort_index()
            bs = bs[bs > 0]
            if not bs.empty:
                base = float(bs.iloc[0])
                bench_by_date = {d: float(v / base * 100.0) for d, v in bs.items()}

        rf = float(settings.RISK_FREE_RATE or 0.0)
        out = compute_summary_from_nav(nav, bench_by_date, rf, daily_returns=combined_ret)
        return out, nav, bench_by_date

    @staticmethod
    def _detail_sections(out: dict, nav: pd.Series, bench_by_date: Optional[dict]) -> dict:
        """The computed part of the detail payload (metrics, benchmark metrics,
        equity/benchmark curves, monthly returns, drawdown history) — what is
        stored in PaperPortfolio.detail_json."""
        m = out["metrics"]

        def pct(v):
            return format_metric_value(v, "%") if v is not None else None

        def x2(v):
            return round(float(v), 2) if v is not None else None

        metrics = {
            "cagr": pct(m["cagr"]), "total_return": pct(m["total_return"]),
            "volatility": pct(m["volatility"]), "sharpe": x2(m["sharpe"]),
            "sortino": x2(m["sortino"]), "calmar": x2(m["calmar"]),
            "max_drawdown": pct(m["max_drawdown"]), "final_nav": round(m["final_nav"], 2),
        }
        benchmark_metrics = {
            "benchmark_cagr": pct(m["benchmark_cagr"]),
            "benchmark_total_return": pct(m["benchmark_total_return"]),
            "excess_cagr": pct(m["excess_cagr"]),
            "hit_ratio_vs_benchmark": pct(m["hit_ratio_vs_benchmark"]),
            # Percent, as in the backtest report (1.0 ratio -> 100.0)
            "upside_capture": pct(m["upside_capture"]),
            "downside_capture": pct(m["downside_capture"]),
            # Capture covers complete months only; the running month is excluded.
            "capture_through": m.get("capture_through"),
        }

        dd = out["drawdown_series"].reindex(nav.index)
        equity_curve = [{
            "time": d.strftime("%Y-%m-%d"),
            "value": round(float(v), 2),
            "drawdown": round(float(x) * 100.0, 2),
        } for d, v, x in zip(nav.index, nav.values, dd.values)]
        benchmark_curve = [{"time": d.strftime("%Y-%m-%d"), "value": round(v, 2)}
                           for d, v in (bench_by_date or {}).items()]

        # Monthly returns grouped by year with YTD (same shape as results API)
        from collections import OrderedDict
        years = OrderedDict()
        for row in out["monthly_returns"]:
            years.setdefault(row["year"], []).append({
                "month": row["month"], "monthly_return": row["monthly_return"],
                "benchmark_monthly_return": row["benchmark_monthly_return"],
                "excess_monthly_return": row["excess_monthly_return"],
                "is_complete": row["is_complete"],  # False = running month (MTD)
            })
        monthly_returns = []
        for y, months in years.items():
            ytd = 1.0
            for mm in months:
                if mm["monthly_return"] is not None:
                    ytd *= (1.0 + mm["monthly_return"])
            monthly_returns.append({"year": y, "months": months, "ytd": ytd - 1.0})

        drawdown_history = sorted(out["drawdown_episodes"], key=lambda e: e["drawdown_pct"])[:5]

        return {
            "metrics": metrics,
            "benchmark_metrics": benchmark_metrics,
            "equity_curve": equity_curve,
            "benchmark_curve": benchmark_curve,
            "monthly_returns": monthly_returns,
            "drawdown_history": drawdown_history,
        }

    @staticmethod
    def _compute_and_store_summary(db: Session, portfolio: PaperPortfolio) -> dict:
        """ONE institutional computation per data change, stored for both readers:
          - headline ratios on the LATEST equity-curve row (what the card reads),
          - the full detail payload on portfolio.detail_json (what the detail
            page reads), stamped with DETAIL_JSON_VERSION and the as-of date.
        Does not commit. Raises on failure (the caller decides)."""
        out, nav, bench_by_date = PlatformPaperService._stitched_summary(db, portfolio)
        m = out["metrics"]
        latest = db.query(PaperEquityCurve).filter(
            PaperEquityCurve.automate_equity_ra_id == portfolio.id,
        ).order_by(PaperEquityCurve.date.desc(), PaperEquityCurve.total_days.desc()).first()
        if latest is not None:
            # None under 1 year (absolute return only) — store that too.
            latest.cagr_percent = round(float(m["cagr"]) * 100.0, 2) if m.get("cagr") is not None else None
            if m.get("sharpe") is not None:
                latest.sharpe = round(float(m["sharpe"]), 2)
            latest.calmar = round(float(m["calmar"]), 2) if m.get("calmar") is not None else None
            if m.get("sortino") is not None:
                latest.sortino_ratio = round(float(m["sortino"]), 2)
            if m.get("max_drawdown") is not None:
                latest.max_dd_percent = round(float(m["max_drawdown"]) * 100.0, 2)

        detail = _json_safe(PlatformPaperService._detail_sections(out, nav, bench_by_date))
        detail["version"] = DETAIL_JSON_VERSION
        detail["as_of"] = str(portfolio.last_updated_date) if portfolio.last_updated_date else None
        portfolio.detail_json = detail
        return detail

    @staticmethod
    def _store_institutional_on_latest_row(db: Session, portfolio: PaperPortfolio) -> None:
        """Called after backfill and after each daily update: refresh the stored
        card ratios + detail payload. Fully defensive — never breaks the caller;
        on failure the detail page recomputes on its next open instead."""
        try:
            PlatformPaperService._compute_and_store_summary(db, portfolio)
        except Exception:
            logger.exception("[Paper] institutional metric store failed for %s", portfolio.id)

    @staticmethod
    def _stored_detail(portfolio: PaperPortfolio) -> Optional[dict]:
        """portfolio.detail_json if it is current (same payload version and
        computed for the portfolio's latest day), else None."""
        d = portfolio.detail_json
        if not isinstance(d, dict) or d.get("version") != DETAIL_JSON_VERSION:
            return None
        as_of = str(portfolio.last_updated_date) if portfolio.last_updated_date else None
        return d if d.get("as_of") == as_of else None

    @staticmethod
    def card_metrics(portfolio: PaperPortfolio, latest_row: "PaperEquityCurve") -> dict:
        """Headline card metrics (CAGR, Max DD, Sharpe, NAV) read straight from
        the latest paper equity-curve row — same cheap, single-row fetch the
        live-investment cards use. No recompute.

        The row's cagr_percent/sharpe/max_dd_percent are written with the
        INSTITUTIONAL methodology by _store_institutional_on_latest_row (after
        backfill and each daily update), so the card matches the detail page.
        """
        def r2(v):
            return round(float(v), 2) if v is not None else None

        return {
            "cagr": r2(latest_row.cagr_percent),            # already a percent
            "max_drawdown": r2(latest_row.max_dd_percent),  # percent, negative
            "sharpe": r2(latest_row.sharpe),
            "nav": r2(latest_row.equitycurve_percent),      # base-100 NAV
            # percent since inception — from base-100 NAV, same as the detail page
            "total_return": r2(latest_row.equitycurve_percent - 100.0) if latest_row.equitycurve_percent is not None else None,
            "current_value": r2(latest_row.aum),
            "since_date": str(portfolio.start_date),
            "as_of": str(portfolio.last_updated_date) if portfolio.last_updated_date else None,
        }

    @staticmethod
    def build_detail(db: Session, screener_id) -> dict:
        """Build the user-facing detail payload for an ACTIVE paper strategy.

        Metrics/graphs use the INSTITUTIONAL (backtest) methodology computed
        from a stitched NAV series — backtest daily_nav (full precision) for the
        historical era + paper equity curve for the live days. The paper table's
        own live-formula metric columns are intentionally NOT read. They are
        computed once per daily update and stored in portfolio.detail_json;
        this only reads them (see _stored_detail / _compute_and_store_summary).

        Response shape mirrors the backtest results API so the frontend reuses
        the same components; the bottom section is the current portfolio instead
        of the full tradelog/rebalances.
        """
        portfolio = db.query(PaperPortfolio).filter(
            PaperPortfolio.screener_id == screener_id,
            PaperPortfolio.status == "ACTIVE",
        ).first()
        if not portfolio:
            raise HTTPException(status_code=404, detail="No active paper portfolio for this screener")

        # Strategy config block (name/description/universe/ranking/filters) —
        # description lives on the Screener; ranking + filters on the version.
        screener = db.query(Screener).filter(Screener.id == portfolio.screener_id).first()
        version = db.query(ScreenerVersion).filter(
            ScreenerVersion.id == portfolio.screener_version_id).first()
        # Send filters in the compact shape the frontend uses — drop the keys
        # that are null for this filter type (period/relation/left_field/...),
        # and add a human-readable `label` (same registry the builder uses) so
        # the display page needn't map raw field keys client-side.
        raw_filters = (version.filters_json if version else None) or []
        def _clean_filter(f):
            if not isinstance(f, dict):
                return f
            c = {k: v for k, v in f.items() if v is not None}
            c["label"] = get_filter_label(c.get("field"), c.get("period"))
            return c
        clean_filters = [_clean_filter(f) for f in raw_filters]
        # Ranking: keep the raw field/order, but add a human-readable label +
        # direction so this display-only page needn't map the key client-side.
        rk = (version.ranking_json if version else None) or {}
        ranking = None
        if rk.get("field"):
            order = (rk.get("order") or "desc").lower()
            ranking = {
                "field": rk.get("field"),
                "period": rk.get("period"),
                "order": order,
                "label": get_sort_label(rk.get("field"), rk.get("period")),
                "direction": "Ascending" if order == "asc" else "Descending",
            }
        strategy_block = {
            "name": portfolio.strategy_name or (screener.name if screener else None),
            "description": screener.description if screener else None,
            "universe": portfolio.universe_json,
            "rebalance_frequency": portfolio.rebalance_frequency,  # WEEKLY / MONTHLY
            # Params the admin's backtest ran with, copied onto the portfolio at
            # start — the user gets these as defaults and may edit them on invest.
            "portfolio_size": portfolio.portfolio_size,
            "wrh": portfolio.worst_hold_rank,
            "ranking": ranking,
            "filters": clean_filters,
        }

        # Metrics/curves/monthly/drawdowns: precomputed by the nightly update
        # (or at start) and read as-is. Computed here only when missing or stale
        # (new column, payload version bump, or a failed nightly store) — then
        # stored, so the next open is a plain read again.
        detail = PlatformPaperService._stored_detail(portfolio)
        if detail is None:
            detail = PlatformPaperService._compute_and_store_summary(db, portfolio)
            try:
                db.commit()
            except Exception:
                logger.exception("[Paper] could not store detail_json for %s — served uncached", portfolio.id)
                db.rollback()

        # Current portfolio (active paper holdings) — replaces backtest tradelog
        trades = db.query(PaperTradelog).filter(
            PaperTradelog.automate_equity_ra_id == portfolio.id,
            PaperTradelog.active == True,
        ).order_by(PaperTradelog.buy_date.asc()).all()
        current_portfolio = [{
            "tradingsymbol": t.tradingsymbol, "buy_date": str(t.buy_date),
            "buy_price": t.buy_price, "qty": t.buy_qty, "ltp": t.ltp,
            "current_value": t.current_value, "unrealised_pnl": t.unrealised_pnl,
            "profit_percent": t.profit_percent, "hold_days": t.hold,
            "weightage": round((t.current_value or 0) / float(portfolio.final_aum) * 100, 2) if portfolio.final_aum else None,
        } for t in trades]

        # Latest paper equity-curve snapshot — same shape as the live-investment
        # dashboard's `latest_equity_curve` so the frontend reuses one component.
        latest_eq = db.query(PaperEquityCurve).filter(
            PaperEquityCurve.automate_equity_ra_id == portfolio.id,
        ).order_by(PaperEquityCurve.date.desc(), PaperEquityCurve.total_days.desc()).first()
        latest_equity_curve = None
        if latest_eq:
            latest_equity_curve = EquityCurvePointResponse(
                date=latest_eq.date,
                cash=latest_eq.cash,
                stocks_value=latest_eq.stocks_value,
                aum=latest_eq.aum,
                strategy_roc=latest_eq.strategy_roc,
                strategy_daily_return=latest_eq.strategy_daily_return,
                strategy_daily_performance=latest_eq.strategy_daily_performance,
                equitycurve_percent=latest_eq.equitycurve_percent,
                max_dd_percent=latest_eq.max_dd_percent,
                total_pnl=latest_eq.total_pnl,
                sharpe=latest_eq.sharpe,
                cagr_percent=latest_eq.cagr_percent,
                total_trades=latest_eq.total_trades,
                winning_trades=latest_eq.winning_trades,
                losing_trades=latest_eq.losing_trades,
                winning_percent=latest_eq.winning_percent,
                losing_percent=latest_eq.losing_percent,
                avg_win=latest_eq.avg_win,
                avg_loss=latest_eq.avg_loss,
                total_charges=latest_eq.total_charges,
                monthly_return=latest_eq.monthly_return,
            )

        return {
            "run_name": portfolio.strategy_name,
            "strategy": strategy_block,
            "status": portfolio.status,
            "initial_capital": float(portfolio.initial_aum),
            "current_value": float(portfolio.final_aum),
            "start_date": str(portfolio.start_date),
            "backfill_end_date": str(portfolio.backfill_end_date) if portfolio.backfill_end_date else None,
            "last_updated_date": str(portfolio.last_updated_date) if portfolio.last_updated_date else None,
            "metrics": detail["metrics"],
            "benchmark_metrics": detail["benchmark_metrics"],
            "equity_curve": detail["equity_curve"],
            "benchmark_curve": detail["benchmark_curve"],
            "benchmark_label": _get_benchmark_index_name(portfolio),
            "monthly_returns": detail["monthly_returns"],
            "drawdown_history": detail["drawdown_history"],
            "current_portfolio": current_portfolio,
            "latest_equity_curve": latest_equity_curve,
        }

    @staticmethod
    def _backfill_equitycurve(
        db: Session,
        portfolio: PaperPortfolio,
        run: BacktestRun,
        summary: BacktestSummary,
        ledger: dict,
        eob_sell_cost: float,
    ) -> int:
        """Convert daily_nav_json blob -> row-per-day equity curve rows.

        ledger: from _backfill_tradelog — per-date charges (same amounts as the
            tradelog rows -> running total_charges, so the first
            update_equitycurve() after handover sees no "new" charges), net cash
            flows (-> exact cash / stocks split per row) and closed-trade P&L
            (-> trade statistics, same formulas as update_equitycurve).
        eob_sell_cost: the backtest's hypothetical final liquidation cost. Paper
            keeps holding those stocks, so it is added back on the last row.
        """
        initial_capital = float(run.initial_capital)
        index_map = _index_close_map()

        # Benchmark prices must be REAL index closes (not the backtest's base-100
        # NAV) so that update_equitycurve's benchmark_roc math — which compares
        # today's price against the FIRST row — stays consistent after handover.
        uj = portfolio.universe_json or {}
        if uj.get("type") == "index" and uj.get("value") and uj["value"] != "NIFTY 50":
            try:
                from app.core.benchmark_registry import resolve_benchmark_table
                benchmark_map = _index_close_map(resolve_benchmark_table(uj["value"]))
            except Exception:
                benchmark_map = {}
        else:
            benchmark_map = index_map  # ALL universe → NIFTY 50 benchmark
        rows = []
        prev = None  # previous row dict
        running_max_dd = 0.0
        max_dd_absolute = 0.0
        peak_aum = initial_capital
        # Running cagr_percent/sharpe so the card (which reads the latest row) has
        # real values immediately after backfill — same formula update_equitycurve
        # uses for live rows, so there is no discontinuity at handover.
        first_date = pd.to_datetime(summary.daily_nav_json[0]["trade_date"]).date()
        nav_series: list[float] = []
        # Backtest trade ledger, applied on/before each date (a flow dated on a
        # non-NAV day lands on the next row; anything left lands on the last row)
        pending = sorted(set(ledger["charges"]) | set(ledger["cash"]) | set(ledger["closed"]))
        charges_total = cash_flow_total = 0.0
        closed_pnl: list[float] = []
        stats = trade_stats(pd.Series(closed_pnl, dtype="float64"))
        rebalance_days = {
            pd.to_datetime(ev["rebalance_date"]).date() for ev in (summary.rebalance_events_json or [])
            if ev.get("rebalance_date")
        }
        last_aum_in: dict = {"W-SUN": {}, "M": {}, "Q": {}, "Y": {}}
        first_index = first_bench = None
        last_i = len(summary.daily_nav_json) - 1
        for i, e in enumerate(summary.daily_nav_json):
            d = pd.to_datetime(e["trade_date"]).date()
            nav = float(e.get("portfolio_nav_net") or 0.0)
            if i == last_i and eob_sell_cost:
                nav += eob_sell_cost / initial_capital * 100.0
            aum = round(initial_capital * nav / 100.0, 2)
            nav_series.append(nav)
            if i == last_i and eob_sell_cost:
                # Same peak convention as the engine's post-liquidation recompute
                dd = round((nav / max(nav_series) - 1.0) * 100.0, 2)
            else:
                dd = round(float(e.get("drawdown") or 0.0) * 100.0, 2)
            if dd < running_max_dd:
                running_max_dd = dd
                max_dd_absolute = round(aum - peak_aum, 2)
            peak_aum = max(peak_aum, aum)

            closed_changed = False
            while pending and (pending[0] <= d or i == last_i):
                k = pending.pop(0)
                charges_total += ledger["charges"].get(k, 0.0)
                cash_flow_total += ledger["cash"].get(k, 0.0)
                if k in ledger["closed"]:
                    closed_pnl.extend(ledger["closed"][k])
                    closed_changed = True
            if closed_changed:
                stats = trade_stats(pd.Series(closed_pnl, dtype="float64"))
            realised = round(sum(closed_pnl), 2)
            cash = round(initial_capital + cash_flow_total, 2)
            total_pnl = round(aum - initial_capital, 2)

            total_calender_day = (d - first_date).days + 1
            # Base 100 (= initial capital); None under 1 year.
            cagr = annualised_return(nav / 100.0, total_calender_day)
            cagr_percent = round(cagr * 100, 2) if cagr is not None else None
            _std = float(np.std(np.array(nav_series, dtype=float)))
            sharpe = round((nav - 100.0) / _std, 2) if _std != 0 else 0.0

            index_price = float(index_map.get(d, 0.0))
            bench_price = float(benchmark_map.get(d, 0.0))
            first_index = first_index or index_price or None
            first_bench = first_bench or bench_price or None
            p_aum = prev["aum"] if prev else None
            p_idx = prev["index_price"] if prev else 0.0
            p_bm = prev["benchmark_price"] if prev else 0.0
            strategy_perf = round((aum - p_aum) / p_aum * 100.0, 2) if p_aum else 0.0
            index_perf = round((index_price - p_idx) / p_idx * 100.0, 2) if p_idx and index_price else 0.0

            # Same period-return rule as update_equitycurve: vs the last AUM of
            # the previous week/month/quarter/year, else vs the first row.
            ts = pd.Timestamp(d)
            period_ret = {}
            for freq in last_aum_in:
                ref = last_aum_in[freq].get(ts.to_period(freq) - 1, rows[0]["aum"] if rows else aum)
                period_ret[freq] = round((aum - ref) / ref * 100.0, 2) if ref else 0.0

            row = {
                "id": uuid.uuid4(),
                "automate_equity_ra_id": portfolio.id,
                "date": d,
                "total_days": i + 1,
                "portfolio_size": run.portfolio_size,
                "stocks_value": round(aum - cash, 2),
                "cash": cash,
                "aum": aum,
                "index_price": index_price,
                "strategy_roc": round(nav - 100.0, 2),
                "index_roc": round((index_price - first_index) / first_index * 100.0, 2) if first_index and index_price else 0.0,
                "strategy_daily_return": round(aum - p_aum, 2) if p_aum is not None else 0.0,
                "index_daily_return": round(index_price - p_idx, 2) if p_idx and index_price else 0.0,
                "strategy_daily_performance": strategy_perf,
                "index_daily_performance": index_perf,
                # total_pnl = unrealised + realised - charges (update_equitycurve identity)
                "unrealised_pnl": round(total_pnl - realised + charges_total, 2),
                "realised_pnl": realised,
                "total_pnl": total_pnl,
                **stats,
                "avg_profit_per_day": round(total_pnl / (i + 1), 2),
                "max_dd_percent": round(running_max_dd, 2),
                "max_dd_absolute": max_dd_absolute,
                "current_dd_percent": dd,
                "sqn": 0.0, "k_multiple": 0.0, "sharpe": sharpe,
                "calmar": (round(cagr_percent / abs(running_max_dd), 2) if running_max_dd != 0 else 0.0) if cagr_percent is not None else None,
                "sortino_ratio": 0.0,
                "equitycurve_percent": round(nav, 2),
                "cagr_percent": cagr_percent,
                "neg_2sd": 0.0, "equitycurve_avg": 0.0, "pos_2sd": 0.0,
                "total_charges": round(charges_total, 2),
                "rebalance": d in rebalance_days,
                "weekly_return": period_ret["W-SUN"], "monthly_return": period_ret["M"],
                "quarterly_return": period_ret["Q"], "yearly_return": period_ret["Y"],
                "benchmark_price": bench_price,
                "benchmark_roc": round((bench_price - first_bench) / first_bench * 100.0, 2) if first_bench and bench_price else 0.0,
                "benchmark_daily_return": round(bench_price - p_bm, 2) if p_bm and bench_price else 0.0,
                "benchmark_daily_performance": round((bench_price - p_bm) / p_bm * 100.0, 2) if p_bm and bench_price else 0.0,
            }
            rows.append(row)
            for freq in last_aum_in:
                last_aum_in[freq][ts.to_period(freq)] = aum
            prev = row
        if rows:
            db.bulk_insert_mappings(PaperEquityCurve, rows)
        return len(rows)

    @staticmethod
    def _backfill_tradelog(
        db: Session, portfolio: PaperPortfolio, run: BacktestRun, summary: BacktestSummary, cost_rate: float,
    ) -> tuple[int, float, dict]:
        """Convert backtest_holding_period rows -> tradelog rows.

        END_OF_BACKTEST rows become OPEN positions (active=True) — the paper
        portfolio continues holding them. Same convention as live rows:
        realised/unrealised P&L are gross, the backtest's costs (cost_rate per
        side, = cost_drag) go in buy_charges/sell_charges. Open rows carry only
        their buy charge — the backtest's hypothetical final sell never happens.
        hold = trading days held incl. buy (and sell) day — the same count the
        daily job keeps adding to; weightage = % of AUM at purchase.

        Returns (row_count, open_value_sum, ledger) where ledger holds per-date
        "charges", "cash" (net cash flow) and "closed" (gross P&L of trades
        closed that day) for the equity-curve backfill.
        """
        hps = db.query(BacktestHoldingPeriod).filter(
            BacktestHoldingPeriod.backtest_run_id == run.id,
        ).order_by(BacktestHoldingPeriod.entry_date.asc()).all()

        nav_dates = [pd.to_datetime(e["trade_date"]).date() for e in summary.daily_nav_json]
        nav_by_date = {d: float(e.get("portfolio_nav_net") or 0.0) for d, e in zip(nav_dates, summary.daily_nav_json)}
        initial_capital = float(run.initial_capital)

        def trading_days(start: date, end: date) -> int:
            return max(1, bisect_right(nav_dates, end) - bisect_left(nav_dates, start))

        def weight_at_buy(d: date, amount: float) -> Optional[float]:
            aum = initial_capital * nav_by_date.get(d, 0.0) / 100.0
            return round(amount / aum * 100.0, 2) if aum else None

        def add(kind: str, d: date, v):
            if kind == "closed":
                ledger["closed"].setdefault(d, []).append(v)
            else:
                ledger[kind][d] = ledger[kind].get(d, 0.0) + v

        rows = []
        open_value = 0.0
        ledger: dict = {"charges": {}, "cash": {}, "closed": {}}
        for hp in hps:
            qty = int(hp.qty or 0)
            entry_price = float(hp.entry_price or 0.0)
            exit_price = float(hp.exit_price or 0.0)
            buy_amount = round(qty * entry_price, 2)
            buy_charges = round(buy_amount * cost_rate, 2)
            add("charges", hp.entry_date, buy_charges)
            add("cash", hp.entry_date, -(buy_amount + buy_charges))
            is_open = (hp.exit_reason == "END_OF_BACKTEST")
            if is_open:
                current_value = round(qty * exit_price, 2)  # exit_price = last close
                open_value += current_value
                rows.append({
                    "id": uuid.uuid4(),
                    "automate_equity_ra_id": portfolio.id,
                    "tradingsymbol": hp.symbol, "isin": "",
                    "buy_date": hp.entry_date, "sell_date": None,
                    "hold": trading_days(hp.entry_date, nav_dates[-1]),
                    "weightage": weight_at_buy(hp.entry_date, buy_amount),
                    "buy_qty": qty, "buy_price": entry_price, "buy_amount": buy_amount,
                    "sell_qty": 0, "sell_price": 0.0, "sell_amount": 0.0,
                    "pyramiding": 1, "volatility": 0.0,
                    "ltp": exit_price, "stoploss": 0.0, "risk": 0.0, "risk_percent": 0.0,
                    "current_value": current_value,
                    "unrealised_pnl": round(current_value - buy_amount, 2),
                    "realised_pnl": 0.0,
                    "profit_percent": round((current_value - buy_amount) / buy_amount * 100.0, 2) if buy_amount else 0.0,
                    "buy_charges": buy_charges, "sell_charges": 0.0,
                    "active": True,
                    "buy_order_id": "BACKTEST", "sell_order_id": None,
                    "pyramiding_data": f"{hp.entry_date},{qty},{entry_price};",
                    "profit_booking_data": "",
                })
            else:
                sell_amount = round(qty * exit_price, 2)
                sell_charges = round(sell_amount * cost_rate, 2)
                realised = round(sell_amount - buy_amount, 2)  # gross; net = realised - charges = hp.pnl_abs
                add("charges", hp.exit_date, sell_charges)
                add("cash", hp.exit_date, sell_amount - sell_charges)
                add("closed", hp.exit_date, realised)
                rows.append({
                    "id": uuid.uuid4(),
                    "automate_equity_ra_id": portfolio.id,
                    "tradingsymbol": hp.symbol, "isin": "",
                    "buy_date": hp.entry_date, "sell_date": hp.exit_date,
                    "hold": trading_days(hp.entry_date, hp.exit_date),
                    "weightage": weight_at_buy(hp.entry_date, buy_amount),
                    "buy_qty": qty, "buy_price": entry_price, "buy_amount": buy_amount,
                    "sell_qty": qty, "sell_price": exit_price, "sell_amount": sell_amount,
                    "pyramiding": 1, "volatility": 0.0,
                    "ltp": exit_price, "stoploss": 0.0, "risk": 0.0, "risk_percent": 0.0,
                    "current_value": 0.0, "unrealised_pnl": 0.0,
                    "realised_pnl": realised,
                    "profit_percent": round(realised / buy_amount * 100.0, 2) if buy_amount else 0.0,
                    "buy_charges": buy_charges, "sell_charges": sell_charges,
                    "active": False,
                    "buy_order_id": "BACKTEST", "sell_order_id": "BACKTEST",
                    "pyramiding_data": f"{hp.entry_date},{qty},{entry_price};",
                    "profit_booking_data": "",
                })
        if rows:
            db.bulk_insert_mappings(PaperTradelog, rows)
        return len(rows), round(open_value, 2), ledger

    # ── Daily update ──────────────────────────────────────────────────────

    @staticmethod
    def run_daily_update(db: Session, TODAY: Optional[date] = None) -> int:
        """Daily paper update for all ACTIVE portfolios, up to and including TODAY.

        Catches up EVERY missed trading day (last_updated_date+1 .. TODAY) in
        order, each with that day's own screener signal and prices — so a missed
        cron run or a late start never skips a day or a rebalance. A portfolio
        stops at the first day whose EOD market data is not loaded yet (or that
        fails); it is retried on the next run, never skipped.
        Returns the number of portfolios that got at least one new day.
        """
        TODAY = TODAY or date.today()
        portfolios = db.query(PaperPortfolio).filter(PaperPortfolio.status == "ACTIVE").all()
        data_ready: dict = {}
        count = sum(1 for p in portfolios if PlatformPaperService.catch_up(db, p, TODAY, data_ready))
        logger.info("[Paper] Updated %d paper portfolios up to %s", count, TODAY)
        return count

    @staticmethod
    def catch_up(db: Session, portfolio: PaperPortfolio, TODAY: date, data_ready: Optional[dict] = None) -> bool:
        """Process one portfolio's missed trading days up to TODAY, in order,
        committing each day. Returns True if at least one day was added."""
        data_ready = {} if data_ready is None else data_ready
        pid = portfolio.id
        d = (portfolio.last_updated_date or portfolio.backfill_end_date or TODAY) + timedelta(days=1)
        updated = False
        while d <= TODAY:
            if is_trading_day(d):
                if d not in data_ready:
                    data_ready[d] = _market_data_ready(d)
                if not data_ready[d]:
                    if _index_has_data_after(d):
                        # Later days have data, this one never will: an exchange
                        # holiday missing from holidays.json.
                        logger.warning("[Paper] %s has no market data but later days do — treated as a holiday "
                                       "(add it to holidays.json)", d)
                        d += timedelta(days=1)
                        continue
                    logger.warning("[Paper] EOD market data for %s not loaded yet — portfolio %s waits (retried next run)", d, pid)
                    break
                try:
                    if PlatformPaperService._update_one(db, portfolio, d):
                        updated = True
                    db.commit()
                except _DataNotReady as exc:
                    db.rollback()
                    logger.warning("[Paper] Portfolio %s, %s: %s — waits (retried next run)", pid, d, exc)
                    break
                except Exception:
                    logger.exception("[Paper] Error updating portfolio %s for %s — later days wait for it", pid, d)
                    db.rollback()
                    break
            d += timedelta(days=1)
        return updated

    @staticmethod
    def _update_one(db: Session, portfolio: PaperPortfolio, TODAY: date) -> bool:
        # Idempotency: one curve row per date
        if portfolio.last_updated_date and portfolio.last_updated_date >= TODAY:
            logger.info("[Paper] Portfolio %s already updated for %s — skipping", portfolio.id, TODAY)
            return False

        # Every holding must have TODAY's OHLC before the day is booked (the index
        # row alone does not prove the stock tables finished loading).
        held = [s for (s,) in db.query(PaperTradelog.tradingsymbol).filter(
            PaperTradelog.automate_equity_ra_id == portfolio.id, PaperTradelog.active == True,  # noqa: E712
        ).distinct()]
        _require_day_prices(_prices_on(held, TODAY), held, TODAY)

        cost_rate = PlatformPaperService._portfolio_cost_rate(db, portfolio)
        rebalance_due = bool(portfolio.next_rebalance_date and TODAY >= portfolio.next_rebalance_date)
        if rebalance_due:
            PlatformPaperService._backtest_rebalance(db, portfolio, TODAY, cost_rate)

        # ── Same processing as live _process_strategy_daily_update ─────────
        buy_df = model_df(db, PaperBuyStock, portfolio.id)
        sell_df = model_df(db, PaperSellStock, portfolio.id)
        tradelog_df = model_df(db, PaperTradelog, portfolio.id)
        equitycurve_df = model_df(db, PaperEquityCurve, portfolio.id)

        # ── Corporate Actions: symbol rename, ISIN sync, bonus/split ───────
        # Same shared function and same position in the flow as the live 16:30
        # job — must run BEFORE fill processing / LTP refresh.
        from app.services.corporate_action_service import apply_corporate_actions_to_strategy
        tradelog_df = apply_corporate_actions_to_strategy(db, portfolio, tradelog_df, TODAY)

        has_pending_fills = False
        if not buy_df.empty and not buy_df.loc[(buy_df["updated_in_tradelog"].fillna(False) == False) & (buy_df["actual_qty"].fillna(0) > 0)].empty:
            has_pending_fills = True
        if not sell_df.empty and not sell_df.loc[(sell_df["updated_in_tradelog"].fillna(False) == False) & (sell_df["actual_qty"].fillna(0) > 0)].empty:
            has_pending_fills = True

        # Fills carry the backtest's per-side cost; MTM at TODAY's close.
        tradelog_df, today_cash = update_tradelog(
            TODAY, f"(paper:{portfolio.id})", tradelog_df, buy_df, sell_df, make_circuit_table(), 0.0,
            buy_charge_rate=cost_rate, sell_charge_rate=cost_rate,
        )
        upsert_df_to_db(db, tradelog_df, PaperTradelog, commit=False)
        if has_pending_fills:
            update_df_to_db(db, buy_df, PaperBuyStock, commit=False)
            update_df_to_db(db, sell_df, PaperSellStock, commit=False)

        if tradelog_df.empty or equitycurve_df.empty:
            logger.info("[Paper] Portfolio %s has no tradelog/curve rows — skipping curve append", portfolio.id)
            return False

        # rebalance flag = scheduled rebalance day (as the backtest marks it),
        # even when the ranking needed no trades
        equitycurve_df = update_equitycurve(TODAY, portfolio, tradelog_df, equitycurve_df, today_cash, rebalance_due)
        insert_single_row_to_db(db, equitycurve_df.iloc[-1].to_dict(), PaperEquityCurve, commit=False)

        last = equitycurve_df.iloc[-1]
        portfolio.cash = float(last["cash"] or 0)
        portfolio.stock_value = float(last["stocks_value"] or 0)
        portfolio.final_aum = float(last["aum"] or 0)
        portfolio.pnl = float(last["total_pnl"] or 0)
        portfolio.todays_pnl = float(last["strategy_daily_return"] or 0)
        portfolio.last_updated_date = TODAY

        if rebalance_due:
            portfolio.next_rebalance_date = _next_rebalance_after(TODAY, portfolio.rebalance_frequency)

        # Overwrite the just-written row's headline ratios with the institutional
        # values (same as the detail page) so the card and detail always agree.
        db.flush()
        PlatformPaperService._store_institutional_on_latest_row(db, portfolio)
        return True

    @staticmethod
    def _portfolio_cost_rate(db: Session, portfolio: PaperPortfolio) -> float:
        """Per-side cost of the portfolio's source backtest (default 0.30%)."""
        run = db.query(BacktestRun).filter(BacktestRun.id == portfolio.backtest_run_id).first() \
            if portfolio.backtest_run_id else None
        return _cost_rate(run) if run else PAPER_DEFAULT_COST_RATE

    @staticmethod
    def _backtest_rebalance(db: Session, portfolio: PaperPortfolio, TODAY: date, cost_rate: float) -> None:
        """Rebalance with the SAME rules as backtest_engine, so paper is the
        backtest continued forward day by day:
          - signal: the screener CSV strictly BEFORE TODAY (no look-ahead)
          - selection: keep holdings ranked <= WRH, fill up to portfolio_size
            from the top of the ranking
          - fills at TODAY's open (sells fall back to close, then last price)
          - cost: the source backtest's per-side rate (charged in update_tradelog)
          - cash: buys share (cash + 80% of net sell proceeds) x 0.98 equally
        update_tradelog_ltp then marks the new positions at TODAY's close, so
        the rebalance day's open->close move is real P&L, as in the backtest.
        """
        pid = portfolio.id
        ps, wrh = int(portfolio.portfolio_size), int(portfolio.worst_hold_rank)

        # Idempotent on retry: drop this day's fills that never reached the tradelog
        for model in (PaperBuyStock, PaperSellStock):
            db.query(model).filter(
                model.automate_equity_ra_id == pid, model.date == TODAY,
                model.updated_in_tradelog.isnot(True),
            ).delete(synchronize_session=False)
        db.commit()

        signal_dates = [d for d in csv_data_service.get_available_screener_dates() if d < TODAY]
        if not signal_dates:
            logger.error("[Paper] No screener CSV before %s — rebalance skipped for %s", TODAY, pid)
            return
        signal_date = max(signal_dates)

        version = db.query(ScreenerVersion).filter(ScreenerVersion.id == portfolio.screener_version_id).first() \
            if portfolio.screener_version_id else None
        results, _, _ = screener_execution_service._execute_with_params(
            universe_json=(version.universe_json if version else portfolio.universe_json) or {},
            filters_json=(version.filters_json if version else portfolio.filters_json) or [],
            ranking_json=(version.ranking_json if version else portfolio.ranking_json) or {},
            limit=max(ps, wrh), offset=0, target_date=signal_date, universe_date=TODAY,
        )
        ranked, isin_of = [], {}
        for r in results:
            sym = resolve_stock_symbol(r["symbol"])
            if sym not in isin_of:
                ranked.append(sym)
                isin_of[sym] = (r.get("indicators") or {}).get("isin") or ""
        if not ranked:
            # The engine would liquidate everything here; a missing/empty
            # screener file is far likelier than a real "no stock qualifies".
            logger.error("[Paper] Screener returned no stocks for %s (signal %s) — holdings kept, rebalance skipped for %s",
                         TODAY, signal_date, pid)
            return
        rank_map = {s: i + 1 for i, s in enumerate(ranked)}

        tradelog_df = model_df(db, PaperTradelog, pid)
        held: dict = {}
        if not tradelog_df.empty:
            for _, row in tradelog_df.loc[tradelog_df["active"] == True].iterrows():
                held[resolve_stock_symbol(row["tradingsymbol"])] = row

        retained = [s for s in held if rank_map.get(s, 10**9) <= wrh]
        new_basket = list(retained)
        for s in ranked[:ps]:
            if len(new_basket) >= ps:
                break
            if s not in new_basket:
                new_basket.append(s)
        sold = sorted(set(held) - set(new_basket))
        to_buy = [s for s in new_basket if s not in held]

        traded = sorted(set(sold) | set(to_buy))
        prices = _prices_on(traded, TODAY)
        _require_day_prices(prices, traded, TODAY)

        cash_before_sells = float(portfolio.cash or 0.0)
        net_cash = cash_before_sells
        stock_sell_amount = 0.0
        sell_rows = []
        def num(v) -> float:
            return float(v) if v is not None and pd.notna(v) else 0.0

        for s in sold:
            row = held[s]
            qty = int(num(row["buy_qty"])) - int(num(row["sell_qty"]))
            if qty <= 0:
                continue
            o, c, _ = prices.get(s, (0.0, 0.0, None))
            sell_px = o or c or num(row["ltp"])
            if sell_px <= 0:
                continue
            gross = qty * sell_px
            net_cash += gross * (1 - cost_rate)
            stock_sell_amount += gross * (1 - cost_rate)
            sell_rows.append({
                "automate_equity_ra_id": pid, "tradingsymbol": row["tradingsymbol"],
                "isin": row.get("isin") or "", "date": TODAY,
                "qty": qty, "price": sell_px, "amount": round(gross, 2),
                "actual_qty": qty, "actual_price": sell_px, "actual_amount": round(gross, 2),
                "order_id": _paper_order_id(), "method": "WRH", "circuit": False, "updated_in_tradelog": False,
            })

        open_slots = max(0, ps - (len(held) - len(sell_rows)))
        candidates = to_buy[:open_slots]
        aum = float(portfolio.final_aum or portfolio.initial_aum or 0.0)
        buy_rows = []
        if candidates:
            # Broker releases only 80% of sell proceeds on the same day (engine rule)
            deployable = max(0.0, (cash_before_sells + stock_sell_amount * 0.80) * 0.98)
            per_stock = deployable / len(candidates)
            for s in candidates:
                buy_px = prices.get(s, (0.0, 0.0, None))[0]
                if buy_px <= 0:
                    continue
                qty = int(per_stock / buy_px) if per_stock > 0 else 0
                if qty <= 0:
                    continue
                needed = qty * buy_px * (1 + cost_rate)
                if needed > net_cash:
                    qty = max(0, int(net_cash / (buy_px * (1 + cost_rate))))
                    if qty <= 0:
                        continue
                    needed = qty * buy_px * (1 + cost_rate)
                if needed > net_cash:
                    continue
                net_cash -= needed
                amount = round(qty * buy_px, 2)
                buy_rows.append({
                    "automate_equity_ra_id": pid, "tradingsymbol": s, "isin": isin_of.get(s, ""), "date": TODAY,
                    "qty": qty, "price": buy_px, "amount": amount,
                    "weightage": round(amount / aum * 100, 2) if aum else 0.0,
                    "actual_qty": qty, "actual_price": buy_px, "actual_amount": amount,
                    "stoploss": 0.0, "volatility": 0.0,
                    "order_id": _paper_order_id(), "circuit": False, "updated_in_tradelog": False,
                })

        insert_df_to_db(db, pd.DataFrame(sell_rows, columns=make_sell_table().columns), PaperSellStock)
        insert_df_to_db(db, pd.DataFrame(buy_rows, columns=make_buy_table().columns), PaperBuyStock)
        logger.info("[Paper] Rebalance %s on %s (signal %s): %d sells, %d buys at open, cost %.2f%%/side",
                    pid, TODAY, signal_date, len(sell_rows), len(buy_rows), cost_rate * 100)


def _prices_on(symbols: list, TODAY: date) -> dict:
    """{symbol: (open, close, row_date)} — the latest OHLC row on or before
    TODAY (30-day lookback), same source and forward-fill rule as the engine."""
    if not symbols:
        return {}
    eq_db = EquitySessionLocal()
    try:
        df = equity_data_service.get_multi_stock_ohlc(symbols, TODAY - timedelta(days=30), TODAY, eq_db)
    finally:
        eq_db.close()
    out = {}
    if df.empty:
        return out
    for sym, g in df.sort_values("date").groupby("symbol"):
        opens, closes = g["open"].dropna(), g["close"].dropna()
        out[sym] = (
            float(opens.iloc[-1]) if len(opens) else 0.0,
            float(closes.iloc[-1]) if len(closes) else 0.0,
            g["date"].iloc[-1],
        )
    return out


class _DataNotReady(Exception):
    """The day's EOD data is not fully loaded yet — the day is retried next run."""


def _require_day_prices(prices: dict, symbols: list, d: date) -> None:
    """Every symbol must have an OHLC row dated `d`. While no later day has
    index data, a gap means the import is still running -> _DataNotReady. Once
    later data exists the gap is permanent (suspension, no table), so continue
    with the last available price — the backtest's forward-fill rule."""
    lacking = sorted(s for s in symbols if s not in prices or prices[s][2] != d)
    if not lacking:
        return
    if not _index_has_data_after(d):
        raise _DataNotReady(f"no {d} OHLC yet for {lacking}")
    logger.warning("[Paper] %s: still no OHLC for %s although later days are loaded — using the last available "
                   "price, as the backtest does", d, lacking)


def _index_has_data_after(d: date) -> bool:
    """True if NIFTY 50 already has an EOD row for a date after `d`."""
    try:
        with equity_engine.connect() as conn:
            row = conn.execute(
                text(f'SELECT 1 FROM public."NIFTY 50" WHERE "{settings.EQUITY_TABLE_DATE_COL}" > :d LIMIT 1'),
                {"d": d},
            ).first()
        return row is not None
    except Exception:
        logger.exception("[Paper] Could not check market data after %s", d)
        return False


def _trading_days_after(start: date, end: date) -> int:
    """Number of trading days d with start < d <= end."""
    n, d = 0, start + timedelta(days=1)
    while d <= end:
        n += 1 if is_trading_day(d) else 0
        d += timedelta(days=1)
    return n


def _market_data_ready(d: date) -> bool:
    """True once the day's NIFTY 50 EOD row is in equity_ohlc — the same table
    update_equitycurve prices the index from. Paper never books a day on
    yesterday's prices."""
    try:
        with equity_engine.connect() as conn:
            row = conn.execute(
                text(f'SELECT 1 FROM public."NIFTY 50" WHERE "{settings.EQUITY_TABLE_DATE_COL}" = :d LIMIT 1'),
                {"d": d},
            ).first()
        return row is not None
    except Exception:
        logger.exception("[Paper] Could not check market data for %s", d)
        return False


platform_paper_service = PlatformPaperService()
