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
  2. run_daily_update(): daily job (after market close).
     - On rebalance-due days: generate synthetic fills (buy/sell rows with
       actual_qty/actual_price = close, order_id = "PAPER-...") using the SAME
       selection functions as live (get_sell_df_investment/get_buy_df_investment).
     - Every trading day: run the SAME update_tradelog() + update_equitycurve()
       functions as the live 16:30 job. No broker, no approval, no postback.

This module only IMPORTS from live_investment_service — it never modifies it.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.core.database import equity_engine
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
from app.core.performance_metrics import compute_summary_from_nav
# Shared equitycase-style machinery — imported, NEVER modified.
from app.services.live_investment_service import (
    _get_benchmark_index_name,
    _safe_latest_symbol_price,
    get_buy_df_investment,
    get_sell_df_investment,
    get_strategy_builder_screener_df,
    insert_df_to_db,
    insert_single_row_to_db,
    make_circuit_table,
    model_df,
    update_df_to_db,
    update_equitycurve,
    update_tradelog,
    upsert_df_to_db,
)

logger = logging.getLogger("paper_trading")


def _paper_order_id() -> str:
    return f"PAPER-{uuid.uuid4().hex[:8]}"


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
            next_rebalance_date=next_trading_day(next_rebalance_prepare_date(run.to_date + timedelta(days=1), run.rebalance_frequency)),
            filters_json=version.filters_json if version else None,
            universe_json=version.universe_json if version else None,
            ranking_json=version.ranking_json if version else None,
        )
        db.add(portfolio)
        db.flush()  # portfolio.id needed for child rows

        n_curve = PlatformPaperService._backfill_equitycurve(db, portfolio, run, summary)
        n_trades, open_value = PlatformPaperService._backfill_tradelog(db, portfolio, run)

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
            portfolio.last_updated_date = last_curve.date

        # Store institutional headline ratios on the latest row so the card
        # matches the detail page from the very first (backfill-only) render.
        db.flush()
        PlatformPaperService._store_institutional_on_latest_row(db, portfolio)

        db.commit()
        db.refresh(portfolio)
        logger.info("[Paper] Started portfolio %s for screener %s — %d curve rows, %d trades backfilled",
                    portfolio.id, screener_id, n_curve, n_trades)
        return portfolio

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
        summary = db.query(BacktestSummary).filter(
            BacktestSummary.backtest_run_id == portfolio.backtest_run_id,
        ).first()
        backfill_end = portfolio.backfill_end_date

        # Historical era: backtest daily_nav (precise nav + stored returns)
        hist_nav, hist_ret = {}, {}
        if summary and summary.daily_nav_json:
            for e in summary.daily_nav_json:
                d = pd.Timestamp(e["trade_date"])
                hist_nav[d] = float(e["portfolio_nav_net"])
                hist_ret[d] = float(e.get("portfolio_return_net") or 0.0)

        # Live era + continuous raw benchmark from paper equity curve
        curve_rows = db.query(PaperEquityCurve).filter(
            PaperEquityCurve.automate_equity_ra_id == portfolio.id,
        ).order_by(PaperEquityCurve.date.asc(), PaperEquityCurve.total_days.asc()).all()
        live_nav, bench_raw = {}, {}
        for r in curve_rows:
            d = pd.Timestamp(r.date)
            if r.benchmark_price:
                bench_raw[d] = float(r.benchmark_price)
            if backfill_end and r.date > backfill_end:
                live_nav[d] = float(r.equitycurve_percent or 0.0)

        nav_map = dict(hist_nav)
        nav_map.update(live_nav)
        nav = pd.Series(nav_map).sort_index()
        if nav.empty:
            raise HTTPException(status_code=400, detail="No NAV data available for this strategy")

        # Returns: stored (exact) for historical dates, nav-derived for live days
        derived = nav.pct_change()
        combined_ret = pd.Series(hist_ret).reindex(nav.index)
        for d in nav.index:
            if d not in hist_ret:
                combined_ret.loc[d] = derived.loc[d]
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
    def _store_institutional_on_latest_row(db: Session, portfolio: PaperPortfolio) -> None:
        """Write the INSTITUTIONAL headline ratios (the same numbers the detail
        page shows) onto the LATEST paper equity-curve row, so the card — which
        reads that row cheaply — matches the detail exactly. One computation, one
        storage, both surfaces agree. Called after backfill and after each daily
        update. Fully defensive: never breaks the caller.
        """
        try:
            out, _, _ = PlatformPaperService._stitched_summary(db, portfolio)
            m = out["metrics"]
            latest = db.query(PaperEquityCurve).filter(
                PaperEquityCurve.automate_equity_ra_id == portfolio.id,
            ).order_by(PaperEquityCurve.date.desc(), PaperEquityCurve.total_days.desc()).first()
            if latest is None:
                return
            if m.get("cagr") is not None:
                latest.cagr_percent = round(float(m["cagr"]) * 100.0, 2)
            if m.get("sharpe") is not None:
                latest.sharpe = round(float(m["sharpe"]), 2)
            if m.get("max_drawdown") is not None:
                latest.max_dd_percent = round(float(m["max_drawdown"]) * 100.0, 2)
        except Exception:
            logger.exception("[Paper] institutional metric store failed for %s", portfolio.id)

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
            "total_return": r2(latest_row.strategy_roc),    # percent since inception
            "current_value": r2(latest_row.aum),
            "since_date": str(portfolio.start_date),
            "as_of": str(portfolio.last_updated_date) if portfolio.last_updated_date else None,
        }

    @staticmethod
    def build_detail(db: Session, screener_id) -> dict:
        """Build the user-facing detail payload for an ACTIVE paper strategy.

        Metrics/graphs use the INSTITUTIONAL (backtest) methodology recomputed
        from a stitched NAV series — backtest daily_nav (full precision) for the
        historical era + paper equity curve for the live days. The paper table's
        own live-formula metric columns are intentionally NOT read.

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

        out, nav, bench_by_date = PlatformPaperService._stitched_summary(db, portfolio)
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
            "upside_capture": x2(m["upside_capture"]),
            "downside_capture": x2(m["downside_capture"]),
        }

        dd = out["drawdown_series"]
        equity_curve = [{
            "time": d.strftime("%Y-%m-%d"),
            "value": round(float(nav.loc[d]), 2),
            "drawdown": round(float(dd.loc[d]) * 100.0, 2),
        } for d in nav.index]
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
            })
        monthly_returns = []
        for y, months in years.items():
            ytd = 1.0
            for mm in months:
                if mm["monthly_return"] is not None:
                    ytd *= (1.0 + mm["monthly_return"])
            monthly_returns.append({"year": y, "months": months, "ytd": ytd - 1.0})

        drawdown_history = sorted(out["drawdown_episodes"], key=lambda e: e["drawdown_pct"])[:5]

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

        return {
            "run_name": portfolio.strategy_name,
            "strategy": strategy_block,
            "status": portfolio.status,
            "initial_capital": float(portfolio.initial_aum),
            "current_value": float(portfolio.final_aum),
            "start_date": str(portfolio.start_date),
            "backfill_end_date": str(portfolio.backfill_end_date) if portfolio.backfill_end_date else None,
            "last_updated_date": str(portfolio.last_updated_date) if portfolio.last_updated_date else None,
            "metrics": metrics,
            "benchmark_metrics": benchmark_metrics,
            "equity_curve": equity_curve,
            "benchmark_curve": benchmark_curve,
            "benchmark_label": _get_benchmark_index_name(portfolio),
            "monthly_returns": monthly_returns,
            "drawdown_history": drawdown_history,
            "current_portfolio": current_portfolio,
        }

    @staticmethod
    def _backfill_equitycurve(db: Session, portfolio: PaperPortfolio, run: BacktestRun, summary: BacktestSummary) -> int:
        """Convert daily_nav_json blob -> row-per-day equity curve rows."""
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
        prev_aum = None
        running_max_dd = 0.0
        # Running cagr_percent/sharpe so the card (which reads the latest row) has
        # real values immediately after backfill — same formula update_equitycurve
        # uses for live rows, so there is no discontinuity at handover.
        first_date = pd.to_datetime(summary.daily_nav_json[0]["trade_date"]).date()
        base_nav = float(summary.daily_nav_json[0].get("portfolio_nav_net") or 100.0) or 100.0
        nav_series: list[float] = []
        for i, e in enumerate(summary.daily_nav_json):
            d = pd.to_datetime(e["trade_date"]).date()
            nav = float(e.get("portfolio_nav_net") or 0.0)
            aum = round(initial_capital * nav / 100.0, 2)
            dd = round(float(e.get("drawdown") or 0.0) * 100.0, 2)
            running_max_dd = min(running_max_dd, dd)

            nav_series.append(nav)
            total_calender_day = (d - first_date).days + 1
            cagr_percent = round(((((nav / base_nav) ** (1 / (total_calender_day / 365))) - 1) * 100), 2) \
                if total_calender_day > 0 and base_nav else 0.0
            _std = float(np.std(np.array(nav_series, dtype=float)))
            sharpe = round((nav - 100.0) / _std, 2) if _std != 0 else 0.0
            rows.append({
                "id": uuid.uuid4(),
                "automate_equity_ra_id": portfolio.id,
                "date": d,
                "total_days": i + 1,
                "portfolio_size": run.portfolio_size,
                "stocks_value": aum,   # cash/stock split not in daily_nav — reconciled on last row
                "cash": 0.0,
                "aum": aum,
                "index_price": float(index_map.get(d, 0.0)),
                "strategy_roc": round(nav - 100.0, 2),
                "index_roc": 0.0,
                "strategy_daily_return": round(aum - prev_aum, 2) if prev_aum is not None else 0.0,
                "index_daily_return": 0.0,
                "strategy_daily_performance": round((aum - prev_aum) / prev_aum * 100.0, 2) if prev_aum else 0.0,
                "index_daily_performance": 0.0,
                "unrealised_pnl": 0.0,
                "realised_pnl": 0.0,
                "total_pnl": round(aum - initial_capital, 2),
                "winning_trades": 0, "losing_trades": 0, "total_trades": 0,
                "winning_percent": 0.0, "losing_percent": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0, "rr": 0.0, "profit_factor": 0.0,
                "biggest_winning_trade": 0.0, "biggest_losing_trade": 0.0,
                "expectancy": 0.0, "avg_profit_per_day": 0.0,
                "max_dd_percent": round(running_max_dd, 2),
                "max_dd_absolute": round(running_max_dd * initial_capital / 100.0, 2),
                "current_dd_percent": dd,
                "sqn": 0.0, "k_multiple": 0.0, "sharpe": sharpe,
                "calmar": round(cagr_percent / abs(running_max_dd), 2) if running_max_dd != 0 else 0.0,
                "sortino_ratio": 0.0,
                "equitycurve_percent": round(nav, 2),
                "cagr_percent": cagr_percent,
                "neg_2sd": 0.0, "equitycurve_avg": 0.0, "pos_2sd": 0.0,
                "total_charges": 0.0,
                "rebalance": False,
                "weekly_return": 0.0, "monthly_return": 0.0, "quarterly_return": 0.0, "yearly_return": 0.0,
                "benchmark_price": float(benchmark_map.get(d, 0.0)),
                "benchmark_roc": 0.0, "benchmark_daily_return": 0.0, "benchmark_daily_performance": 0.0,
            })
            prev_aum = aum
        if rows:
            db.bulk_insert_mappings(PaperEquityCurve, rows)
        return len(rows)

    @staticmethod
    def _backfill_tradelog(db: Session, portfolio: PaperPortfolio, run: BacktestRun) -> tuple[int, float]:
        """Convert backtest_holding_period rows -> tradelog rows.

        END_OF_BACKTEST rows become OPEN positions (active=True) — the paper
        portfolio continues holding them. Returns (row_count, open_value_sum).
        """
        hps = db.query(BacktestHoldingPeriod).filter(
            BacktestHoldingPeriod.backtest_run_id == run.id,
        ).order_by(BacktestHoldingPeriod.entry_date.asc()).all()

        rows = []
        open_value = 0.0
        for hp in hps:
            qty = int(hp.qty or 0)
            entry_price = float(hp.entry_price or 0.0)
            exit_price = float(hp.exit_price or 0.0)
            buy_amount = round(qty * entry_price, 2)
            is_open = (hp.exit_reason == "END_OF_BACKTEST")
            if is_open:
                current_value = round(qty * exit_price, 2)  # exit_price = last close
                open_value += current_value
                rows.append({
                    "id": uuid.uuid4(),
                    "automate_equity_ra_id": portfolio.id,
                    "tradingsymbol": hp.symbol, "isin": "",
                    "buy_date": hp.entry_date, "sell_date": None,
                    "hold": int(hp.holding_days or 0),
                    "weightage": None,
                    "buy_qty": qty, "buy_price": entry_price, "buy_amount": buy_amount,
                    "sell_qty": 0, "sell_price": 0.0, "sell_amount": 0.0,
                    "pyramiding": 1, "volatility": 0.0,
                    "ltp": exit_price, "stoploss": 0.0, "risk": 0.0, "risk_percent": 0.0,
                    "current_value": current_value,
                    "unrealised_pnl": round(current_value - buy_amount, 2),
                    "realised_pnl": 0.0,
                    "profit_percent": round((current_value - buy_amount) / buy_amount * 100.0, 2) if buy_amount else 0.0,
                    "buy_charges": 0.0, "sell_charges": 0.0,
                    "active": True,
                    "buy_order_id": "BACKTEST", "sell_order_id": None,
                    "pyramiding_data": f"{hp.entry_date},{qty},{entry_price};",
                    "profit_booking_data": "",
                })
            else:
                sell_amount = round(qty * exit_price, 2)
                realised = round(float(hp.pnl_abs) if hp.pnl_abs is not None else sell_amount - buy_amount, 2)
                rows.append({
                    "id": uuid.uuid4(),
                    "automate_equity_ra_id": portfolio.id,
                    "tradingsymbol": hp.symbol, "isin": "",
                    "buy_date": hp.entry_date, "sell_date": hp.exit_date,
                    "hold": int(hp.holding_days or 0),
                    "weightage": None,
                    "buy_qty": qty, "buy_price": entry_price, "buy_amount": buy_amount,
                    "sell_qty": qty, "sell_price": exit_price, "sell_amount": sell_amount,
                    "pyramiding": 1, "volatility": 0.0,
                    "ltp": exit_price, "stoploss": 0.0, "risk": 0.0, "risk_percent": 0.0,
                    "current_value": 0.0, "unrealised_pnl": 0.0,
                    "realised_pnl": realised,
                    "profit_percent": round(realised / buy_amount * 100.0, 2) if buy_amount else 0.0,
                    "buy_charges": 0.0, "sell_charges": 0.0,
                    "active": False,
                    "buy_order_id": "BACKTEST", "sell_order_id": "BACKTEST",
                    "pyramiding_data": f"{hp.entry_date},{qty},{entry_price};",
                    "profit_booking_data": "",
                })
        if rows:
            db.bulk_insert_mappings(PaperTradelog, rows)
        return len(rows), round(open_value, 2)

    # ── Daily update ──────────────────────────────────────────────────────

    @staticmethod
    def run_daily_update(db: Session, TODAY: Optional[date] = None) -> int:
        """Daily paper update for all ACTIVE portfolios. Same order as live:
        synthetic rebalance fills first (if due), then tradelog + equity curve."""
        TODAY = TODAY or date.today()
        if not is_trading_day(TODAY):
            logger.info("[Paper] %s is a non-trading day — skipping", TODAY)
            return 0

        portfolios = db.query(PaperPortfolio).filter(PaperPortfolio.status == "ACTIVE").all()
        count = 0
        for portfolio in portfolios:
            try:
                if PlatformPaperService._update_one(db, portfolio, TODAY):
                    count += 1
                db.commit()
            except Exception:
                logger.exception("[Paper] Error updating portfolio %s", portfolio.id)
                db.rollback()
        logger.info("[Paper] Updated %d paper portfolios for %s", count, TODAY)
        return count

    @staticmethod
    def _update_one(db: Session, portfolio: PaperPortfolio, TODAY: date) -> bool:
        # Idempotency: one curve row per date
        if portfolio.last_updated_date and portfolio.last_updated_date >= TODAY:
            logger.info("[Paper] Portfolio %s already updated for %s — skipping", portfolio.id, TODAY)
            return False

        rebalance_due = bool(portfolio.next_rebalance_date and TODAY >= portfolio.next_rebalance_date)
        if rebalance_due:
            PlatformPaperService._synthetic_rebalance(db, portfolio, TODAY)

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

        tradelog_df, today_cash = update_tradelog(
            TODAY, f"(paper:{portfolio.id})", tradelog_df, buy_df, sell_df, make_circuit_table(), 0.0
        )
        upsert_df_to_db(db, tradelog_df, PaperTradelog, commit=False)
        if has_pending_fills:
            update_df_to_db(db, buy_df, PaperBuyStock, commit=False)
            update_df_to_db(db, sell_df, PaperSellStock, commit=False)

        if tradelog_df.empty or equitycurve_df.empty:
            logger.info("[Paper] Portfolio %s has no tradelog/curve rows — skipping curve append", portfolio.id)
            return False

        equitycurve_df = update_equitycurve(TODAY, portfolio, tradelog_df, equitycurve_df, today_cash, has_pending_fills)
        insert_single_row_to_db(db, equitycurve_df.iloc[-1].to_dict(), PaperEquityCurve, commit=False)

        last = equitycurve_df.iloc[-1]
        portfolio.cash = float(last["cash"] or 0)
        portfolio.stock_value = float(last["stocks_value"] or 0)
        portfolio.final_aum = float(last["aum"] or 0)
        portfolio.pnl = float(last["total_pnl"] or 0)
        portfolio.todays_pnl = float(last["strategy_daily_return"] or 0)
        portfolio.last_updated_date = TODAY

        if rebalance_due:
            portfolio.next_rebalance_date = next_trading_day(
                next_rebalance_prepare_date(TODAY + timedelta(days=1), portfolio.rebalance_frequency)
            )

        # Overwrite the just-written row's headline ratios with the institutional
        # values (same as the detail page) so the card and detail always agree.
        db.flush()
        PlatformPaperService._store_institutional_on_latest_row(db, portfolio)
        return True

    @staticmethod
    def _synthetic_rebalance(db: Session, portfolio: PaperPortfolio, TODAY: date) -> None:
        """Generate buy/sell rows with synthetic fills at close prices.
        Same selection logic as live prepare_rebalance, minus broker/approval."""
        screener_df = get_strategy_builder_screener_df(
            db, portfolio.screener_version_id, max(portfolio.portfolio_size, portfolio.worst_hold_rank)
        )
        tradelog_df = model_df(db, PaperTradelog, portfolio.id)

        # Overlay TODAY's close prices for accurate synthetic fill prices
        if not tradelog_df.empty:
            tradelog_df = tradelog_df.copy()
            for i in tradelog_df.loc[tradelog_df["active"] == True].index:
                sym = tradelog_df.at[i, "tradingsymbol"]
                fallback = float(tradelog_df.at[i, "ltp"] or tradelog_df.at[i, "buy_price"] or 0.0)
                tradelog_df.at[i, "ltp"] = _safe_latest_symbol_price(TODAY, sym, fallback)
        if not screener_df.empty:
            screener_df = screener_df.copy()
            for i, row in screener_df.iterrows():
                screener_df.at[i, "close"] = _safe_latest_symbol_price(TODAY, row["tradingsymbol"], float(row["close"] or 0.0))

        sell_df = get_sell_df_investment(TODAY, portfolio, f"(paper:{portfolio.id})", screener_df, tradelog_df, portfolio.worst_hold_rank)
        amount_to_sell = float(sell_df["amount"].sum()) if not sell_df.empty else 0.0

        # Paper fills are exact — only keep a small buffer for buy/sell charges
        # (0.11% + 0.21%) which update_equitycurve deducts from cash.
        cash_available = (float(portfolio.cash or 0.0) + amount_to_sell) * 0.995

        buy_df = get_buy_df_investment(
            TODAY, portfolio, f"(paper:{portfolio.id})",
            screener_df.head(portfolio.portfolio_size), tradelog_df,
            sell_stock_count=len(sell_df),
            portfolio_size=portfolio.portfolio_size,
            cash_available=cash_available,
            aum=float(portfolio.final_aum or portfolio.initial_aum),
        )

        # Synthetic fill: qty/price become actual, tagged with a PAPER order id
        for df in (sell_df, buy_df):
            for i, row in df.iterrows():
                df.at[i, "actual_qty"] = int(row["qty"])
                df.at[i, "actual_price"] = float(row["price"])
                df.at[i, "actual_amount"] = round(int(row["qty"]) * float(row["price"]), 2)
                df.at[i, "order_id"] = _paper_order_id()

        insert_df_to_db(db, sell_df, PaperSellStock)
        insert_df_to_db(db, buy_df, PaperBuyStock)
        logger.info("[Paper] Rebalance %s: %d sells, %d buys (synthetic fills)", portfolio.id, len(sell_df), len(buy_df))


platform_paper_service = PlatformPaperService()
