"""
Backtest Engine Service.

Two DB sessions are used:
  - app_db (screener_backtest_db): reading BacktestRun record, writing all result tables
  - equity_db (equity_ohlc): reading per-symbol OHLC price data (read-only)

Rebalance calendar is computed purely from available screener CSV dates -
no rebalance_calendar DB table needed.

Screener execution reads from CSV via screener_execution_service.
"""
import atexit
import hashlib
import json
import logging
import uuid
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from statistics import median
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from app.core.database import SessionLocal, EquitySessionLocal
from app.models.backtest import BacktestHoldingPeriod, BacktestRebalanceConstituent, BacktestRun
from app.models.result import BacktestDailyNav, BacktestDrawdownEpisode, BacktestMonthlyReturn, BacktestRebalanceEvent, BacktestSummary
from app.services import csv_data_service
from app.services import equity_data_service
from app.services.screener_execution_service import screener_execution_service

logger = logging.getLogger(__name__)

_backtest_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="backtest")
atexit.register(_backtest_executor.shutdown, wait=False)


class BacktestEngineService:

    # ── Hashing & submission ──────────────────────────────────────────────────

    def _hash(self, data: dict) -> str:
        return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()

    def submit_backtest(self, db: Session, request_data: dict, user_id, screener_id, screener_version_id) -> uuid.UUID:
        req_hash = self._hash(request_data)
        existing = db.query(BacktestRun).filter(BacktestRun.request_hash == req_hash).first()
        if existing:
            if existing.status in ("COMPLETED", "RUNNING", "QUEUED"):
                logger.info("Reusing existing backtest run %s (status=%s)", existing.id, existing.status)
                return existing.id
            if existing.status == "FAILED":
                db.delete(existing); db.commit()

        from_date = datetime.strptime(request_data["from_date"], "%Y-%m-%d").date()
        to_date   = datetime.strptime(request_data["to_date"],   "%Y-%m-%d").date()
        freq      = request_data.get("frequency", "weekly").lower()
        portfolio_size = request_data.get("portfolio_size", 30)
        wrh       = request_data.get("wrh", 40)
        universe_cfg   = request_data.get("universe", {})
        benchmark_symbol = (
            universe_cfg.get("value", "NIFTY 200").replace("_", " ")
            if universe_cfg.get("type") == "index" else "NIFTY 200"
        )

        run = BacktestRun(
            user_id=user_id, screener_id=screener_id, screener_version_id=screener_version_id,
            run_name=request_data.get("run_name", f"{from_date} to {to_date}"),
            benchmark_symbol=benchmark_symbol,
            from_date=from_date, to_date=to_date, rebalance_frequency=freq,
            portfolio_size=portfolio_size, wrh=wrh,
            transaction_cost_bps=float(request_data.get("transaction_cost_bps", 20.0)),
            slippage_bps=float(request_data.get("slippage_bps", 10.0)),
            initial_capital=float(request_data.get("initial_capital", 1_000_000.0)),
            status="RUNNING", request_hash=req_hash,
            created_at=datetime.utcnow(), started_at=datetime.utcnow(),
        )
        db.add(run); db.commit(); db.refresh(run)
        return run.id

    # ── Rebalance calendar (pure Python, no DB) ───────────────────────────────

    def build_rebalance_calendar(self, from_date: date, to_date: date, freq: str) -> List[date]:
        """
        Generates rebalance dates from available screener CSV dates.
        - weekly:  first available date in each ISO week
        - monthly: first available date in each calendar month
        Start date is always the first available screener date >= from_date.
        """
        all_dates = csv_data_service.get_available_screener_dates()
        if not all_dates:
            logger.error("No screener CSV dates available for rebalance calendar")
            return []

        in_range = [d for d in all_dates if from_date <= d <= to_date]
        if not in_range:
            return []

        start = in_range[0]
        rebalance_pool = [d for d in in_range if d > start]

        # Determine the start date's period key so we skip its week/month
        if freq == "monthly":
            start_key = (start.year, start.month)
            grouped: Dict[Tuple[int,int], date] = {}
            for d in rebalance_pool:
                key = (d.year, d.month)
                if key == start_key:
                    continue  # skip dates in same month as start
                if key not in grouped:
                    grouped[key] = d  # first date in month wins
            rebalance_dates = sorted(grouped.values())
        else:  # weekly (default)
            start_key = start.isocalendar()[:2]
            grouped: Dict[Tuple[int,int], date] = {}
            for d in rebalance_pool:
                key = d.isocalendar()[:2]  # (year, week)
                if key == start_key:
                    continue  # skip dates in same week as start
                if key not in grouped:
                    grouped[key] = d  # first date in week wins
            rebalance_dates = sorted(grouped.values())

        return [start] + rebalance_dates

    # ── Price loading helpers ─────────────────────────────────────────────────

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        try:
            if value is None or (isinstance(value, float) and np.isnan(value)):
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _next_trading_date(target: date, trading_dates: List[date]) -> Optional[date]:
        idx = bisect_left(trading_dates, target)
        return trading_dates[idx] if idx < len(trading_dates) else None

    def _load_price_frames(
        self, symbols: List[str], from_date: date, to_date: date, equity_db: Session
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[date]]:
        """Loads open/close price frames for all symbols from equity_ohlc DB."""
        combined = equity_data_service.get_multi_stock_ohlc(symbols, from_date, to_date, equity_db)
        if combined.empty:
            return pd.DataFrame(), pd.DataFrame(), []

        combined = combined.drop_duplicates(subset=["date", "symbol"], keep="last")
        trading_dates = sorted(combined["date"].unique().tolist())
        idx = pd.DatetimeIndex(pd.to_datetime(trading_dates))

        open_px  = combined.pivot(index="date", columns="symbol", values="open")
        close_px = combined.pivot(index="date", columns="symbol", values="close")
        open_px.index  = pd.to_datetime(open_px.index)
        close_px.index = pd.to_datetime(close_px.index)
        open_px  = open_px.reindex(idx).ffill()
        close_px = close_px.reindex(idx).ffill()
        return open_px, close_px, trading_dates

    @staticmethod
    def _lookup_px(frame: pd.DataFrame, dt: date, symbol: str) -> float:
        ts = pd.Timestamp(dt)
        if ts not in frame.index or symbol not in frame.columns:
            return 0.0
        value = frame.at[ts, symbol]
        return 0.0 if pd.isna(value) else float(value)

    def _valuation(self, holdings: Dict, dt: date, open_px: pd.DataFrame, close_px: pd.DataFrame) -> float:
        total = 0.0
        for symbol, pos in holdings.items():
            px = self._lookup_px(open_px, dt, symbol) or self._lookup_px(close_px, dt, symbol) or self._safe_float(pos.get("last_close"))
            total += pos["qty"] * px
        return total

    # ── Benchmark helpers ─────────────────────────────────────────────────────

    def _load_benchmark_nav(
        self, bm_sym: str, from_date: date, to_date: date, trading_dates: List[date], equity_db: Session
    ) -> Tuple[Dict[date, float], Dict[date, float]]:
        """Returns (bm_nav_by_date, bm_ret_by_date) dicts aligned to trading_dates."""
        bm_df = equity_data_service.get_benchmark_ohlc(bm_sym, from_date, to_date, equity_db)
        if bm_df.empty:
            logger.warning("No benchmark data for '%s'", bm_sym)
            return {}, {}

        bm_close = dict(zip(bm_df["date"], bm_df["close"].astype(float)))
        sorted_bm_dates = sorted(bm_close)

        bm_nav_by_date: Dict[date, float] = {}
        bm_ret_by_date: Dict[date, float] = {}
        base_bm_close: Optional[float] = None
        prev_bm_nav = 100.0
        prev_bm_close: Optional[float] = None

        for td in trading_dates:
            idx_bm = bisect_left(sorted_bm_dates, td)
            if idx_bm < len(sorted_bm_dates) and sorted_bm_dates[idx_bm] == td:
                bm_c = bm_close[td]
            elif idx_bm > 0:
                bm_c = bm_close[sorted_bm_dates[idx_bm - 1]]
            else:
                bm_c = prev_bm_close

            if bm_c is None:
                bm_nav_by_date[td] = prev_bm_nav
                bm_ret_by_date[td] = 0.0
                continue

            if base_bm_close is None:
                base_bm_close = bm_c
            bm_nav = (bm_c / base_bm_close) * 100.0
            bm_ret = (bm_nav / prev_bm_nav - 1.0) if prev_bm_nav > 0 else 0.0
            bm_nav_by_date[td] = bm_nav
            bm_ret_by_date[td] = bm_ret
            prev_bm_nav = bm_nav
            prev_bm_close = bm_c

        return bm_nav_by_date, bm_ret_by_date

    # ── Drawdown episodes ─────────────────────────────────────────────────────

    @staticmethod
    def _make_drawdown_episodes(nav_series: pd.Series) -> List[dict]:
        episodes = []
        if nav_series.empty:
            return episodes
        running_peak = nav_series.cummax()
        dd = (nav_series - running_peak) / running_peak
        in_dd = False; start_dt = trough_dt = None; trough_dd = 0.0; peak_nav = 0.0
        for dt, dd_val in dd.items():
            if dd_val < 0 and not in_dd:
                in_dd = True; start_dt = dt; trough_dt = dt; trough_dd = float(dd_val); peak_nav = float(running_peak.loc[dt])
            elif dd_val < 0 and in_dd:
                if dd_val < trough_dd:
                    trough_dd = float(dd_val); trough_dt = dt
            elif dd_val >= 0 and in_dd:
                episodes.append({"start_date": start_dt.date(), "trough_date": trough_dt.date(), "recovery_date": dt.date(), "drawdown": trough_dd, "peak_nav": peak_nav, "trough_nav": float(nav_series.loc[trough_dt]), "duration_days": int((dt - start_dt).days)})
                in_dd = False; start_dt = trough_dt = None; trough_dd = 0.0; peak_nav = 0.0
        if in_dd and start_dt and trough_dt:
            episodes.append({"start_date": start_dt.date(), "trough_date": trough_dt.date(), "recovery_date": None, "drawdown": trough_dd, "peak_nav": peak_nav, "trough_nav": float(nav_series.loc[trough_dt]), "duration_days": int((nav_series.index[-1] - start_dt).days)})
        return episodes


    # ── Main simulation ───────────────────────────────────────────────────────

    def execute_backtest_background(self, run_id: uuid.UUID):
        """
        Runs in a background thread. Opens its own DB sessions.
        app_db  → reads run config, writes all result rows
        equity_db → reads per-symbol OHLC (read-only)
        """
        app_db = SessionLocal()
        equity_db = EquitySessionLocal()
        run_record = None
        try:
            run_record = app_db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
            if not run_record:
                logger.error("BacktestRun %s not found", run_id)
                return

            # ── Read params from typed columns (no JSON blob needed) ──────────
            from_date       = run_record.from_date
            to_date         = run_record.to_date
            freq            = run_record.rebalance_frequency.lower()
            portfolio_size  = run_record.portfolio_size
            wrh             = run_record.wrh
            total_cost_rate = (float(run_record.transaction_cost_bps) + float(run_record.slippage_bps)) / 10000.0
            initial_capital = float(run_record.initial_capital)
            bm_sym          = (run_record.benchmark_symbol or "NIFTY 200").replace("_", " ")

            # ── Need universe/filters/ranking from screener version ───────────
            from app.models.screener import ScreenerVersion
            version = None
            if run_record.screener_version_id:
                version = app_db.query(ScreenerVersion).filter(ScreenerVersion.id == run_record.screener_version_id).first()
            universe_json = version.universe_json if version else {}
            filters_json  = version.filters_json  if version else []
            ranking_json  = version.ranking_json  if version else {}

            # ── Build rebalance calendar from CSV dates ───────────────────────
            basket_dates = self.build_rebalance_calendar(from_date, to_date, freq)
            if not basket_dates:
                run_record.status = "FAILED"; run_record.error_message = "No rebalance dates found."; app_db.commit(); return

            # ── Run screener on each rebalance date ───────────────────────────
            all_baskets: List[BacktestRebalanceConstituent] = []
            basket_plan: List[dict] = []
            previous_basket_symbols: set = set()
            fetch_limit = max(portfolio_size, wrh)

            for b_date in basket_dates:
                results, _, _ = screener_execution_service._execute_with_params(
                    universe_json=universe_json, filters_json=filters_json,
                    ranking_json=ranking_json, limit=fetch_limit, offset=0, target_date=b_date
                )
                all_eligible     = [r["symbol"] for r in results]
                rank_map         = {sym: idx + 1 for idx, sym in enumerate(all_eligible)}
                eligible_symbols = all_eligible[:portfolio_size]

                retained   = [s for s in previous_basket_symbols if rank_map.get(s, 10**9) <= wrh]
                new_basket = list(retained)
                for sym in eligible_symbols:
                    if len(new_basket) >= portfolio_size: break
                    if sym not in new_basket: new_basket.append(sym)

                sold_symbols     = sorted(previous_basket_symbols - set(new_basket))
                buy_symbols      = [s for s in new_basket if s not in previous_basket_symbols]
                retained_symbols = [s for s in new_basket if s in previous_basket_symbols]
                target_weight    = (1.0 / portfolio_size) if new_basket else 0.0

                basket_plan.append({"date": b_date, "rank_map": rank_map, "basket": new_basket, "sold_symbols": sold_symbols, "buy_symbols": buy_symbols, "retained_symbols": retained_symbols})

                for sym in sold_symbols:
                    all_baskets.append(BacktestRebalanceConstituent(backtest_run_id=run_record.id, rebalance_date=b_date, symbol=sym, rank_position=rank_map.get(sym, 999), action="SELL", target_weight=0.0, is_exited=True))
                for sym in new_basket:
                    is_retained = sym in previous_basket_symbols
                    all_baskets.append(BacktestRebalanceConstituent(backtest_run_id=run_record.id, rebalance_date=b_date, symbol=sym, rank_position=rank_map.get(sym, 999), action="RETAIN" if is_retained else "BUY", target_weight=target_weight, is_new_entry=not is_retained, is_retained=is_retained))

                previous_basket_symbols = set(new_basket)

            app_db.bulk_save_objects(all_baskets); app_db.commit()

            # ── Load price data from equity_ohlc ──────────────────────────────
            used_symbols = sorted({obj.symbol for obj in all_baskets if obj.target_weight > 0 or obj.action == "SELL"})
            if not used_symbols:
                run_record.status = "FAILED"; run_record.error_message = "No eligible symbols."; app_db.commit(); return

            open_px, close_px, trading_dates = self._load_price_frames(used_symbols, from_date, to_date, equity_db)
            if open_px.empty or not trading_dates:
                run_record.status = "FAILED"; run_record.error_message = "No OHLC data found in equity_ohlc."; app_db.commit(); return

            # ── Benchmark NAV ─────────────────────────────────────────────────
            bm_nav_by_date, bm_ret_by_date = self._load_benchmark_nav(bm_sym, from_date, to_date, trading_dates, equity_db)

            # ── Align basket plan to actual trading dates ─────────────────────
            aligned_plan, seen_exec = [], set()
            for plan in basket_plan:
                exec_date = self._next_trading_date(plan["date"], trading_dates)
                if exec_date is None or exec_date in seen_exec: continue
                seen_exec.add(exec_date); updated = dict(plan); updated["exec_date"] = exec_date; aligned_plan.append(updated)

            if not aligned_plan:
                run_record.status = "FAILED"; run_record.error_message = "No basket dates matched trading dates."; app_db.commit(); return

            plan_by_exec_date = {p["exec_date"]: p for p in aligned_plan}

            # ── Portfolio simulation ──────────────────────────────────────────
            holdings: Dict[str, dict] = {}
            net_cash = gross_cash = float(initial_capital)
            cumulative_cost_abs = 0.0
            realized_holding_periods: List[int] = []
            holding_period_objects: List[BacktestHoldingPeriod] = []
            rebalance_events_payload: List[dict] = []
            daily_nav_objects: List[BacktestDailyNav] = []
            prev_nav_net_abs = prev_nav_gross_abs = float(initial_capital)
            running_peak_norm = 100.0

            for current_date in trading_dates:
                day_cost_abs = day_trade_notional = event_turnover = 0.0
                positions_before = len(holdings)
                event_value_before_abs = event_value_after_abs = None
                added_count = dropped_count = retained_count = 0

                if current_date in plan_by_exec_date:
                    plan = plan_by_exec_date[current_date]
                    rank_map_now = plan["rank_map"]
                    positions_before = len(holdings)
                    event_value_before_abs = net_cash + self._valuation(holdings, current_date, open_px, close_px)

                    cash_before_sells = net_cash
                    stock_sell_amount = 0.0

                    for symbol in plan["sold_symbols"]:
                        pos = holdings.get(symbol)
                        if not pos: continue
                        sell_px = self._lookup_px(open_px, current_date, symbol) or self._lookup_px(close_px, current_date, symbol) or self._safe_float(pos.get("last_close"))
                        if sell_px <= 0: continue
                        gross_sale = pos["qty"] * sell_px; sell_cost = gross_sale * total_cost_rate
                        net_proceeds = gross_sale - sell_cost
                        net_cash += net_proceeds; gross_cash += gross_sale
                        stock_sell_amount += net_proceeds
                        day_cost_abs += sell_cost; cumulative_cost_abs += sell_cost; day_trade_notional += gross_sale; dropped_count += 1
                        holding_days = max(1, (current_date - pos["entry_date"]).days)
                        realized_holding_periods.append(holding_days)
                        entry_p = pos.get("entry_price", 0.0)
                        gross_ret = (sell_px / entry_p - 1.0) if entry_p > 0 else None
                        net_ret = (gross_ret - total_cost_rate) if gross_ret is not None else None
                        holding_period_objects.append(BacktestHoldingPeriod(backtest_run_id=run_record.id, symbol=symbol, entry_date=pos["entry_date"], exit_date=current_date, entry_rank=pos.get("entry_rank"), holding_days=holding_days, entry_price=entry_p, exit_price=sell_px, entry_weight=pos.get("entry_weight", 0.0), exit_weight=float(gross_sale / (event_value_before_abs or prev_nav_net_abs)) if (event_value_before_abs or prev_nav_net_abs) > 0 else 0.0, gross_return=gross_ret, net_return=net_ret, exit_reason="NOT_IN_TOP_N"))
                        del holdings[symbol]

                    retained_count = sum(1 for s in plan["retained_symbols"] if s in holdings)
                    open_slots = max(0, portfolio_size - len(holdings))
                    candidate_buys = plan["buy_symbols"][:open_slots]

                    if candidate_buys:
                        # Broker releases only 80% of sell proceeds on same day
                        deployable_cash = max(0.0, (cash_before_sells + stock_sell_amount * 0.80) * 0.98)
                        capital_per_stock = deployable_cash / len(candidate_buys) if candidate_buys else 0.0
                        for symbol in candidate_buys:
                            buy_px = self._lookup_px(open_px, current_date, symbol)
                            if buy_px <= 0: continue
                            qty = int(capital_per_stock / buy_px) if capital_per_stock > 0 else 0
                            if qty <= 0: continue
                            gross_buy = qty * buy_px; buy_cost = gross_buy * total_cost_rate; total_needed = gross_buy + buy_cost
                            if total_needed > net_cash:
                                qty = max(0, int(net_cash / (buy_px * (1 + total_cost_rate))))
                                if qty <= 0: continue
                                gross_buy = qty * buy_px; buy_cost = gross_buy * total_cost_rate; total_needed = gross_buy + buy_cost
                            if total_needed > net_cash or qty <= 0: continue
                            net_cash -= total_needed; gross_cash -= gross_buy
                            day_cost_abs += buy_cost; cumulative_cost_abs += buy_cost; day_trade_notional += gross_buy; added_count += 1
                            holdings[symbol] = {"qty": qty, "entry_date": current_date, "entry_rank": rank_map_now.get(symbol), "entry_price": buy_px, "last_close": self._lookup_px(close_px, current_date, symbol) or buy_px, "entry_weight": float(gross_buy / (event_value_before_abs or prev_nav_net_abs)) if (event_value_before_abs or prev_nav_net_abs) > 0 else 0.0}

                    event_value_after_abs = net_cash + self._valuation(holdings, current_date, open_px, close_px)
                    event_turnover = (day_trade_notional / (event_value_before_abs or prev_nav_net_abs)) if (event_value_before_abs or prev_nav_net_abs) > 0 else 0.0
                    rebalance_events_payload.append({"rebalance_date": current_date, "portfolio_value_before": float(event_value_before_abs or prev_nav_net_abs), "portfolio_value_after": float(event_value_after_abs or prev_nav_net_abs), "turnover": float(event_turnover), "transaction_cost": float(day_cost_abs), "positions_before": positions_before, "positions_after": len(holdings), "added": added_count, "dropped": dropped_count, "retained": retained_count})

                holdings_value_abs = 0.0
                for symbol, pos in holdings.items():
                    close_val = self._lookup_px(close_px, current_date, symbol)
                    if close_val <= 0: close_val = self._safe_float(pos.get("last_close"), pos.get("entry_price", 0.0))
                    pos["last_close"] = close_val; holdings_value_abs += pos["qty"] * close_val

                nav_net_abs   = net_cash + holdings_value_abs
                nav_gross_abs = gross_cash + holdings_value_abs
                nav_net_norm  = (nav_net_abs / initial_capital) * 100.0
                nav_gross_norm = (nav_gross_abs / initial_capital) * 100.0
                daily_return_net   = (nav_net_abs / prev_nav_net_abs - 1.0) if prev_nav_net_abs > 0 else 0.0
                daily_return_gross = (nav_gross_abs / prev_nav_gross_abs - 1.0) if prev_nav_gross_abs > 0 else 0.0
                running_peak_norm  = max(running_peak_norm, nav_net_norm)
                drawdown = (nav_net_norm / running_peak_norm - 1.0) if running_peak_norm > 0 else 0.0
                daily_cost_ratio = (day_cost_abs / prev_nav_net_abs) if prev_nav_net_abs > 0 else 0.0

                daily_nav_objects.append(BacktestDailyNav(backtest_run_id=run_record.id, trade_date=current_date, portfolio_return_gross=float(daily_return_gross), portfolio_return_net=float(daily_return_net), portfolio_nav_gross=float(nav_gross_norm), portfolio_nav_net=float(nav_net_norm), benchmark_return=bm_ret_by_date.get(current_date), benchmark_nav=bm_nav_by_date.get(current_date), running_peak_nav=float(running_peak_norm), drawdown=float(drawdown), daily_turnover=float(event_turnover), daily_cost=float(daily_cost_ratio)))
                prev_nav_net_abs = nav_net_abs; prev_nav_gross_abs = nav_gross_abs

            if not daily_nav_objects:
                run_record.status = "FAILED"; run_record.error_message = "Simulation produced no NAV rows."; app_db.commit(); return

            # ── Close open positions ──────────────────────────────────────────
            for symbol, pos in holdings.items():
                holding_days = max(1, (trading_dates[-1] - pos["entry_date"]).days)
                realized_holding_periods.append(holding_days)
                close_px_val = pos.get("last_close", pos.get("entry_price", 0.0))
                entry_p = pos.get("entry_price", 0.0)
                gross_val = pos["qty"] * close_px_val
                gross_ret_open = (close_px_val / entry_p - 1.0) if entry_p > 0 else None
                net_ret_open   = (gross_ret_open - total_cost_rate) if gross_ret_open is not None else None
                holding_period_objects.append(BacktestHoldingPeriod(backtest_run_id=run_record.id, symbol=symbol, entry_date=pos["entry_date"], exit_date=trading_dates[-1], entry_rank=pos.get("entry_rank"), holding_days=holding_days, entry_price=entry_p, exit_price=close_px_val, entry_weight=pos.get("entry_weight", 0.0), exit_weight=float(gross_val / nav_net_abs) if nav_net_abs > 0 else 0.0, gross_return=gross_ret_open, net_return=net_ret_open, exit_reason="END_OF_BACKTEST"))

            app_db.bulk_save_objects(daily_nav_objects)
            if holding_period_objects: app_db.bulk_save_objects(holding_period_objects)
            app_db.commit()

            # ── Rebalance events ──────────────────────────────────────────────
            rebalance_objects = [BacktestRebalanceEvent(backtest_run_id=run_record.id, rebalance_date=p["rebalance_date"], portfolio_value_before=p["portfolio_value_before"] / initial_capital * 100.0, portfolio_value_after=p["portfolio_value_after"] / initial_capital * 100.0, turnover=p["turnover"], transaction_cost=p["transaction_cost"] / initial_capital, positions_before=p["positions_before"], positions_after=p["positions_after"], added_count=p["added"], dropped_count=p["dropped"], retained_count=p["retained"]) for p in rebalance_events_payload]
            if rebalance_objects: app_db.bulk_save_objects(rebalance_objects); app_db.commit()

            # ── Summary statistics ────────────────────────────────────────────
            nav_df = pd.DataFrame([{"trade_date": obj.trade_date, "ret_net": obj.portfolio_return_net, "ret_gross": obj.portfolio_return_gross, "nav_net": obj.portfolio_nav_net, "nav_gross": obj.portfolio_nav_gross, "drawdown": obj.drawdown, "turnover": obj.daily_turnover, "cost": obj.daily_cost} for obj in daily_nav_objects])
            nav_df["trade_date"] = pd.to_datetime(nav_df["trade_date"])
            nav_df = nav_df.sort_values("trade_date").set_index("trade_date")
            ret_net = nav_df["ret_net"].astype(float); nav_net = nav_df["nav_net"].astype(float)
            nav_gross = nav_df["nav_gross"].astype(float); drawdown_series = nav_df["drawdown"].astype(float)
            total_return       = float(nav_net.iloc[-1] / 100.0 - 1.0)
            gross_total_return = float(nav_gross.iloc[-1] / 100.0 - 1.0)
            elapsed_days = max(1, (nav_df.index[-1] - nav_df.index[0]).days)
            cagr      = float((nav_net.iloc[-1] / nav_net.iloc[0]) ** (365.25 / elapsed_days) - 1.0) if nav_net.iloc[0] > 0 else 0.0
            annual_vol = float(ret_net.std(ddof=0) * np.sqrt(252)) if len(ret_net) > 1 else 0.0
            sharpe    = float(cagr / annual_vol) if annual_vol > 0 else 0.0
            downside  = ret_net[ret_net < 0]; downside_dev = float(downside.std(ddof=0) * np.sqrt(252)) if len(downside) > 0 else 0.0
            sortino   = float(cagr / downside_dev) if downside_dev > 0 else 0.0
            max_dd    = float(drawdown_series.min()) if not drawdown_series.empty else 0.0
            calmar    = float(cagr / abs(max_dd)) if max_dd != 0 else 0.0
            monthly_nav = nav_net.resample("ME").last()
            monthly_nav_padded = pd.concat([pd.Series([100.0], index=[monthly_nav.index[0] - pd.offsets.MonthEnd(1)]), monthly_nav])
            monthly_rets = monthly_nav_padded.pct_change().dropna()

            bm_nav_series = pd.Series({pd.Timestamp(d): v for d, v in bm_nav_by_date.items()}).sort_index() if bm_nav_by_date else None
            bm_monthly_rets = None
            if bm_nav_series is not None and not bm_nav_series.empty:
                bm_monthly_nav = bm_nav_series.resample("ME").last()
                bm_monthly_nav_padded = pd.concat([pd.Series([100.0], index=[bm_monthly_nav.index[0] - pd.offsets.MonthEnd(1)]), bm_monthly_nav])
                bm_monthly_rets = bm_monthly_nav_padded.pct_change().dropna()

            monthly_objects = []
            for dt, mret in monthly_rets.items():
                bm_mret = float(bm_monthly_rets.get(dt, 0.0)) if bm_monthly_rets is not None else None
                excess_mret = (float(mret) - bm_mret) if bm_mret is not None else float(mret)
                monthly_objects.append(BacktestMonthlyReturn(backtest_run_id=run_record.id, year=dt.year, month=dt.month, monthly_return=float(mret), benchmark_monthly_return=bm_mret, excess_monthly_return=excess_mret))
            if monthly_objects: app_db.bulk_save_objects(monthly_objects); app_db.commit()

            positive_month_pct = float((monthly_rets > 0).mean()) if len(monthly_rets) > 0 else 0.0
            best_month = float(monthly_rets.max()) if len(monthly_rets) > 0 else 0.0
            worst_month = float(monthly_rets.min()) if len(monthly_rets) > 0 else 0.0
            avg_month   = float(monthly_rets.mean()) if len(monthly_rets) > 0 else 0.0
            event_turnovers = [p["turnover"] for p in rebalance_events_payload]
            avg_turnover = float(np.mean(event_turnovers)) if event_turnovers else 0.0
            annualized_turnover = float(np.sum(event_turnovers) * (252 / max(1, len(nav_df)))) if event_turnovers else 0.0
            retention_pcts = [(p["retained"] / p["positions_before"]) for p in rebalance_events_payload if p["positions_before"] > 0]
            churn_pcts     = [(p["dropped"] / p["positions_before"])  for p in rebalance_events_payload if p["positions_before"] > 0]
            avg_retention_pct = float(np.mean(retention_pcts)) if retention_pcts else 0.0
            avg_churn_pct     = float(np.mean(churn_pcts)) if churn_pcts else 0.0
            avg_holding_days    = float(np.mean(realized_holding_periods)) if realized_holding_periods else 0.0
            median_holding_days = float(median(realized_holding_periods)) if realized_holding_periods else 0.0

            benchmark_total_return = benchmark_cagr_val = excess_cagr_val = hit_ratio_val = upside_cap_val = downside_cap_val = None
            if bm_nav_by_date:
                bm_nav_arr = pd.Series({pd.Timestamp(d): v for d, v in bm_nav_by_date.items()}).sort_index().astype(float)
                if not bm_nav_arr.empty and bm_nav_arr.iloc[0] > 0:
                    benchmark_total_return = float(bm_nav_arr.iloc[-1] / 100.0 - 1.0)
                    bm_elapsed = max(1, (bm_nav_arr.index[-1] - bm_nav_arr.index[0]).days)
                    benchmark_cagr_val = float((bm_nav_arr.iloc[-1] / bm_nav_arr.iloc[0]) ** (365.25 / bm_elapsed) - 1.0)
                    excess_cagr_val = cagr - benchmark_cagr_val
                    bm_ret_arr = bm_nav_arr.pct_change().fillna(0.0)
                    port_ret_arr = nav_df["ret_net"].reindex(bm_ret_arr.index).fillna(0.0)
                    both_nonzero = (port_ret_arr != 0) | (bm_ret_arr != 0)
                    if both_nonzero.sum() > 0:
                        hit_ratio_val = float((port_ret_arr[both_nonzero] > bm_ret_arr[both_nonzero]).mean())
                    up_days = bm_ret_arr > 0
                    if up_days.sum() > 0: upside_cap_val = float(port_ret_arr[up_days].mean() / bm_ret_arr[up_days].mean())
                    down_days = bm_ret_arr < 0
                    if down_days.sum() > 0: downside_cap_val = float(port_ret_arr[down_days].mean() / bm_ret_arr[down_days].mean())

            summary = BacktestSummary(
                backtest_run_id=run_record.id,
                metrics_json={
                    "total_return": total_return,
                    "cagr": cagr,
                    "volatility": annual_vol,
                    "sharpe": sharpe,
                    "sortino": sortino,
                    "calmar": calmar,
                    "max_drawdown": max_dd,
                    "benchmark_total_return": benchmark_total_return,
                    "benchmark_cagr": benchmark_cagr_val,
                    "excess_cagr": excess_cagr_val,
                    "hit_ratio_vs_benchmark": hit_ratio_val,
                    "upside_capture": upside_cap_val,
                    "downside_capture": downside_cap_val,
                    "positive_month_pct": positive_month_pct,
                    "best_month": best_month,
                    "worst_month": worst_month,
                    "avg_month": avg_month,
                    "total_rebalances": len(rebalance_events_payload),
                    "avg_turnover": avg_turnover,
                    "annualized_turnover": annualized_turnover,
                    "total_cost_drag": float(gross_total_return - total_return),
                    "avg_holding_days": avg_holding_days,
                    "median_holding_days": median_holding_days,
                    "avg_retention_pct": avg_retention_pct,
                    "avg_churn_pct": avg_churn_pct,
                },
            )
            app_db.add(summary)

            dd_episodes = self._make_drawdown_episodes(nav_net)
            dd_objects = [BacktestDrawdownEpisode(backtest_run_id=run_record.id, peak_date=ep["start_date"], trough_date=ep["trough_date"], recovery_date=ep["recovery_date"], drawdown_pct=ep["drawdown"], peak_to_trough_days=(ep["trough_date"] - ep["start_date"]).days, trough_to_recovery_days=(ep["recovery_date"] - ep["trough_date"]).days if ep["recovery_date"] else None, total_recovery_days=(ep["recovery_date"] - ep["start_date"]).days if ep["recovery_date"] else None) for ep in dd_episodes]
            if dd_objects: app_db.bulk_save_objects(dd_objects)

            run_record.status = "COMPLETED"; run_record.completed_at = datetime.utcnow(); run_record.progress_pct = 100
            app_db.commit()
            logger.info("Backtest %s COMPLETED", run_id)
            return run_record.id

        except Exception as e:
            logger.error("Backtest %s FAILED: %s", run_id, e, exc_info=True)
            app_db.rollback()
            try:
                if run_record:
                    run_record.status = "FAILED"; run_record.error_message = str(e)
                    app_db.add(run_record); app_db.commit()
            except Exception as inner_e:
                app_db.rollback(); logger.error("Failed to update run status: %s", inner_e)
            raise
        finally:
            equity_db.close(); app_db.close()


backtest_engine_service = BacktestEngineService()
