"""
Equitycase-style live investment service for Strategy Builder.

This file intentionally mirrors the flow of equitycase_logic/strategy_equity/
investment_strategy_ra.py:

16:45/preparation logic:
    screener_df -> get_sell_df_investment() -> sell table
    screener_df -> get_buy_df_investment()  -> buy table

09:15/execution logic:
    equitycase_logic places direct broker orders.
    Here we generate Publisher/offsite basket and wait for postback.

16:30/daily MTM logic:
    Postback updates buy/sell/circuit stock tables directly.
    Daily 16:30 celery job processes fills into tradelog entries, refreshes LTP,
    and appends equity curve rows.
"""
from __future__ import annotations
import pytz
import uuid
import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import date, timedelta, datetime, timezone
from typing import Any, Dict, Optional
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, func
from app.core.database import equity_engine, EquitycaseSessionLocal
from app.models.screener import ScreenerVersion
from app.models.live_investment import (
    LiveStrategy,
    LiveBuyStock,
    LiveSellStock,
    LiveCircuitStock,
    LiveTradelog,
    LiveEquityCurve,
    LivePublisherBasket,
    LiveBrokerAccount,
    LiveStatus,
)
import redis
from app.core.encryption import decrypt_token
from app.services.screener_execution_service import screener_execution_service
from app.core.trading_calendar import next_trading_day, should_prepare_rebalance, next_rebalance_prepare_date
from app.services.broker_publishers import get_publisher_adapter
from app.core.config import settings
from app.services.notifications import notify_all

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Small DB dataframe helpers equivalent to database_equity.py style
# -----------------------------------------------------------------------------

def model_df(db: Session, model, automate_equity_ra_id) -> pd.DataFrame:
    q = db.query(model).filter(model.automate_equity_ra_id == automate_equity_ra_id)
    # Order chronologically by date when available, NOT by random UUID id.
    # LiveEquityCurve can have multiple rows on the same date (initial + daily MTM),
    # so use total_days as secondary sort. Other date-bearing models use date + id.
    if hasattr(model, "date") and hasattr(model, "total_days"):
        q = q.order_by(model.date.asc(), model.total_days.asc(), model.id.asc())
    elif hasattr(model, "date"):
        q = q.order_by(model.date.asc(), model.id.asc())
    elif hasattr(model, "buy_date"):
        q = q.order_by(model.buy_date.asc(), model.id.asc())
    else:
        q = q.order_by(model.id.asc())
    return pd.read_sql(q.statement, db.bind)


def insert_df_to_db(db: Session, df: pd.DataFrame, model) -> None:
    if df is None or df.empty:
        return
    rows = df.to_dict(orient="records")
    db.bulk_insert_mappings(model, rows)
    db.commit()


def update_df_to_db(db: Session, df: pd.DataFrame, model, commit: bool = True) -> None:
    if df is None or df.empty:
        return
    rows = df.to_dict(orient="records")
    update_rows = []
    for row in rows:
        pk = row.get("id")
        if pk is None or pd.isna(pk):
            continue
        clean = {k: (None if pd.isna(v) else v) for k, v in row.items() if hasattr(model, k)}
        update_rows.append(clean)
    if update_rows:
        db.bulk_update_mappings(model, update_rows)
    if commit:
        db.commit()


def upsert_df_to_db(db: Session, df: pd.DataFrame, model, commit: bool = True) -> None:
    if df is None or df.empty:
        return
    rows = df.to_dict(orient="records")
    update_rows = []
    insert_rows = []
    for row in rows:
        pk = row.get("id")
        if pk is not None and pd.notna(pk):
            clean = {k: (None if pd.isna(v) else v) for k, v in row.items() if hasattr(model, k)}
            update_rows.append(clean)
        else:
            clean = {k: (None if pd.isna(v) else v) for k, v in row.items() if hasattr(model, k)}
            clean.pop("id", None)
            insert_rows.append(clean)
    if update_rows:
        db.bulk_update_mappings(model, update_rows)
    if insert_rows:
        db.bulk_insert_mappings(model, insert_rows)
    if commit:
        db.commit()


def insert_single_row_to_db(db: Session, row: Dict[str, Any], model, commit: bool = True) -> None:
    clean = {k: (None if pd.isna(v) else v) for k, v in row.items() if hasattr(model, k)}
    clean.pop("id", None)
    db.add(model(**clean))
    if commit:
        db.commit()


# -----------------------------------------------------------------------------
# Equitycase-like DataFrame table makers
# -----------------------------------------------------------------------------

def make_buy_table() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "automate_equity_ra_id", "tradingsymbol", "isin", "date", "qty", "price",
        "amount", "weightage", "actual_qty", "actual_price",
        "actual_amount", "stoploss", "volatility", "order_id", "circuit",
        "updated_in_tradelog",
    ])


def make_sell_table() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "automate_equity_ra_id", "tradingsymbol", "isin", "date", "qty", "price",
        "amount", "actual_qty", "actual_price", "actual_amount", "order_id",
        "method", "circuit", "updated_in_tradelog",
    ])


def make_circuit_table() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "automate_equity_ra_id", "tradingsymbol", "isin", "date", "qty", "price",
        "amount", "weightage", "actual_qty", "actual_price", "actual_amount",
        "stoploss", "volatility", "order_id", "updated_in_tradelog",
        "lower_upper", "action", "active",
    ])


@dataclass
class LiveConfig:
    portfolio_size: int
    worst_hold_rank: int
    rebalance_frequency: str


# -----------------------------------------------------------------------------
# Live LTP helper — same Kite API used by basket
# -----------------------------------------------------------------------------

def _fetch_live_ltp(strategy, symbols: list) -> Dict[str, float]:
    """Fetch real-time LTP using the strategy's broker adapter.

    Returns {tradingsymbol: last_price} dict. Empty dict on failure.
    Used to show accurate live prices in previews.
    """
    if not symbols:
        return {}
    try:
        adapter = get_publisher_adapter(strategy.broker)
        if hasattr(adapter, 'fetch_ltp_bulk'):
            return adapter.fetch_ltp_bulk(symbols)
    except Exception as e:
        logger.warning("[LiveLTP] Failed to fetch live LTP: %s — falling back to stored prices", e)
    return {}


# -----------------------------------------------------------------------------
# Screener conversion: Strategy Builder result -> equitycase-compatible screener_df
# -----------------------------------------------------------------------------

def get_strategy_builder_screener_df(
    db: Session,
    screener_version_id,
    portfolio_size: int,
) -> pd.DataFrame:
    """
    Returns columns expected by equitycase investment logic:
        tradingsymbol, isin, close, rank, volar

    rank is row number after Strategy Builder ranking.
    volar is taken from sort_value when available; otherwise rank fallback.
    """
    result = screener_execution_service.execute_screener(db, screener_version_id, limit=max(portfolio_size, 1), offset=0)
    rows = []
    for i, item in enumerate(result.get("results", []), start=1):
        indicators = item.get("indicators") or {}
        symbol = item.get("symbol") or item.get("tradingsymbol")
        close = item.get("close") or indicators.get("close")
        sort_value = item.get("sort_value") or indicators.get("sort_value")
        isin = indicators.get("isin") or indicators.get("ISIN") or ""
        if symbol and close is not None:
            rows.append({
                "tradingsymbol": symbol,
                "isin": isin,
                "close": float(close),
                "rank": i,
                "volar": float(sort_value) if sort_value is not None else float(portfolio_size - i + 1),
            })
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# Same investment selection logic as equitycase_logic
# -----------------------------------------------------------------------------

def get_sell_df_investment(
    TODAY: date,
    user: LiveStrategy,
    unique_user: str,
    screener_df: pd.DataFrame,
    tradelog_df: pd.DataFrame,
    worst_hold_rank: int,
) -> pd.DataFrame:
    sell_df = make_sell_table()
    new_sell_rows = []
    if tradelog_df.empty:
        return sell_df

    active_tradelog_df = tradelog_df.loc[tradelog_df["active"] == True]
    holding_stock_set = set(screener_df["tradingsymbol"].head(worst_hold_rank)) if not screener_df.empty else set()

    for _, row in active_tradelog_df.iterrows():
        tradingsymbol = row["tradingsymbol"]
        if tradingsymbol not in holding_stock_set:
            buy_qty = int(row["buy_qty"] or 0)
            sell_qty = int(row["sell_qty"] or 0)
            remaining_qty = buy_qty - sell_qty
            if remaining_qty <= 0:
                continue
            sell_price = float(row["ltp"] or row["buy_price"] or 0)
            sell_amount = round(remaining_qty * sell_price, 2)
            new_sell_rows.append({
                "automate_equity_ra_id": user.id,
                "tradingsymbol": tradingsymbol,
                "isin": row.get("isin", ""),
                "date": TODAY,
                "qty": remaining_qty,
                "price": sell_price,
                "amount": sell_amount,
                "actual_qty": 0,
                "actual_price": 0.0,
                "actual_amount": 0.0,
                "order_id": None,
                "method": "WRH",
                "circuit": False,
                "updated_in_tradelog": False,
            })

    if new_sell_rows:
        sell_df = pd.concat([sell_df, pd.DataFrame(new_sell_rows)], ignore_index=True)
    return sell_df


def get_buy_df_investment(
    TODAY: date,
    user: LiveStrategy,
    unique_user: str,
    screener_df: pd.DataFrame,
    tradelog_df: pd.DataFrame,
    sell_stock_count: int,
    portfolio_size: int,
    cash_available: float,
    aum: float,
) -> pd.DataFrame:
    buy_df = make_buy_table()
    active_symbols = set()
    if not tradelog_df.empty:
        active_symbols = set(tradelog_df.loc[tradelog_df["active"] == True, "tradingsymbol"])

    buy_capacity = portfolio_size - len(active_symbols) + sell_stock_count
    if buy_capacity <= 0 or cash_available <= 0 or screener_df.empty:
        return buy_df

    cash_per_stock = cash_available / buy_capacity
    new_buy_rows = []

    for _, row in screener_df.iterrows():
        if buy_capacity <= 0:
            break
        tradingsymbol = row["tradingsymbol"]
        if tradingsymbol in active_symbols:
            continue
        close = float(row["close"] or 0)
        if close <= 0:
            continue
        buy_qty = int(cash_per_stock // close)
        if buy_qty <= 0:
            continue
        buy_amount = round(buy_qty * close, 2)
        weightage = round((buy_amount / aum) * 100, 2) if aum else 0.0
        new_buy_rows.append({
            "automate_equity_ra_id": user.id,
            "tradingsymbol": tradingsymbol,
            "isin": row.get("isin", ""),
            "date": TODAY,
            "qty": buy_qty,
            "price": close,
            "amount": buy_amount,
            "weightage": weightage,
            "actual_qty": 0,
            "actual_price": 0.0,
            "actual_amount": 0.0,
            "stoploss": 0.0,
            "volatility": 0.0,
            "order_id": None,
            "circuit": False,
            "updated_in_tradelog": False,
        })
        buy_capacity -= 1

    if new_buy_rows:
        buy_df = pd.concat([buy_df, pd.DataFrame(new_buy_rows)], ignore_index=True)
    return buy_df


# -----------------------------------------------------------------------------
# Publisher/order-book replacement logic
# -----------------------------------------------------------------------------

def _publisher_tag(strategy_id, side: str, row_id: int) -> str:
    """Generate a unique Kite Publisher tag for each order.

    Kite tag constraint: max 8 alphanumeric chars.
    Each order gets a unique random tag (uuid4 hex prefix).
    """
    return uuid.uuid4().hex[:8]


def build_publisher_payload(strategy: LiveStrategy, buy_df: pd.DataFrame, sell_df: pd.DataFrame) -> Dict[str, Any]:
    """Broker-agnostic wrapper. Broker-specific payload lives in app/services/broker_publishers/."""
    adapter = get_publisher_adapter(strategy.broker)
    return adapter.build_payload(strategy=strategy, buy_df=buy_df, sell_df=sell_df)


def assign_publisher_tags(db: Session, strategy: LiveStrategy, basket_type: str, side: str = "ALL") -> LivePublisherBasket:
    """Equivalent of 09:15 direct order placement, but only creates broker-specific Publisher/offsite payload.

    Important broker lock rule:
    - Strategy already has broker_account_id / broker / broker_account_label locked from Go Live.
    - Rebalance and Exit never accept broker selection again.
    - This function always uses the locked broker adapter.

    side parameter (only used for REBALANCE basket_type):
    - "ALL"  — include both buy and sell orders (default, used by INITIAL/EXIT)
    - "SELL" — include only sell orders (rebalance step 1)
    - "BUY"  — include only buy orders (rebalance step 2, after sells complete)
    """
    # Resolve the effective basket_type for storage (e.g., REBALANCE_SELL, REBALANCE_BUY)
    effective_basket_type = basket_type
    if basket_type == "REBALANCE" and side in ("SELL", "BUY"):
        effective_basket_type = f"REBALANCE_{side}"

    # Guard: prevent double Trade Now — return existing pending basket if any
    # Must check by effective_basket_type so BUY request doesn't return old SELL basket
    # IMPORTANT: Only fetch baskets created TODAY to avoid returning abandoned baskets from past rebalances!
    existing_basket = db.query(LivePublisherBasket).filter(
        LivePublisherBasket.automate_equity_ra_id == strategy.id,
        LivePublisherBasket.status == "PENDING_USER_APPROVAL",
        LivePublisherBasket.basket_type == effective_basket_type,
        func.date(LivePublisherBasket.created_at) == date.today(),
    ).order_by(LivePublisherBasket.created_at.desc()).first()
    if existing_basket:
        # Only treat it as a genuine double-click (reuse the basket) when the strategy
        # status actually matches this basket's pending phase. If the status has since
        # moved on (e.g. it was reset to REBALANCE_READY by a re-prepare), the basket is
        # STALE — reusing it would skip tag assignment + the status transition and wedge
        # the flow. In that case, cancel the stale basket and fall through to create a
        # fresh one.
        expected_status = {
            "INITIAL": {LiveStatus.PENDING_USER_APPROVAL},
            "REBALANCE": {LiveStatus.REBALANCE_PENDING_USER_APPROVAL},
            "REBALANCE_SELL": {LiveStatus.REBALANCE_PENDING_USER_APPROVAL},
            "REBALANCE_BUY": {LiveStatus.REBALANCE_SELL_COMPLETE, LiveStatus.REBALANCE_PROCESSING},
            "EXIT": {LiveStatus.EXIT_PENDING_USER_APPROVAL},
        }.get(effective_basket_type, set())
        if not expected_status or strategy.status in expected_status:
            return existing_basket
        logger.info("[AssignTags] Cancelling stale %s basket %s (strategy status=%s, not a live pending phase) — creating fresh basket",
                    effective_basket_type, existing_basket.id, strategy.status.value)
        existing_basket.status = "CANCELLED"
        db.flush()

    buy_df = model_df(db, LiveBuyStock, strategy.id)
    sell_df = model_df(db, LiveSellStock, strategy.id)

    # Only pending rows should be sent.
    if not buy_df.empty:
        buy_df = buy_df.loc[(buy_df["actual_qty"].fillna(0) == 0) & (buy_df["updated_in_tradelog"].fillna(False) == False)].copy()
        for i, row in buy_df.iterrows():
            ptag = row.get("publisher_tag")
            if pd.isna(ptag) or not ptag:
                buy_df.at[i, "publisher_tag"] = _publisher_tag(strategy.id, "BUY", int(row["id"]))
        update_df_to_db(db, buy_df, LiveBuyStock)

    if not sell_df.empty:
        sell_df = sell_df.loc[(sell_df["actual_qty"].fillna(0) == 0) & (sell_df["updated_in_tradelog"].fillna(False) == False)].copy()
        for i, row in sell_df.iterrows():
            ptag = row.get("publisher_tag")
            if pd.isna(ptag) or not ptag:
                sell_df.at[i, "publisher_tag"] = _publisher_tag(strategy.id, "SELL", int(row["id"]))
        update_df_to_db(db, sell_df, LiveSellStock)

    # ── Tag verification: ensure no order row has an empty publisher_tag ──
    # If any row has an empty tag, the Kite order will have no tag → postback
    # can't match → actual_qty/price/order_id never update → status gets stuck.
    for df_label, df_check in [("buy", buy_df), ("sell", sell_df)]:
        if not df_check.empty:
            empty_tags = df_check[df_check["publisher_tag"].isna() | (df_check["publisher_tag"] == "")]
            if not empty_tags.empty:
                symbols = list(empty_tags["tradingsymbol"])
                logger.error("[AssignTags] %d %s rows still have EMPTY publisher_tag after assignment! symbols=%s",
                             len(empty_tags), df_label, symbols)

    # ── Side filtering for split rebalance ────────────────────────────────
    if side == "SELL":
        buy_df = buy_df.iloc[0:0]  # Empty — only sell orders in this basket
    elif side == "BUY":
        # Guard: BUY side only allowed after all sell orders are complete
        # Check for any sell orders that are NOT in a terminal state
        # (broker_status is NULL means not yet sent/no postback received)
        pending_sell = db.query(LiveSellStock).filter(
            LiveSellStock.automate_equity_ra_id == strategy.id,
            LiveSellStock.updated_in_tradelog == False,
            or_(
                LiveSellStock.broker_status.is_(None),
                ~LiveSellStock.broker_status.in_(["COMPLETE", "REJECTED", "CANCELLED"]),
            ),
        ).count()
        if pending_sell > 0:
            raise HTTPException(
                status_code=400,
                detail="Please complete sell orders before placing buy orders.",
            )
        sell_df = sell_df.iloc[0:0]  # Empty — only buy orders in this basket

    payload = build_publisher_payload(strategy, buy_df, sell_df)
    if not payload.get("basket") and not payload.get("orders"):
        raise HTTPException(status_code=400, detail="No pending orders available for Publisher basket.")

    basket = LivePublisherBasket(
        automate_equity_ra_id=strategy.id,
        broker_account_id=getattr(strategy, "broker_account_id", None),
        broker=strategy.broker,
        basket_key=f"{str(strategy.id)[:8]}-{effective_basket_type}-{uuid.uuid4().hex[:8]}",
        basket_type=effective_basket_type,
        status="PENDING_USER_APPROVAL",
        publisher_payload=payload,
    )
    db.add(basket)

    if effective_basket_type in ("REBALANCE", "REBALANCE_SELL"):
        strategy.status = LiveStatus.REBALANCE_PENDING_USER_APPROVAL
    elif effective_basket_type == "REBALANCE_BUY":
        pass  # Keep REBALANCE_SELL_COMPLETE — upgrades to REBALANCE_PROCESSING on Kite redirect
    elif effective_basket_type == "EXIT":
        strategy.status = LiveStatus.EXIT_PENDING_USER_APPROVAL
    else:
        strategy.status = LiveStatus.PENDING_USER_APPROVAL
        # subscription_active stays False — card appears on portfolio only after
        # first postback fills (set to True in update_from_postback → ACTIVE).
        # Dashboard is still accessible by live_id (no subscription_active check).
    db.commit()
    db.refresh(basket)
    return basket


# (Dead code removed: postback_to_orders_df, update_buy_df_from_orders_df,
#  update_sell_df_from_orders_df, get_circuit_df — all replaced by direct
#  postback handling in update_from_postback and daily celery job.)


def update_tradelog_circuit_df(
    TODAY: date,
    unique_user: str,
    tradelog_df: pd.DataFrame,
    circuit_df: pd.DataFrame,
    today_cash: float,
) -> tuple[pd.DataFrame, float]:
    """Strategy Builder equivalent of equitycase_logic update_tradelog_circuit_df().

    Circuit rows are created from partial fills. For this Publisher flow, this
    function is mostly future-compatible plumbing, but it follows the same cash,
    charges and buy/sell update style as Equitycase.
    """
    if circuit_df is None or circuit_df.empty:
        return tradelog_df, today_cash

    for i, row in circuit_df.iterrows():
        if bool(row.get("updated_in_tradelog")):
            continue
        if not row.get("order_id") or int(row.get("actual_qty") or 0) <= 0:
            continue

        action = str(row.get("action") or "").lower()
        if action == "buy":
            actual_qty = int(row.get("actual_qty") or 0)
            actual_price = float(row.get("actual_price") or 0)
            actual_amount = round(float(row.get("actual_amount") or actual_qty * actual_price), 2)
            buy_charges = round(actual_amount * 0.0011, 2)
            today_cash -= actual_amount

            ltp = actual_price
            current_value = round(actual_qty * ltp, 2)
            unrealised_pnl = round(actual_qty * (ltp - actual_price), 2)
            profit_percent = round(unrealised_pnl / actual_amount * 100, 2) if actual_amount else 0.0
            new_row = {
                "automate_equity_ra_id": row["automate_equity_ra_id"],
                "tradingsymbol": row["tradingsymbol"],
                "isin": row.get("isin", ""),
                "buy_date": TODAY,
                "sell_date": None,
                "hold": 0,
                "weightage": row.get("weightage"),
                "buy_qty": actual_qty,
                "buy_price": actual_price,
                "buy_amount": actual_amount,
                "sell_qty": 0,
                "sell_price": 0.0,
                "sell_amount": 0.0,
                "pyramiding": 1,
                "volatility": row.get("volatility", 0.0),
                "ltp": ltp,
                "stoploss": row.get("stoploss", 0.0),
                "risk": 0.0,
                "risk_percent": 0.0,
                "current_value": current_value,
                "unrealised_pnl": unrealised_pnl,
                "realised_pnl": 0.0,
                "profit_percent": profit_percent,
                "buy_charges": buy_charges,
                "sell_charges": 0.0,
                "active": True,
                "buy_order_id": row.get("order_id"),
                "sell_order_id": None,
                "pyramiding_data": f"{TODAY},{actual_qty},{actual_price};",
                "profit_booking_data": "",
            }
            tradelog_df = pd.concat([tradelog_df, pd.DataFrame([new_row])], ignore_index=True)
            circuit_df.at[i, "updated_in_tradelog"] = True

        elif action == "sell":
            actual_qty = int(row.get("actual_qty") or 0)
            actual_price = float(row.get("actual_price") or 0)
            actual_amount = round(float(row.get("actual_amount") or actual_qty * actual_price), 2)
            sell_charges = round(actual_amount * 0.0021, 2)
            today_cash += actual_amount

            active_idx = tradelog_df.loc[
                (tradelog_df["tradingsymbol"] == row["tradingsymbol"]) &
                (tradelog_df["active"] == True)
            ].index
            if len(active_idx) == 0:
                continue
            idx = active_idx[-1]
            tradelog_df = _apply_equitycase_sell_to_tradelog_row(
                tradelog_df, idx, TODAY, actual_qty, actual_price, actual_amount, sell_charges, row.get("order_id")
            )
            circuit_df.at[i, "updated_in_tradelog"] = True

    return tradelog_df, round(today_cash, 2)


def update_tradelog_buy_df(
    TODAY: date,
    unique_user: str,
    tradelog_df: pd.DataFrame,
    buy_df: pd.DataFrame,
    today_cash: float,
) -> tuple[pd.DataFrame, float]:
    """Same buy-row treatment as equitycase_logic update_tradelog_buy_df()."""
    if buy_df is None or buy_df.empty:
        return tradelog_df, today_cash

    buy_new_rows = []
    cash_out = 0.0
    for i, row in buy_df.iterrows():
        if bool(row.get("updated_in_tradelog")):
            continue
        if not row.get("order_id") or int(row.get("actual_qty") or 0) <= 0:
            continue

        actual_qty = int(row.get("actual_qty") or 0)
        actual_price = float(row.get("actual_price") or 0)
        actual_amount = round(float(row.get("actual_amount") or actual_qty * actual_price), 2)
        buy_charges = round(actual_amount * 0.0011, 2)
        cash_out += actual_amount

        ltp = actual_price
        current_value = round(actual_qty * ltp, 2)
        unrealised_pnl = round(actual_qty * (ltp - actual_price), 2)
        profit_percent = round(unrealised_pnl / actual_amount * 100, 2) if actual_amount else 0.0

        buy_new_rows.append({
            "automate_equity_ra_id": row["automate_equity_ra_id"],
            "tradingsymbol": row["tradingsymbol"],
            "isin": row.get("isin", ""),
            "buy_date": TODAY,
            "sell_date": None,
            "hold": 0,
            "weightage": row.get("weightage"),
            "buy_qty": actual_qty,
            "buy_price": actual_price,
            "buy_amount": actual_amount,
            "sell_qty": 0,
            "sell_price": 0.0,
            "sell_amount": 0.0,
            "pyramiding": 1,
            "volatility": row.get("volatility", 0.0),
            "ltp": ltp,
            "stoploss": row.get("stoploss", 0.0),
            "risk": 0.0,
            "risk_percent": 0.0,
            "current_value": current_value,
            "unrealised_pnl": unrealised_pnl,
            "realised_pnl": 0.0,
            "profit_percent": profit_percent,
            "buy_charges": buy_charges,
            "sell_charges": 0.0,
            "active": True,
            "buy_order_id": row.get("order_id"),
            "sell_order_id": None,
            "pyramiding_data": f"{TODAY},{actual_qty},{actual_price};",
            "profit_booking_data": "",
        })
        buy_df.at[i, "updated_in_tradelog"] = True

    if buy_new_rows:
        tradelog_df = pd.concat([tradelog_df, pd.DataFrame(buy_new_rows)], ignore_index=True)
    today_cash -= cash_out
    return tradelog_df, round(today_cash, 2)


def update_tradelog_pyramiding_df(
    TODAY: date,
    unique_user: str,
    tradelog_df: pd.DataFrame,
    pyramiding_df: Optional[pd.DataFrame],
    today_cash: float,
) -> tuple[pd.DataFrame, float]:
    """Compatibility stub. Strategy Builder investment flow does not use pyramiding now."""
    return tradelog_df, today_cash


def update_tradelog_profit_booking_df(
    TODAY: date,
    unique_user: str,
    tradelog_df: pd.DataFrame,
    profit_booking_df: Optional[pd.DataFrame],
    today_cash: float,
) -> tuple[pd.DataFrame, float]:
    """Compatibility stub. Strategy Builder investment flow does not use profit-booking now."""
    return tradelog_df, today_cash


def _apply_equitycase_sell_to_tradelog_row(
    tradelog_df: pd.DataFrame,
    idx: int,
    TODAY: date,
    today_sell_qty: int,
    today_sell_price: float,
    today_sell_amount: float,
    today_sell_charges: float,
    sell_order_id: Optional[str],
) -> pd.DataFrame:
    """Apply sell using Equitycase's cumulative sell amount / average sell price style."""
    buy_qty = int(tradelog_df.at[idx, "buy_qty"] or 0)
    buy_amount = float(tradelog_df.at[idx, "buy_amount"] or 0.0)

    old_sell_qty = int(tradelog_df.at[idx, "sell_qty"] or 0)
    old_sell_amount = float(tradelog_df.at[idx, "sell_amount"] or 0.0)
    old_sell_charges = float(tradelog_df.at[idx, "sell_charges"] or 0.0)

    new_sell_qty = old_sell_qty + today_sell_qty
    new_sell_amount = round(old_sell_amount + today_sell_amount, 2)
    new_sell_price = round(new_sell_amount / new_sell_qty, 2) if new_sell_qty else 0.0
    new_sell_charges = round(old_sell_charges + today_sell_charges, 2)
    new_hold = int(tradelog_df.at[idx, "hold"] or 0) + 1

    old_realised_pnl = float(tradelog_df.at[idx, "realised_pnl"] or 0.0)
    # Keep Equitycase style exactly: cumulative sell amount - original buy amount.
    new_realised_pnl = new_sell_amount - buy_amount
    realised_pnl = round(old_realised_pnl + new_realised_pnl, 2)
    profit_percent = round(realised_pnl / buy_amount * 100, 2) if buy_amount else 0.0

    tradelog_df.at[idx, "hold"] = new_hold
    tradelog_df.at[idx, "sell_qty"] = new_sell_qty
    tradelog_df.at[idx, "sell_price"] = new_sell_price
    tradelog_df.at[idx, "sell_amount"] = new_sell_amount
    tradelog_df.at[idx, "realised_pnl"] = realised_pnl
    tradelog_df.at[idx, "profit_percent"] = profit_percent
    tradelog_df.at[idx, "sell_charges"] = new_sell_charges

    old_profit_booking_data = tradelog_df.at[idx, "profit_booking_data"]
    old_profit_booking_data = "" if pd.isna(old_profit_booking_data) or old_profit_booking_data is None else str(old_profit_booking_data)
    tradelog_df.at[idx, "profit_booking_data"] = old_profit_booking_data + f"{TODAY},-{today_sell_qty},{today_sell_price};"

    if buy_qty == new_sell_qty:
        tradelog_df.at[idx, "active"] = False
        tradelog_df.at[idx, "sell_date"] = TODAY
        tradelog_df.at[idx, "risk"] = 0.0
        tradelog_df.at[idx, "risk_percent"] = 0.0
        tradelog_df.at[idx, "current_value"] = 0.0
        tradelog_df.at[idx, "unrealised_pnl"] = 0.0
        tradelog_df.at[idx, "sell_order_id"] = sell_order_id
    return tradelog_df


def update_tradelog_sell_df(
    TODAY: date,
    unique_user: str,
    tradelog_df: pd.DataFrame,
    sell_df: pd.DataFrame,
    today_cash: float,
) -> tuple[pd.DataFrame, float]:
    """Same sell-row treatment as equitycase_logic update_tradelog_sell_df()."""
    if sell_df is None or sell_df.empty:
        return tradelog_df, today_cash

    cash_in = 0.0
    for i, row in sell_df.iterrows():
        if bool(row.get("updated_in_tradelog")):
            continue
        if not row.get("order_id") or int(row.get("actual_qty") or 0) <= 0:
            continue

        actual_qty = int(row.get("actual_qty") or 0)
        actual_price = float(row.get("actual_price") or 0)
        actual_amount = round(float(row.get("actual_amount") or actual_qty * actual_price), 2)
        sell_charges = round(actual_amount * 0.0021, 2)
        cash_in += actual_amount

        active_idx = tradelog_df.loc[
            (tradelog_df["tradingsymbol"] == row["tradingsymbol"]) &
            (tradelog_df["active"] == True)
        ].index
        if len(active_idx) == 0:
            continue
        idx = active_idx[-1]
        tradelog_df = _apply_equitycase_sell_to_tradelog_row(
            tradelog_df, idx, TODAY, actual_qty, actual_price, actual_amount, sell_charges, row.get("order_id")
        )
        sell_df.at[i, "updated_in_tradelog"] = True

    today_cash += cash_in
    return tradelog_df, round(today_cash, 2)


def _safe_latest_symbol_price(TODAY: date, tradingsymbol: str, fallback: float = 0.0) -> float:
    """Latest close/LTP fallback from shared OHLC source for active holding MTM."""
    try:
        temp_df = pd.read_sql_table(tradingsymbol, equity_engine)
        if temp_df.empty:
            return float(fallback or 0.0)
        if "date" in temp_df.columns:
            temp_df["date"] = pd.to_datetime(temp_df["date"]).dt.date
            temp_df = temp_df.loc[temp_df["date"] <= TODAY]
        if temp_df.empty or "close" not in temp_df.columns:
            return float(fallback or 0.0)
        return float(temp_df["close"].iloc[-1])
    except Exception:
        return float(fallback or 0.0)


def update_tradelog_ltp(
    TODAY: date,
    unique_user: str,
    tradelog_df: pd.DataFrame,
) -> pd.DataFrame:
    """Equitycase-style daily MTM refresh for all active holdings."""
    if tradelog_df is None or tradelog_df.empty:
        return tradelog_df

    active_idx = tradelog_df.loc[tradelog_df["active"] == True].index
    for i in active_idx:
        tradingsymbol = tradelog_df.at[i, "tradingsymbol"]
        fallback = float(tradelog_df.at[i, "ltp"] or tradelog_df.at[i, "buy_price"] or 0.0)
        ltp = _safe_latest_symbol_price(TODAY, tradingsymbol, fallback)

        buy_qty = int(tradelog_df.at[i, "buy_qty"] or 0)
        buy_amount = float(tradelog_df.at[i, "buy_amount"] or 0.0)
        sell_qty = int(tradelog_df.at[i, "sell_qty"] or 0)
        realised_pnl = float(tradelog_df.at[i, "realised_pnl"] or 0.0)
        remaining_qty = buy_qty - sell_qty

        hold = int(tradelog_df.at[i, "hold"] or 0) + 1
        current_value = round(remaining_qty * ltp, 2)
        unrealised_pnl = round(current_value - buy_amount, 2)
        profit_percent = round((unrealised_pnl + realised_pnl) / buy_amount * 100, 2) if buy_amount else 0.0

        tradelog_df.at[i, "hold"] = hold
        tradelog_df.at[i, "ltp"] = ltp
        tradelog_df.at[i, "current_value"] = current_value
        tradelog_df.at[i, "unrealised_pnl"] = unrealised_pnl
        tradelog_df.at[i, "profit_percent"] = profit_percent

    return tradelog_df


def update_tradelog(
    TODAY: date,
    unique_user: str,
    tradelog_df: pd.DataFrame,
    buy_df: pd.DataFrame,
    sell_df: pd.DataFrame,
    circuit_df: pd.DataFrame,
    today_cash: float = 0.0,
) -> tuple[pd.DataFrame, float]:
    """Equitycase-compatible call chain for Strategy Builder live investment.

    The returned today_cash is the day's cash delta only, exactly like
    equitycase_logic. update_equitycurve() adds it to yesterday's cash.
    """
    if tradelog_df is None or tradelog_df.empty:
        tradelog_df = pd.DataFrame(columns=[c.name for c in LiveTradelog.__table__.columns])

    today_cash = 0.0 if today_cash is None else float(today_cash)
    tradelog_df, today_cash = update_tradelog_circuit_df(TODAY, unique_user, tradelog_df, circuit_df, today_cash)
    tradelog_df, today_cash = update_tradelog_buy_df(TODAY, unique_user, tradelog_df, buy_df, today_cash)
    tradelog_df, today_cash = update_tradelog_pyramiding_df(TODAY, unique_user, tradelog_df, None, today_cash)
    tradelog_df, today_cash = update_tradelog_profit_booking_df(TODAY, unique_user, tradelog_df, None, today_cash)
    tradelog_df, today_cash = update_tradelog_sell_df(TODAY, unique_user, tradelog_df, sell_df, today_cash)
    tradelog_df = update_tradelog_ltp(TODAY, unique_user, tradelog_df)
    return tradelog_df, round(today_cash, 2)


def make_equitycurve_table(TODAY: date, user: LiveStrategy) -> pd.DataFrame:
    index_price = _safe_latest_index_price(TODAY)
    return pd.DataFrame([{
        "automate_equity_ra_id": user.id,
        "date": TODAY,
        "total_days": 0,
        "portfolio_size": 0,
        "stocks_value": 0.0,
        "cash": user.initial_aum,
        "aum": user.initial_aum,
        "index_price": index_price,
        "strategy_roc": 0.0,
        "index_roc": 0.0,
        "strategy_daily_return": 0.0,
        "index_daily_return": 0.0,
        "strategy_daily_performance": 0.0,
        "index_daily_performance": 0.0,
        "compare": False,
        "unrealised_pnl": 0.0,
        "realised_pnl": 0.0,
        "total_pnl": 0.0,
        "winning_trades": 0,
        "losing_trades": 0,
        "total_trades": 0,
        "winning_percent": 0.0,
        "losing_percent": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "rr": 0.0,
        "profit_factor": 0.0,
        "biggest_winning_trade": 0.0,
        "biggest_losing_trade": 0.0,
        "expectancy": 0.0,
        "avg_profit_per_day": 0.0,
        "max_dd_percent": 0.0,
        "max_dd_absolute": 0.0,
        "current_dd_percent": 0.0,
        "sqn": 0.0,
        "k_multiple": 0.0,
        "sharpe": 0.0,
        "calmar": 0.0,
        "sortino_ratio": 0.0,
        "equitycurve_percent": 100.0,
        "cagr_percent": 0.0,
        "neg_2sd": 0.0,
        "equitycurve_avg": 0.0,
        "pos_2sd": 0.0,
        "total_charges": 0.0,
        "rebalance": False,
        "weekly_return": 0.0,
        "monthly_return": 0.0,
        "quarterly_return": 0.0,
        "yearly_return": 0.0,
        "benchmark_price": _safe_benchmark_price(TODAY, _get_benchmark_index_name(user)),
        "benchmark_roc": 0.0,
        "benchmark_daily_return": 0.0,
        "benchmark_daily_performance": 0.0,
    }])


def _safe_latest_index_price(TODAY: date) -> float:
    """Return NIFTY 50 latest/today close using the same common OHLC source.

    This is the Strategy Builder equivalent of equitycase_logic reading the
    "NIFTY 50" table from ohlc_engine inside update_equitycurve(). If the
    index table is unavailable, we return 0.0 and keep index metrics neutral
    rather than breaking user postback processing.
    """
    try:
        temp_index_df = pd.read_sql_table("NIFTY 50", equity_engine)
        if temp_index_df.empty or "close" not in temp_index_df.columns:
            return 0.0
        if "date" in temp_index_df.columns:
            temp_index_df["date"] = pd.to_datetime(temp_index_df["date"]).dt.date
            today_rows = temp_index_df.loc[temp_index_df["date"] == TODAY]
            if not today_rows.empty:
                return float(today_rows["close"].iloc[-1])
        return float(temp_index_df["close"].iloc[-1])
    except Exception:
        return 0.0


def _get_benchmark_index_name(strategy: "LiveStrategy") -> str:
    """Extract the user-selected index name from the strategy's universe_json.

    universe_json is like: {"type": "index", "value": "NIFTY 500"}
    If type != "index" or value is missing/empty, default to "NIFTY 50".
    """
    uj = strategy.universe_json
    if not uj or not isinstance(uj, dict):
        return "NIFTY 50"
    if uj.get("type") != "index" or not uj.get("value"):
        return "NIFTY 50"
    return uj["value"]


def _safe_benchmark_price(TODAY: date, index_name: str) -> float:
    """Return the benchmark index close price, same pattern as _safe_latest_index_price.

    Reads from the equity_engine table named by index_name (e.g. "NIFTY 500").
    Falls back to 0.0 if unavailable.
    """
    if not index_name or index_name == "NIFTY 50":
        # Same as the main index — reuse that function to avoid duplicate reads
        return _safe_latest_index_price(TODAY)
    try:
        from app.core.benchmark_registry import resolve_benchmark_table
        table_name = resolve_benchmark_table(index_name)
        temp_df = pd.read_sql_table(table_name, equity_engine)
        if temp_df.empty or "close" not in temp_df.columns:
            return 0.0
        if "date" in temp_df.columns:
            temp_df["date"] = pd.to_datetime(temp_df["date"]).dt.date
            today_rows = temp_df.loc[temp_df["date"] == TODAY]
            if not today_rows.empty:
                return float(today_rows["close"].iloc[-1])
        return float(temp_df["close"].iloc[-1])
    except Exception:
        return 0.0


def update_equitycurve(
    TODAY: date,
    user: LiveStrategy,
    tradelog_df: pd.DataFrame,
    equitycurve_df: pd.DataFrame,
    today_cash: float,
    rebalance: bool,
) -> pd.DataFrame:
    """Equitycase-style update_equitycurve(), adapted only for Strategy Builder tables.

    This intentionally mirrors equitycase_logic/strategy_equity/common_equity_functions.py
    update_equitycurve(): same metrics, same cash/charges treatment, same drawdown,
    CAGR, Sharpe/Calmar/Sortino, weekly/monthly/quarterly/yearly return logic.

    Difference from equitycase_logic:
    - No broker order book is fetched here. Publisher postbacks have already updated
      buy/sell/tradelog rows before this function is called.
    - Index price is read from the shared equity_engine "NIFTY 50" table.
    """
    if equitycurve_df is None or equitycurve_df.empty:
        equitycurve_df = make_equitycurve_table(TODAY, user)

    # Work on a copy because we temporarily convert date dtype for period grouping.
    equitycurve_df = equitycurve_df.copy()

    total_days = int(equitycurve_df["total_days"].iloc[-1] or 0) + 1
    active_mask = tradelog_df["active"] == True if not tradelog_df.empty and "active" in tradelog_df.columns else []
    portfolio_size = int(active_mask.sum()) if len(active_mask) else 0

    stocks_value = round(float(tradelog_df["current_value"].sum()), 2) if not tradelog_df.empty and "current_value" in tradelog_df.columns else 0.0

    yesterday_total_charges = float(equitycurve_df["total_charges"].iloc[-1] or 0.0) if "total_charges" in equitycurve_df.columns else 0.0
    buy_charges = float(tradelog_df["buy_charges"].sum()) if not tradelog_df.empty and "buy_charges" in tradelog_df.columns else 0.0
    sell_charges = float(tradelog_df["sell_charges"].sum()) if not tradelog_df.empty and "sell_charges" in tradelog_df.columns else 0.0
    today_total_charges = round(buy_charges + sell_charges, 2)
    today_charges = today_total_charges - yesterday_total_charges

    # Same equitycase rule: charges are deducted only from cash.
    previous_cash = float(equitycurve_df["cash"].iloc[-1] or 0.0)
    cash = round(previous_cash + today_cash - today_charges, 2)
    aum = round(stocks_value + cash, 2)

    initial_aum = float(equitycurve_df["aum"].iloc[0] or user.initial_aum or 0.0)
    yesterday_aum = float(equitycurve_df["aum"].iloc[-1] or initial_aum or 0.0)

    index_price = _safe_latest_index_price(TODAY)
    first_index_price = float(equitycurve_df["index_price"].iloc[0] or 0.0) if "index_price" in equitycurve_df.columns else 0.0
    previous_index_price = float(equitycurve_df["index_price"].iloc[-1] or 0.0) if "index_price" in equitycurve_df.columns else 0.0

    strategy_roc = round((aum - initial_aum) / initial_aum * 100, 2) if initial_aum else 0.0
    index_roc = round((index_price - first_index_price) / first_index_price * 100, 2) if first_index_price else 0.0

    strategy_daily_return = round(aum - yesterday_aum, 2)
    index_daily_return = round(index_price - previous_index_price, 2) if previous_index_price else 0.0

    strategy_daily_performance = round((aum - yesterday_aum) / yesterday_aum * 100, 2) if yesterday_aum else 0.0
    index_daily_performance = round((index_price - previous_index_price) / previous_index_price * 100, 2) if previous_index_price else 0.0
    compare = 1 if strategy_daily_performance > index_daily_performance else 0

    # ── Benchmark (user-selected index) ──────────────────────────────────
    benchmark_index_name = _get_benchmark_index_name(user)
    benchmark_price = _safe_benchmark_price(TODAY, benchmark_index_name)
    first_benchmark_price = float(equitycurve_df["benchmark_price"].iloc[0] or 0.0) if "benchmark_price" in equitycurve_df.columns else 0.0
    previous_benchmark_price = float(equitycurve_df["benchmark_price"].iloc[-1] or 0.0) if "benchmark_price" in equitycurve_df.columns else 0.0
    benchmark_roc = round((benchmark_price - first_benchmark_price) / first_benchmark_price * 100, 2) if first_benchmark_price else 0.0
    benchmark_daily_return = round(benchmark_price - previous_benchmark_price, 2) if previous_benchmark_price else 0.0
    benchmark_daily_performance = round((benchmark_price - previous_benchmark_price) / previous_benchmark_price * 100, 2) if previous_benchmark_price else 0.0

    total_unrealised_pnl = round(float(tradelog_df["unrealised_pnl"].sum()), 2) if not tradelog_df.empty and "unrealised_pnl" in tradelog_df.columns else 0.0
    total_realised_pnl = round(float(tradelog_df["realised_pnl"].sum()), 2) if not tradelog_df.empty and "realised_pnl" in tradelog_df.columns else 0.0
    total_pnl = round(total_unrealised_pnl + total_realised_pnl - today_total_charges, 2)

    if not tradelog_df.empty:
        unrealised = tradelog_df["unrealised_pnl"] if "unrealised_pnl" in tradelog_df.columns else 0.0
        realised = tradelog_df["realised_pnl"] if "realised_pnl" in tradelog_df.columns else 0.0
        combined_pnl = unrealised + realised
    else:
        combined_pnl = pd.Series(dtype="float64")

    winning_trades = int((combined_pnl > 0.0).sum())
    losing_trades = int((combined_pnl <= 0.0).sum()) if not combined_pnl.empty else 0
    total_trades = winning_trades + losing_trades

    win_percent = round((winning_trades / total_trades) * 100, 2) if total_trades > 0 else 0.0
    loss_percent = round((losing_trades / total_trades) * 100, 2) if total_trades > 0 else 0.0

    pos_series = combined_pnl[combined_pnl > 0.0]
    avg_win = round(float(pos_series.mean()), 2) if not pos_series.empty else 0.0

    neg_series = combined_pnl[combined_pnl <= 0.0]
    avg_loss = round(float(neg_series.mean()), 2) if not neg_series.empty else 0.0

    rr = round(((avg_win / avg_loss) * -1), 2) if avg_loss != 0 else 0.0
    profit_factor = round(((winning_trades * avg_win) / (losing_trades * avg_loss) * -1), 2) if avg_loss != 0 and losing_trades != 0 else 0.0

    biggest_winning_trade = round(float(pos_series.max()), 2) if not pos_series.empty else 0.0
    biggest_losing_trade = round(float(neg_series.min()), 2) if not neg_series.empty else 0.0

    expectancy = round((avg_win * win_percent) - (abs(avg_loss) * loss_percent), 2)
    avg_profit_per_day = round((total_pnl / total_days), 2) if total_days != 0 else 0.0

    base_equitycurve_percent = float(equitycurve_df["equitycurve_percent"].iloc[0] or 100.0)
    equitycurve_percent = round((aum * base_equitycurve_percent / initial_aum), 2) if initial_aum else base_equitycurve_percent

    first_date = pd.to_datetime(equitycurve_df["date"].iloc[0]).date()
    total_calender_day = (TODAY - first_date).days + 1
    cagr_percent = round(((((equitycurve_percent / base_equitycurve_percent) ** (1 / (total_calender_day / 365))) - 1) * 100), 2) if total_calender_day > 0 and base_equitycurve_percent else 0.0

    equity_list = equitycurve_df["equitycurve_percent"].fillna(base_equitycurve_percent).tolist()
    equity_list.append(equitycurve_percent)
    equity_series = np.array(equity_list, dtype=float)
    peak_equity = float(max(equity_series)) if len(equity_series) else equitycurve_percent
    dd = round((equitycurve_percent - peak_equity) / peak_equity * 100, 2) if peak_equity else 0.0
    previous_max_dd = float(equitycurve_df["current_dd_percent"].min() or 0.0) if "current_dd_percent" in equitycurve_df.columns else 0.0
    max_dd_percent = round(min(previous_max_dd, dd), 2)

    try:
        dd_aum = float(equitycurve_df.loc[equitycurve_df["current_dd_percent"] == 0, "aum"].values[-1])
    except Exception:
        dd_aum = initial_aum
    last_max_dd_percent = float(equitycurve_df["max_dd_percent"].iloc[-1] or 0.0) if "max_dd_percent" in equitycurve_df.columns else 0.0
    if max_dd_percent < last_max_dd_percent:
        max_dd_absolute = round(max_dd_percent * dd_aum / 100, 2)
    else:
        max_dd_absolute = float(equitycurve_df["max_dd_absolute"].iloc[-1] or 0.0) if "max_dd_absolute" in equitycurve_df.columns else 0.0

    sqn = 0.0
    k_multiple = 0.0

    equity_series_std = float(np.std(equity_series)) if len(equity_series) else 0.0
    sharpe = round((equitycurve_percent - 100) / equity_series_std, 2) if equity_series_std != 0 else 0.0
    calmar = round(cagr_percent / abs(max_dd_percent), 2) if max_dd_percent != 0 else 0.0
    average_trade = round((total_pnl / total_trades), 2) if total_trades != 0 else 0.0

    downside_deviation = equitycurve_df[equitycurve_df["strategy_daily_return"] < 0]["strategy_daily_return"].std() if "strategy_daily_return" in equitycurve_df.columns else 0.0
    sortino_ratio = round((average_trade / downside_deviation), 5) if downside_deviation and not pd.isna(downside_deviation) else 0.0

    equitycurve_avg = round(float(equity_series.mean()), 2) if len(equity_series) else 0.0
    neg_sd = round(equitycurve_avg - equity_series_std * 2, 2)
    pos_sd = round(equitycurve_avg + equity_series_std * 2, 2)

    eq_for_periods = equitycurve_df.copy()
    eq_for_periods["date"] = pd.to_datetime(eq_for_periods["date"])
    first_date_aum = float(eq_for_periods["aum"].iloc[0] or initial_aum or 0.0)
    last_week_dates = eq_for_periods.groupby(eq_for_periods["date"].dt.to_period("W-SUN"))["date"].last()
    last_month_dates = eq_for_periods.groupby(eq_for_periods["date"].dt.to_period("M"))["date"].last()
    last_quarter_dates = eq_for_periods.groupby(eq_for_periods["date"].dt.to_period("Q"))["date"].last()
    last_year_dates = eq_for_periods.groupby(eq_for_periods["date"].dt.to_period("Y"))["date"].last()

    current_date = pd.Timestamp(TODAY)
    current_week = current_date.to_period("W-SUN")
    current_month = current_date.to_period("M")
    current_quarter = current_date.to_period("Q")
    current_year = current_date.to_period("Y")

    def calculate_return(current_aum, last_date, default_aum):
        if pd.notna(last_date):
            matched = eq_for_periods.loc[eq_for_periods["date"] == last_date, "aum"]
            if not matched.empty and float(matched.values[0] or 0.0) != 0:
                last_aum = float(matched.values[0])
                return ((current_aum - last_aum) / last_aum) * 100
        return ((current_aum - default_aum) / default_aum) * 100 if default_aum else 0.0

    weekly_return = round(calculate_return(aum, last_week_dates.get(current_week - 1, pd.NaT), first_date_aum), 2)
    monthly_return = round(calculate_return(aum, last_month_dates.get(current_month - 1, pd.NaT), first_date_aum), 2)
    quarterly_return = round(calculate_return(aum, last_quarter_dates.get(current_quarter - 1, pd.NaT), first_date_aum), 2)
    yearly_return = round(calculate_return(aum, last_year_dates.get(current_year - 1, pd.NaT), first_date_aum), 2)

    new_row = {
        "automate_equity_ra_id": user.id,
        "date": TODAY,
        "total_days": total_days,
        "portfolio_size": portfolio_size,
        "stocks_value": stocks_value,
        "cash": cash,
        "aum": aum,
        "index_price": index_price,
        "strategy_roc": strategy_roc,
        "index_roc": index_roc,
        "strategy_daily_return": strategy_daily_return,
        "index_daily_return": index_daily_return,
        "strategy_daily_performance": strategy_daily_performance,
        "index_daily_performance": index_daily_performance,
        "compare": compare,
        "unrealised_pnl": total_unrealised_pnl,
        "realised_pnl": total_realised_pnl,
        "total_pnl": total_pnl,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "total_trades": total_trades,
        "winning_percent": win_percent,
        "losing_percent": loss_percent,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "rr": rr,
        "profit_factor": profit_factor,
        "biggest_winning_trade": biggest_winning_trade,
        "biggest_losing_trade": biggest_losing_trade,
        "expectancy": expectancy,
        "avg_profit_per_day": avg_profit_per_day,
        "max_dd_percent": max_dd_percent,
        "max_dd_absolute": max_dd_absolute,
        "current_dd_percent": dd,
        "sqn": sqn,
        "k_multiple": k_multiple,
        "sharpe": sharpe,
        "calmar": calmar,
        "sortino_ratio": sortino_ratio,
        "equitycurve_percent": equitycurve_percent,
        "cagr_percent": cagr_percent,
        "neg_2sd": neg_sd,
        "equitycurve_avg": equitycurve_avg,
        "pos_2sd": pos_sd,
        "total_charges": today_total_charges,
        "rebalance": rebalance,
        "weekly_return": weekly_return,
        "monthly_return": monthly_return,
        "quarterly_return": quarterly_return,
        "yearly_return": yearly_return,
        "benchmark_price": benchmark_price,
        "benchmark_roc": benchmark_roc,
        "benchmark_daily_return": benchmark_daily_return,
        "benchmark_daily_performance": benchmark_daily_performance,
    }

    return pd.concat([equitycurve_df, pd.DataFrame([new_row])], ignore_index=True)


def _delete_unapproved_preview_rows(db: Session, strategy_id, *, include_sell: bool = False) -> None:
    """Remove rows that are not approved/executed yet, plus terminal rejected/cancelled
    rows with no fills (to avoid pile-up on retry)."""
    # 1. Delete unapproved preview rows (never sent to broker)
    db.query(LiveBuyStock).filter(
        LiveBuyStock.automate_equity_ra_id == strategy_id,
        or_(LiveBuyStock.order_id.is_(None), LiveBuyStock.order_id == ""),
        or_(LiveBuyStock.actual_qty.is_(None), LiveBuyStock.actual_qty == 0),
        LiveBuyStock.updated_in_tradelog == False,
    ).delete(synchronize_session=False)

    # 2. Delete rejected/cancelled rows with no fills (terminal, worthless on retry)
    db.query(LiveBuyStock).filter(
        LiveBuyStock.automate_equity_ra_id == strategy_id,
        LiveBuyStock.broker_status.in_(["REJECTED", "CANCELLED"]),
        or_(LiveBuyStock.actual_qty.is_(None), LiveBuyStock.actual_qty == 0),
    ).delete(synchronize_session=False)

    if include_sell:
        db.query(LiveSellStock).filter(
            LiveSellStock.automate_equity_ra_id == strategy_id,
            or_(LiveSellStock.order_id.is_(None), LiveSellStock.order_id == ""),
            or_(LiveSellStock.actual_qty.is_(None), LiveSellStock.actual_qty == 0),
            LiveSellStock.updated_in_tradelog == False,
        ).delete(synchronize_session=False)

        db.query(LiveSellStock).filter(
            LiveSellStock.automate_equity_ra_id == strategy_id,
            LiveSellStock.broker_status.in_(["REJECTED", "CANCELLED"]),
            or_(LiveSellStock.actual_qty.is_(None), LiveSellStock.actual_qty == 0),
        ).delete(synchronize_session=False)

    # 3. Cancel any leftover PENDING_USER_APPROVAL baskets whose rows we just deleted.
    # Without this, a re-prepared rebalance leaves an orphaned PENDING basket behind,
    # which then trips the double-click guard in assign_publisher_tags and silently
    # blocks the next Sell/Buy click (status never advances, no tags assigned).
    # Callers reach this only after confirming there are no in-flight/filled orders.
    db.query(LivePublisherBasket).filter(
        LivePublisherBasket.automate_equity_ra_id == strategy_id,
        LivePublisherBasket.status == "PENDING_USER_APPROVAL",
    ).update({"status": "CANCELLED"}, synchronize_session=False)

    db.commit()


def _validate_trade_now_status(strategy: LiveStrategy, basket_type: str, side: str = "ALL") -> None:
    # Block ALL trade_now calls during _PROCESSING — orders are in-flight on Kite
    PROCESSING_STATUSES = {
        LiveStatus.INITIAL_PROCESSING,
        LiveStatus.REBALANCE_PROCESSING,
        LiveStatus.EXIT_PROCESSING,
    }
    if strategy.status in PROCESSING_STATUSES:
        raise HTTPException(status_code=400, detail="Your orders are being processed by the broker. Please wait for processing to complete.")

    if basket_type == "REBALANCE" and side == "BUY":
        expected = {LiveStatus.REBALANCE_SELL_COMPLETE}
    elif basket_type == "REBALANCE" and side == "SELL":
        expected = {LiveStatus.REBALANCE_READY, LiveStatus.REBALANCE_PENDING_USER_APPROVAL}
    else:
        expected = {
            "INITIAL": {LiveStatus.PREVIEW_READY, LiveStatus.ALL_REJECTED},
            "REBALANCE": {LiveStatus.REBALANCE_READY},
            "EXIT": {LiveStatus.ACTIVE, LiveStatus.EXIT_PENDING_USER_APPROVAL},
        }.get((basket_type or "INITIAL").upper(), set())
    if expected and strategy.status not in expected:
        current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
        expected_str = ", ".join(s.value for s in expected)
        raise HTTPException(status_code=400, detail=f"Cannot create {basket_type} basket when status={current}. Expected one of: {expected_str}.")


# -----------------------------------------------------------------------------
# Same-day self-heal for strategies stuck in *_PROCESSING (lost/late postback)
# -----------------------------------------------------------------------------

# How long a strategy may sit in a *_PROCESSING state before a preview page load
# proactively reconciles it against the broker orderbook.
PROCESSING_STALE_SECONDS = 180

_PROCESSING_STATUSES = {
    LiveStatus.INITIAL_PROCESSING,
    LiveStatus.REBALANCE_PROCESSING,
    LiveStatus.EXIT_PROCESSING,
}


def _reconcile_stale_processing(db: Session, strategy: LiveStrategy) -> LiveStrategy:
    """Self-heal a strategy stuck in *_PROCESSING because of a lost/late postback.

    Normally a strategy leaves *_PROCESSING when the broker postback arrives and the
    debounced orderbook-verify task processes it. If that postback is lost, nothing
    unlocks the strategy until the 16:30 safety-net cron — the user is frozen on a
    spinner for hours.

    This runs on the preview page load: if the strategy has been in *_PROCESSING for
    more than PROCESSING_STALE_SECONDS, it triggers the SAME reconciliation the cron
    uses (verify_and_process_from_orderbook), which asks the broker orderbook what
    actually happened and advances the state from the truth.

    Safety: this never blindly resets status. It only advances based on real
    kite.orders() data, so it cannot cause duplicate orders. Debounced via Redis
    (60s) so repeated refreshes don't hammer the broker API. Best-effort — any
    failure is swallowed and the strategy is returned unchanged.
    """
    if strategy.status not in _PROCESSING_STATUSES:
        return strategy

    updated_at = strategy.updated_at
    if updated_at is None:
        return strategy
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - updated_at).total_seconds()
    if age_seconds < PROCESSING_STALE_SECONDS:
        return strategy

    # Debounce: at most one reconcile per strategy per 60s window.
    try:
        is_first = LiveInvestmentService._redis_client.set(
            f"preview_reconcile:{strategy.id}", "1", nx=True, ex=60
        )
        if not is_first:
            return strategy
    except Exception:
        pass  # Redis unavailable — proceed anyway (better to reconcile than stay stuck)

    basket = db.query(LivePublisherBasket).filter(
        LivePublisherBasket.automate_equity_ra_id == strategy.id,
    ).order_by(LivePublisherBasket.created_at.desc()).first()
    if not basket:
        return strategy

    try:
        logger.info("[PreviewReconcile] Strategy %s stuck in %s for %.0fs — reconciling basket %s from orderbook",
                    strategy.id, strategy.status.value, age_seconds, basket.id)
        LiveInvestmentService.verify_and_process_from_orderbook(db, str(basket.id))
        db.refresh(strategy)
    except Exception:
        logger.exception("[PreviewReconcile] Reconcile failed for strategy %s — leaving as-is", strategy.id)
        db.rollback()

    return strategy


def _lock_to_processing(db: Session, strategy: LiveStrategy, processing_status: LiveStatus, reason: str) -> LiveStrategy:
    """Orders are in-flight but the strategy status hasn't caught up yet — e.g. the user
    returned from Kite but the redirect callback was skipped (broker already connected
    today), or a postback set order data before the status upgraded.

    Instead of raising a 400 — which the frontend renders as a dead "Bad Request" page —
    upgrade the strategy to its *_PROCESSING status and return it, so the preview renders
    read-only with a spinner and disabled buttons. The debounced orderbook-verify /
    _reconcile_stale_processing path then advances it to the real terminal state.
    """
    if strategy.status != processing_status:
        logger.info("[PreviewLock] Strategy %s %s → %s (%s)",
                    strategy.id, strategy.status.value, processing_status.value, reason)
        strategy.status = processing_status
        db.commit()
        db.refresh(strategy)
    return _reconcile_stale_processing(db, strategy)


# -----------------------------------------------------------------------------
# Notification helpers
# -----------------------------------------------------------------------------

def _gather_and_notify_rebalance(db: Session, strategy: LiveStrategy, TODAY: date) -> None:
    """
    Gather rebalance details from buy/sell tables and fire notification.
    This is fire-and-forget — failures are logged but never raised.

    User email is fetched from the equitycase DB (separate from screener_backtest_db)
    because on the server the users table lives there.
    """
    try:
        # Fetch user email from equitycase DB (separate database)
        ec_db = EquitycaseSessionLocal()
        try:
            row = ec_db.execute(
                text('SELECT email FROM "user" WHERE id = :user_id LIMIT 1'),
                {"user_id": str(strategy.user_id)},
            ).fetchone()
        finally:
            ec_db.close()

        if not row or not row.email:
            logger.warning("[Notify] No user/email found in equitycase DB for user_id %s (strategy %s) — skipping notification", strategy.user_id, strategy.id)
            return

        user_email = row.email
        user_name = "Investor"  # Fallback since we only pull email now

        # Sells = stocks being REMOVED in this rebalance
        sells = db.query(LiveSellStock).filter(
            LiveSellStock.automate_equity_ra_id == strategy.id,
            LiveSellStock.updated_in_tradelog == False,
        ).all()

        # Buys = stocks being ADDED in this rebalance
        buys = db.query(LiveBuyStock).filter(
            LiveBuyStock.automate_equity_ra_id == strategy.id,
            LiveBuyStock.updated_in_tradelog == False,
            or_(LiveBuyStock.order_id.is_(None), LiveBuyStock.order_id == ""),
        ).all()

        changes = []
        for sell in sells:
            changes.append({
                "tradingsymbol": sell.tradingsymbol,
                "action": "SELL",
                "qty": int(sell.qty),
            })
        for buy in buys:
            changes.append({
                "tradingsymbol": buy.tradingsymbol,
                "action": "BUY",
                "qty": int(buy.qty),
            })

        if not changes:
            logger.info("[Notify] No buy/sell changes for strategy %s — sending empty notification", strategy.id)

        ist = pytz.timezone("Asia/Kolkata")
        
        # Execution date from DB — show "Today" if email is sent on the execution day itself
        if strategy.next_rebalance_date and strategy.next_rebalance_date == TODAY:
            rebalance_date = "Today"
        elif strategy.next_rebalance_date:
            rebalance_date = strategy.next_rebalance_date.strftime("%d %b %Y")
        else:
            rebalance_date = ""
        timestamp = datetime.now(ist).strftime("%d %b %Y, %I:%M %p IST")

        dashboard_url = f"{settings.FRONTEND_BASE_URL}/live-investment/{strategy.id}"

        notify_all(
            "send_rebalance_ready",
            user_email=user_email,
            user_name=user_name,
            strategy_name=strategy.strategy_name or "Unnamed Strategy",
            strategy_id=str(strategy.id),
            changes=changes,
            dashboard_url=dashboard_url,
            timestamp=timestamp,
            rebalance_date=rebalance_date,
        )
    except Exception:
        logger.exception("[Notify] Rebalance notification failed for strategy %s — non-blocking", strategy.id)


# -----------------------------------------------------------------------------
# Orderbook/postback order processing helper
# -----------------------------------------------------------------------------

def _process_basket_orders(
    db: Session,
    strategy: LiveStrategy,
    basket: LivePublisherBasket,
    orders_data: dict,
    basket_tags: set,
    source: str,
    TODAY: date,
) -> None:
    """Bulk update order rows and transition strategy status.

    This is the shared processing logic used by both:
    - verify_and_process_from_orderbook (primary path, from kite.orders() data)
    - Fallback path (from stored broker_raw_postback data)

    Args:
        db: Database session
        strategy: The LiveStrategy being processed
        basket: The LivePublisherBasket with the order tags
        orders_data: Dict mapping publisher_tag → order dict.
                     Order dict has: tag, order_id, status, filled_quantity,
                     average_price, tradingsymbol, transaction_type, placed_by, status_message
        basket_tags: Set of publisher_tags for this basket
        source: "orderbook" or "postback_fallback" (for logging)
        TODAY: Current date
    """
    TERMINAL_STATUSES = {"COMPLETE", "REJECTED", "CANCELLED"}

    # ── Guard: skip already-completed baskets (idempotency) ──
    if basket.status in ("COMPLETE", "ALL_REJECTED"):
        logger.info("[ProcessOrders] Basket %s already %s — skipping re-process | source=%s",
                    basket.id, basket.status, source)
        return

    # ── Guard: don't revert EXITED strategies ──
    if strategy.status == LiveStatus.EXITED:
        logger.info("[ProcessOrders] Strategy %s already EXITED — skipping | source=%s",
                    strategy.id, source)
        return

    # ── Step 1: Bulk update each order row from the order data ──
    for tag, order in orders_data.items():
        if tag not in basket_tags:
            continue

        status = (order.get("status") or "").upper()
        if status == "CANCEL":
            status = "CANCELLED"

        filled_qty = int(order.get("filled_quantity") or 0)
        avg_price = float(order.get("average_price") or 0)
        kite_order_id = str(order.get("order_id") or "")

        # Find the order row by tag
        row_obj = db.query(LiveBuyStock).filter(LiveBuyStock.publisher_tag == tag).first()
        order_table = "buy" if row_obj else None
        if not row_obj:
            row_obj = db.query(LiveSellStock).filter(LiveSellStock.publisher_tag == tag).first()
            order_table = "sell" if row_obj else None
        if not row_obj:
            row_obj = db.query(LiveCircuitStock).filter(LiveCircuitStock.publisher_tag == tag).first()
            order_table = "circuit" if row_obj else None
        if not row_obj:
            logger.warning("[ProcessOrders] No order row found for tag=%s — skipping", tag)
            continue

        # Skip if already processed (idempotent)
        if hasattr(row_obj, "broker_status") and row_obj.broker_status in TERMINAL_STATUSES:
            logger.info("[ProcessOrders] Tag %s already terminal (%s) — skipping", tag, row_obj.broker_status)
            continue

        # Update order row from the order data
        if kite_order_id:
            row_obj.order_id = kite_order_id
        row_obj.broker_status = status
        row_obj.broker_status_message = order.get("status_message")

        if status == "COMPLETE" and filled_qty > 0:
            row_obj.actual_qty = filled_qty
            row_obj.actual_price = avg_price
            row_obj.actual_amount = round(filled_qty * avg_price, 2)
            row_obj.circuit = filled_qty < int(row_obj.qty)
            row_obj.updated_in_tradelog = False
            logger.info("[ProcessOrders] FILLED | tag=%s symbol=%s qty=%d price=%.2f source=%s",
                        tag, row_obj.tradingsymbol, filled_qty, avg_price, source)

        elif status == "REJECTED":
            row_obj.actual_qty = 0
            row_obj.actual_price = 0.0
            row_obj.actual_amount = 0.0
            row_obj.circuit = False
            row_obj.updated_in_tradelog = True
            logger.info("[ProcessOrders] REJECTED | tag=%s symbol=%s reason=%s source=%s",
                        tag, row_obj.tradingsymbol, order.get("status_message"), source)

        elif status == "CANCELLED":
            partial_qty = filled_qty if filled_qty > 0 else 0
            if partial_qty > 0:
                row_obj.actual_qty = partial_qty
                row_obj.actual_price = avg_price
                row_obj.actual_amount = round(partial_qty * avg_price, 2)
                row_obj.circuit = True
                row_obj.updated_in_tradelog = False
            else:
                row_obj.actual_qty = 0
                row_obj.actual_price = 0.0
                row_obj.actual_amount = 0.0
                row_obj.circuit = False  # No circuit row for unplaced/fully-cancelled orders — next rebalance handles it
                row_obj.updated_in_tradelog = True
            logger.info("[ProcessOrders] CANCELLED | tag=%s symbol=%s partial_qty=%d source=%s",
                        tag, row_obj.tradingsymbol, partial_qty, source)

        # Create circuit stock row if needed (partial fill or cancelled)
        if row_obj.circuit and order_table in ("buy", "sell"):
            lower_upper = "upper" if order_table == "buy" else "lower"
            circuit_row = LiveCircuitStock(
                automate_equity_ra_id=strategy.id,
                tradingsymbol=row_obj.tradingsymbol,
                isin=getattr(row_obj, "isin", ""),
                date=TODAY,
                qty=int(row_obj.qty) - int(row_obj.actual_qty or 0),
                price=float(row_obj.price),
                amount=round((int(row_obj.qty) - int(row_obj.actual_qty or 0)) * float(row_obj.price), 2),
                weightage=getattr(row_obj, "weightage", 0.0),
                actual_qty=0,
                actual_price=0.0,
                actual_amount=0.0,
                stoploss=getattr(row_obj, "stoploss", 0.0),
                volatility=getattr(row_obj, "volatility", 0.0),
                order_id=None,
                updated_in_tradelog=False,
                lower_upper=lower_upper,
                action=order_table,
                active=True,
            )
            db.add(circuit_row)

    # ── Step 2: Client ID lock from orderbook data ──
    first_order = next(iter(orders_data.values()), {})
    client_id = first_order.get("placed_by") or first_order.get("account_id")
    if client_id and not strategy.locked_client_id:
        strategy.locked_client_id = client_id

    # ── Flush in-memory updates to DB before counting ──
    # CRITICAL: Session uses autoflush=False, so Step 1's broker_status updates
    # are only in Python memory. Without this flush, the COUNT queries below
    # hit the database (which still has broker_status=NULL) and return 0,
    # causing ALL_REJECTED even when all orders are COMPLETE.
    db.flush()

    # ── Step 3: Count filled vs rejected for status transition ──
    filled_buy = db.query(LiveBuyStock).filter(
        LiveBuyStock.publisher_tag.in_(basket_tags),
        LiveBuyStock.broker_status == "COMPLETE",
    ).count()
    filled_sell = db.query(LiveSellStock).filter(
        LiveSellStock.publisher_tag.in_(basket_tags),
        LiveSellStock.broker_status == "COMPLETE",
    ).count()
    total_filled = filled_buy + filled_sell

    # Collect rejection reasons for logging
    rejected_orders = []
    for model in (LiveBuyStock, LiveSellStock):
        rejected_rows = db.query(model).filter(
            model.publisher_tag.in_(basket_tags),
            model.broker_status == "REJECTED",
        ).all()
        for r in rejected_rows:
            rejected_orders.append({
                "tradingsymbol": r.tradingsymbol,
                "qty": r.qty,
                "reason": r.broker_status_message or "Unknown",
            })

    # ── Step 4: Strategy status transition ──
    previous_status = strategy.status

    if total_filled == 0:
        # ALL orders rejected/cancelled — no fills at all
        if previous_status in (LiveStatus.EXIT_PENDING_USER_APPROVAL, LiveStatus.EXIT_PROCESSING):
            strategy.status = LiveStatus.ACTIVE
        elif previous_status in (LiveStatus.REBALANCE_PENDING_USER_APPROVAL, LiveStatus.REBALANCE_PROCESSING):
            if basket.basket_type == "REBALANCE_SELL":
                strategy.status = LiveStatus.REBALANCE_READY
            elif basket.basket_type == "REBALANCE_BUY":
                strategy.status = LiveStatus.REBALANCE_SELL_COMPLETE
            else:
                strategy.status = LiveStatus.REBALANCE_READY
        else:
            strategy.status = LiveStatus.ALL_REJECTED
        logger.warning("[ProcessOrders] ALL orders failed | strategy=%s prev=%s new=%s reasons=%s source=%s",
                       strategy.id, previous_status.value, strategy.status.value, rejected_orders, source)
    elif previous_status in (LiveStatus.EXIT_PENDING_USER_APPROVAL, LiveStatus.EXIT_PROCESSING):
        strategy.status = LiveStatus.EXITED
        strategy.subscription_active = False
    elif basket.basket_type == "REBALANCE_SELL":
        strategy.status = LiveStatus.REBALANCE_SELL_COMPLETE
        logger.info("[ProcessOrders] REBALANCE_SELL complete | strategy=%s — now awaiting BUY basket | source=%s",
                    strategy.id, source)
    else:
        # INITIAL, REBALANCE (ALL), REBALANCE_BUY, EXIT — go to ACTIVE
        strategy.status = LiveStatus.ACTIVE
        strategy.subscription_active = True
        strategy.next_rebalance_date = next_trading_day(next_rebalance_prepare_date(TODAY, strategy.rebalance_frequency))

    # ── Step 5: Update basket status ──
    basket.raw_postback = {"source": source, "processed_at": str(datetime.utcnow())}
    basket.status = "ALL_REJECTED" if total_filled == 0 else "COMPLETE"

    db.commit()
    db.refresh(strategy)


# -----------------------------------------------------------------------------
# Daily per-strategy tradelog + equity curve processing helper
# -----------------------------------------------------------------------------

def _process_strategy_daily_update(db: Session, strategy: LiveStrategy, TODAY: date) -> bool:
    """Process pending fills into tradelog, refresh LTP, and append equity curve row.

    Shared logic used by:
    - daily_equity_curve_update main loop (active strategies)
    - daily_equity_curve_update exited block (recently exited strategies)

    Returns True if there were pending fills, False otherwise.
    Does NOT commit — caller must commit after any additional status changes.
    """
    buy_df = model_df(db, LiveBuyStock, strategy.id)
    sell_df = model_df(db, LiveSellStock, strategy.id)
    circuit_df = model_df(db, LiveCircuitStock, strategy.id)
    tradelog_df = model_df(db, LiveTradelog, strategy.id)
    equitycurve_df = model_df(db, LiveEquityCurve, strategy.id)

    # ── Corporate Actions: symbol rename, ISIN sync, bonus/split ─────────
    # Must run BEFORE tradelog fill processing so that active holdings have
    # correct tradingsymbol, isin, buy_qty, buy_price before LTP refresh
    # and equity curve calculations.
    from app.services.corporate_action_service import apply_corporate_actions_to_strategy
    tradelog_df = apply_corporate_actions_to_strategy(db, strategy, tradelog_df, TODAY)

    # Check for pending fills (rows not yet processed into tradelog)
    # update_tradelog skips rows where:
    #   - updated_in_tradelog = True (already processed, including REJECTED)
    #   - actual_qty <= 0 (no fill happened)
    # So REJECTED orders (updated_in_tradelog=True, actual_qty=0) are never added to tradelog.
    has_pending_fills = False
    if not buy_df.empty:
        pending_buys = buy_df.loc[
            (buy_df["updated_in_tradelog"].fillna(False) == False) &
            (buy_df["actual_qty"].fillna(0) > 0)
        ]
        if not pending_buys.empty:
            has_pending_fills = True

    if not sell_df.empty:
        pending_sells = sell_df.loc[
            (sell_df["updated_in_tradelog"].fillna(False) == False) &
            (sell_df["actual_qty"].fillna(0) > 0)
        ]
        if not pending_sells.empty:
            has_pending_fills = True

    tradelog_df, today_cash = update_tradelog(
        TODAY, f"({strategy.id})", tradelog_df, buy_df, sell_df, circuit_df, 0.0
    )

    # Save tradelog (LTP already refreshed inside update_tradelog)
    upsert_df_to_db(db, tradelog_df, LiveTradelog, commit=False)

    # Mark buy/sell/circuit as processed (updated_in_tradelog = True)
    if has_pending_fills:
        update_df_to_db(db, buy_df, LiveBuyStock, commit=False)
        update_df_to_db(db, sell_df, LiveSellStock, commit=False)
        update_df_to_db(db, circuit_df, LiveCircuitStock, commit=False)

    if tradelog_df.empty:
        logger.info("[DailyMTM] Skipping equity curve for strategy %s — no tradelog rows", strategy.id)
        return has_pending_fills

    # Append today's equity curve row
    rebalance = has_pending_fills  # Cash delta only on fill days
    equitycurve_df = update_equitycurve(TODAY, strategy, tradelog_df, equitycurve_df, today_cash, rebalance)
    insert_single_row_to_db(db, equitycurve_df.iloc[-1].to_dict(), LiveEquityCurve, commit=False)

    # Sync strategy AUM fields
    last = equitycurve_df.iloc[-1]
    strategy.cash = float(last["cash"] or 0)
    strategy.stock_value = float(last["stocks_value"] or 0)
    strategy.final_aum = float(last["aum"] or 0)
    strategy.pnl = float(last["total_pnl"] or 0)
    strategy.todays_pnl = float(last["strategy_daily_return"] or 0)

    return has_pending_fills


# -----------------------------------------------------------------------------
# Public service methods used by API/tasks
# -----------------------------------------------------------------------------

class LiveInvestmentService:
    @staticmethod
    def create_go_live(
        db: Session,
        *,
        user_id,
        screener_version_id,
        strategy_name: Optional[str],
        broker_account_id,
        portfolio_size: int,
        wrh: int,
        rebalance_frequency: str,
        aum: float,
        TODAY: Optional[date] = None,
    ) -> LiveStrategy:
        TODAY = TODAY or date.today()
        version = db.query(ScreenerVersion).filter(ScreenerVersion.id == screener_version_id).first()
        if not version:
            raise HTTPException(status_code=404, detail="Screener version not found")

        broker_account = db.query(LiveBrokerAccount).filter(
            LiveBrokerAccount.id == broker_account_id,
            LiveBrokerAccount.user_id == user_id,
            LiveBrokerAccount.is_active == True,
        ).first()
        if not broker_account:
            raise HTTPException(status_code=404, detail="Broker account label not found or inactive")

        obj = LiveStrategy(
            user_id=user_id,
            screener_id=version.screener_id,
            screener_version_id=version.id,
            strategy_name=strategy_name,
            broker_account_id=broker_account.id,
            broker=broker_account.broker,
            broker_account_label=broker_account.broker_account_label,
            locked_client_id=broker_account.broker_user_id,
            portfolio_size=portfolio_size,
            worst_hold_rank=wrh,
            rebalance_frequency=rebalance_frequency,
            initial_aum=aum,
            cash=aum,
            stock_value=0.0,
            final_aum=aum,
            pnl=0.0,
            todays_pnl=0.0,
            subscription_active=False,
            status=LiveStatus.DRAFT,
            start_date=TODAY,
            next_rebalance_date=next_trading_day(next_rebalance_prepare_date(TODAY, rebalance_frequency)),
            filters_json=version.filters_json,
            universe_json=version.universe_json,
            ranking_json=version.ranking_json,
        )
        db.add(obj)
        db.commit()
        db.refresh(obj)
        # Initial equitycurve row like equitycase first-day setup.
        insert_df_to_db(db, make_equitycurve_table(TODAY, obj), LiveEquityCurve)
        logger.info("[GoLive] Created strategy %s | name=%s aum=%.2f portfolio_size=%d wrh=%d freq=%s",
                    obj.id, obj.strategy_name, obj.initial_aum, obj.portfolio_size, obj.worst_hold_rank, obj.rebalance_frequency)
        return obj

    @staticmethod
    def create_initial_preview(db: Session, strategy_id, TODAY: Optional[date] = None) -> LiveStrategy:
        TODAY = TODAY or date.today()
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        # ── INITIAL_PROCESSING — orders are in-flight on Kite ──
        # Preview/read endpoint: return 200 with the existing rows so the frontend
        # renders the preview read-only, disables the buttons and shows a spinner
        # (driven by strategy.status == INITIAL_PROCESSING). A 400 here strands the
        # user on a generic "Bad Request" screen. Duplicate-order protection stays in
        # trade_now()/_validate_trade_now_status(), which still 400s.
        if strategy.status == LiveStatus.INITIAL_PROCESSING:
            return _reconcile_stale_processing(db, strategy)

        # ── Stale recovery: PENDING_USER_APPROVAL with 0 fills (user abandoned Kite) ──
        if strategy.status == LiveStatus.PENDING_USER_APPROVAL:
            # ── Safety check: did the user already visit Kite? ──
            # If basket is REDIRECT_RECEIVED, the user went to Kite and came back.
            # Orders might have been placed — postbacks could still be in transit.
            # Do NOT auto-cancel; block until postback/verify completes.
            latest_basket = db.query(LivePublisherBasket).filter(
                LivePublisherBasket.automate_equity_ra_id == strategy.id,
            ).order_by(LivePublisherBasket.created_at.desc()).first()

            if latest_basket and latest_basket.status == "REDIRECT_RECEIVED":
                # Orders are in-flight on Kite. Don't 400 (frontend shows a Bad Request
                # page) — lock to INITIAL_PROCESSING and return 200 so the page renders
                # read-only with a spinner.
                return _lock_to_processing(db, strategy, LiveStatus.INITIAL_PROCESSING, "basket REDIRECT_RECEIVED")

            # Also check if any rows have broker data (order_id or postback) — orders were placed
            has_broker_data = db.query(LiveBuyStock).filter(
                LiveBuyStock.automate_equity_ra_id == strategy.id,
                or_(
                    and_(LiveBuyStock.order_id.isnot(None), LiveBuyStock.order_id != ""),
                    LiveBuyStock.broker_raw_postback.isnot(None),
                ),
            ).count() > 0
            if has_broker_data:
                return _lock_to_processing(db, strategy, LiveStatus.INITIAL_PROCESSING, "broker data present (orders sent)")

            filled = db.query(LiveBuyStock).filter(
                LiveBuyStock.automate_equity_ra_id == strategy.id,
                LiveBuyStock.actual_qty > 0,
            ).count()
            if filled == 0:
                # No orders filled and user never reached Kite — safe to cancel
                db.query(LivePublisherBasket).filter(
                    LivePublisherBasket.automate_equity_ra_id == strategy.id,
                    LivePublisherBasket.status == "PENDING_USER_APPROVAL",
                ).update({"status": "CANCELLED"}, synchronize_session=False)
                db.query(LiveBuyStock).filter(
                    LiveBuyStock.automate_equity_ra_id == strategy.id,
                    or_(LiveBuyStock.actual_qty.is_(None), LiveBuyStock.actual_qty == 0),
                ).delete(synchronize_session=False)
                db.commit()
                logger.info("[InitialPreview] Stale recovery for strategy %s — cancelled baskets + deleted unfilled rows", strategy.id)
                # Fall through to re-create preview
            else:
                # Some orders already filled — lock to INITIAL_PROCESSING and return 200
                # so the frontend shows processing state instead of a Bad Request page.
                return _lock_to_processing(db, strategy, LiveStatus.INITIAL_PROCESSING, "orders already filled")
        elif strategy.status not in {LiveStatus.DRAFT, LiveStatus.PREVIEW_READY, LiveStatus.ALL_REJECTED}:
            current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
            raise HTTPException(status_code=400, detail=f"Preview can be generated only from DRAFT/PREVIEW_READY/ALL_REJECTED. Current status={current}")
        _delete_unapproved_preview_rows(db, strategy.id, include_sell=False)
        screener_df = get_strategy_builder_screener_df(db, strategy.screener_version_id, strategy.portfolio_size)
        tradelog_df = model_df(db, LiveTradelog, strategy.id)
        buy_screener_df = screener_df.head(strategy.portfolio_size).copy()

        # Overlay live LTP on screener close for accurate preview prices
        ltp_map = _fetch_live_ltp(strategy, list(buy_screener_df["tradingsymbol"]))
        if ltp_map:
            buy_screener_df["close"] = buy_screener_df.apply(
                lambda row: ltp_map.get(row["tradingsymbol"], row["close"]), axis=1
            )

        buy_df = get_buy_df_investment(
            TODAY, strategy, f"({strategy.id})", buy_screener_df, tradelog_df,
            sell_stock_count=0,
            portfolio_size=strategy.portfolio_size,
            cash_available=strategy.cash,
            aum=strategy.final_aum or strategy.initial_aum,
        )
        insert_df_to_db(db, buy_df, LiveBuyStock)
        strategy.status = LiveStatus.PREVIEW_READY
        db.commit()
        db.refresh(strategy)
        return strategy

    @staticmethod
    def trade_now(db: Session, strategy_id, basket_type: str = "INITIAL", side: str = "ALL") -> LivePublisherBasket:
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        _validate_trade_now_status(strategy, basket_type, side)

        # Auto-retry for ALL_REJECTED: clean up old rejected rows + regenerate fresh buy rows
        if strategy.status == LiveStatus.ALL_REJECTED and basket_type == "INITIAL":
            TODAY = date.today()
            _delete_unapproved_preview_rows(db, strategy.id, include_sell=False)
            screener_df = get_strategy_builder_screener_df(db, strategy.screener_version_id, strategy.portfolio_size)
            tradelog_df = model_df(db, LiveTradelog, strategy.id)
            buy_df = get_buy_df_investment(
                TODAY, strategy, f"({strategy.id})",
                screener_df.head(strategy.portfolio_size).copy(), tradelog_df,
                sell_stock_count=0,
                portfolio_size=strategy.portfolio_size,
                cash_available=strategy.cash,
                aum=strategy.final_aum or strategy.initial_aum,
            )
            insert_df_to_db(db, buy_df, LiveBuyStock)

        # Clean up rejected rows when retrying BUY from REBALANCE_SELL_COMPLETE
        # Rejected rows have updated_in_tradelog=True so assign_publisher_tags skips them
        if basket_type == "REBALANCE" and side == "BUY" and strategy.status == LiveStatus.REBALANCE_SELL_COMPLETE:
            _delete_unapproved_preview_rows(db, strategy.id, include_sell=False)

        # Auto-refresh exit sell rows with latest LTP before sending basket
        # Same pattern as ALL_REJECTED auto-retry above
        if basket_type == "EXIT":
            LiveInvestmentService.create_exit_preview(db, strategy_id)
            db.refresh(strategy)

        return assign_publisher_tags(db, strategy, basket_type, side)

    @staticmethod
    def prepare_rebalance(db: Session, strategy_id, TODAY: Optional[date] = None, send_email: bool = False) -> LiveStrategy:
        TODAY = TODAY or date.today()
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        _stale_recovered = False
        # ── REBALANCE_PROCESSING — orders are in-flight on Kite ──
        # Do NOT raise 400 here. This is a preview/read endpoint: the frontend
        # rebalance-preview page needs a 200 with the existing buy/sell rows so it
        # can render the preview read-only, disable the Buy/Trade-Now buttons, and
        # show a processing spinner (driven by strategy.status == REBALANCE_PROCESSING).
        # A 400 makes the page fall into its generic error handler and the user gets
        # stuck on a "Bad Request" screen with no preview.
        # Return the strategy as-is (before _delete_unapproved_preview_rows) so the
        # in-flight order rows stay intact. The real guard against duplicate orders
        # lives in trade_now()/_validate_trade_now_status(), which still 400s.
        if strategy.status == LiveStatus.REBALANCE_PROCESSING:
            return _reconcile_stale_processing(db, strategy)

        if strategy.status in {LiveStatus.REBALANCE_SELL_COMPLETE, LiveStatus.REBALANCE_PENDING_USER_APPROVAL}:
            if strategy.status == LiveStatus.REBALANCE_PENDING_USER_APPROVAL:
                # ── Safety check: did the user already visit Kite? ──
                latest_basket = db.query(LivePublisherBasket).filter(
                    LivePublisherBasket.automate_equity_ra_id == strategy.id,
                ).order_by(LivePublisherBasket.created_at.desc()).first()

                if latest_basket and latest_basket.status == "REDIRECT_RECEIVED":
                    # Orders are in-flight on Kite (user returned from the redirect).
                    # Don't 400 — lock to REBALANCE_PROCESSING and return 200 so the
                    # preview renders read-only with a spinner instead of a Bad Request page.
                    return _lock_to_processing(db, strategy, LiveStatus.REBALANCE_PROCESSING, "basket REDIRECT_RECEIVED")

                # Check if any orders were actually filled by broker.
                # If zero fills → user abandoned Kite without completing orders.
                # Also check for broker data (order_id/postback) — if present, orders were placed
                has_broker_data = db.query(LiveSellStock).filter(
                    LiveSellStock.automate_equity_ra_id == strategy.id,
                    or_(
                        and_(LiveSellStock.order_id.isnot(None), LiveSellStock.order_id != ""),
                        LiveSellStock.broker_raw_postback.isnot(None),
                    ),
                ).count() > 0
                if not has_broker_data:
                    has_broker_data = db.query(LiveBuyStock).filter(
                        LiveBuyStock.automate_equity_ra_id == strategy.id,
                        or_(
                            and_(LiveBuyStock.order_id.isnot(None), LiveBuyStock.order_id != ""),
                            LiveBuyStock.broker_raw_postback.isnot(None),
                        ),
                    ).count() > 0
                if has_broker_data:
                    # Orders were sent to the broker but the status hasn't been upgraded
                    # (e.g. redirect callback skipped because already connected today).
                    # Lock to REBALANCE_PROCESSING and return 200 instead of a Bad Request page.
                    return _lock_to_processing(db, strategy, LiveStatus.REBALANCE_PROCESSING, "broker data present (orders sent)")

                filled_sells = db.query(LiveSellStock).filter(
                    LiveSellStock.automate_equity_ra_id == strategy.id,
                    LiveSellStock.actual_qty > 0,
                ).count()
                filled_buys = db.query(LiveBuyStock).filter(
                    LiveBuyStock.automate_equity_ra_id == strategy.id,
                    LiveBuyStock.actual_qty > 0,
                ).count()

                if filled_sells == 0 and filled_buys == 0:
                    # ── Stale basket recovery ──
                    # User went to Kite but placed zero orders. Cancel stale baskets
                    # and aggressively clean up ALL unfilled order rows (including rows
                    # that have stale publisher_tags which would cause tag mismatch on retry).
                    db.query(LivePublisherBasket).filter(
                        LivePublisherBasket.automate_equity_ra_id == strategy.id,
                        LivePublisherBasket.status == "PENDING_USER_APPROVAL",
                    ).update({"status": "CANCELLED"}, synchronize_session=False)

                    # Delete ALL unfilled/unprocessed rows regardless of order_id or tag.
                    # This is more aggressive than _delete_unapproved_preview_rows because
                    # it also catches rows where a non-terminal postback set order_id but
                    # verification never ran (e.g., OPEN postback arrived, user abandoned).
                    db.query(LiveSellStock).filter(
                        LiveSellStock.automate_equity_ra_id == strategy.id,
                        or_(LiveSellStock.actual_qty.is_(None), LiveSellStock.actual_qty == 0),
                        LiveSellStock.updated_in_tradelog == False,
                    ).delete(synchronize_session=False)
                    db.query(LiveBuyStock).filter(
                        LiveBuyStock.automate_equity_ra_id == strategy.id,
                        or_(LiveBuyStock.actual_qty.is_(None), LiveBuyStock.actual_qty == 0),
                        LiveBuyStock.updated_in_tradelog == False,
                    ).delete(synchronize_session=False)

                    # NOTE: Do NOT change strategy.status here — leave as
                    # REBALANCE_PENDING_USER_APPROVAL until re-preparation is complete.
                    # This prevents cron jobs from seeing a transient ACTIVE status.
                    db.commit()  # only saves basket cancellations + row deletions
                    _stale_recovered = True
                    logger.info("[Rebalance] Auto-cancelled stale REBALANCE_PENDING for strategy %s (0 fills) — re-preparing", strategy.id)
                    # Fall through to full rebalance preparation below
                else:
                    # Orders were filled — return as-is for frontend to render current state
                    return strategy
            else:
                # REBALANCE_SELL_COMPLETE — sell basket completed, buy/sell rows exist
                return strategy
        if not _stale_recovered and strategy.status not in {LiveStatus.ACTIVE, LiveStatus.REBALANCE_READY}:
            raise HTTPException(status_code=400, detail=f"Strategy must be ACTIVE or REBALANCE_READY to prepare rebalance. Current status={strategy.status.value}")
        _delete_unapproved_preview_rows(db, strategy.id, include_sell=True)

        screener_df = get_strategy_builder_screener_df(db, strategy.screener_version_id, max(strategy.portfolio_size, strategy.worst_hold_rank))
        tradelog_df = model_df(db, LiveTradelog, strategy.id)
        equitycurve_df = model_df(db, LiveEquityCurve, strategy.id)
        latest = equitycurve_df.iloc[-1] if not equitycurve_df.empty else {"aum": strategy.final_aum, "cash": strategy.cash}
        aum = float(latest.get("aum") or strategy.final_aum or strategy.initial_aum)
        cash = float(latest.get("cash") or strategy.cash or 0.0)

        # Fetch live LTP for both sell (tradelog) and buy (screener) symbols
        all_symbols = set(screener_df["tradingsymbol"]) if not screener_df.empty else set()
        if not tradelog_df.empty:
            active_holdings = tradelog_df.loc[tradelog_df["active"] == True]
            all_symbols.update(active_holdings["tradingsymbol"])
        ltp_map = _fetch_live_ltp(strategy, list(all_symbols))

        # Overlay live LTP on tradelog for accurate sell preview prices
        if ltp_map and not tradelog_df.empty:
            tradelog_df = tradelog_df.copy()
            tradelog_df["ltp"] = tradelog_df.apply(
                lambda row: ltp_map.get(row["tradingsymbol"], row.get("ltp") or 0), axis=1
            )

        sell_df = get_sell_df_investment(TODAY, strategy, f"({strategy.id})", screener_df, tradelog_df, strategy.worst_hold_rank)
        insert_df_to_db(db, sell_df, LiveSellStock)
        amount_to_sell_tomorrow = float(sell_df["amount"].sum()) if not sell_df.empty else 0.0
        cash_available = (cash + (amount_to_sell_tomorrow * 0.80)) * 0.98

        # Overlay live LTP on screener for accurate buy preview prices
        buy_screener_df = screener_df.head(strategy.portfolio_size).copy()
        if ltp_map and not buy_screener_df.empty:
            buy_screener_df["close"] = buy_screener_df.apply(
                lambda row: ltp_map.get(row["tradingsymbol"], row["close"]), axis=1
            )

        buy_df = get_buy_df_investment(
            TODAY, strategy, f"({strategy.id})", buy_screener_df, tradelog_df,
            sell_stock_count=len(sell_df),
            portfolio_size=strategy.portfolio_size,
            cash_available=cash_available,
            aum=aum,
        )
        insert_df_to_db(db, buy_df, LiveBuyStock)

        # ── Empty rebalance: nothing to buy or sell ──
        # Portfolio already matches screener output — no action needed.
        # Stay ACTIVE, roll next_rebalance_date forward, still send email.
        if sell_df.empty and buy_df.empty:
            strategy.next_rebalance_date = next_trading_day(
                next_rebalance_prepare_date(TODAY + timedelta(days=1), strategy.rebalance_frequency)
            )
            db.commit()
            db.refresh(strategy)
            logger.info("[Rebalance] No changes needed for strategy %s — staying ACTIVE, next=%s",
                        strategy.id, strategy.next_rebalance_date)
            # Still send notification email (user wants to know even if no changes)
            if send_email:
                _gather_and_notify_rebalance(db, strategy, TODAY)
            return strategy

        strategy.status = LiveStatus.REBALANCE_READY
        db.commit()
        db.refresh(strategy)

        # Fire-and-forget rebalance notification (email)
        if send_email:
            _gather_and_notify_rebalance(db, strategy, TODAY)

        return strategy

    @staticmethod
    def skip_empty_rebalance(db: Session, strategy_id, TODAY: Optional[date] = None) -> LiveStrategy:
        TODAY = TODAY or date.today()
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        
        valid_statuses = {
            LiveStatus.REBALANCE_READY, 
            LiveStatus.REBALANCE_SELL_COMPLETE,
            LiveStatus.REBALANCE_PENDING_USER_APPROVAL
        }
        if strategy.status not in valid_statuses:
            current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
            raise HTTPException(status_code=400, detail=f"Cannot skip rebalance from status={current}")

        # Clean up orphaned preview rows before resetting
        _delete_unapproved_preview_rows(db, strategy.id, include_sell=True)
        # Cancel any pending baskets
        db.query(LivePublisherBasket).filter(
            LivePublisherBasket.automate_equity_ra_id == strategy.id,
            LivePublisherBasket.status.in_(["PENDING_USER_APPROVAL", "REDIRECT_RECEIVED"]),
        ).update({"status": "CANCELLED"}, synchronize_session=False)

        # Reset to active and roll over date
        strategy.status = LiveStatus.ACTIVE
        strategy.next_rebalance_date = next_trading_day(next_rebalance_prepare_date(TODAY, strategy.rebalance_frequency))
        db.commit()
        db.refresh(strategy)
        return strategy

    @staticmethod
    def create_exit_preview(db: Session, strategy_id, TODAY: Optional[date] = None) -> LiveStrategy:
        TODAY = TODAY or date.today()
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        # ── EXIT_PROCESSING — orders are in-flight on Kite ──
        # Preview/read endpoint: return 200 with the existing rows so the frontend
        # renders the preview read-only, disables the buttons and shows a spinner
        # (driven by strategy.status == EXIT_PROCESSING). A 400 here strands the user
        # on a generic "Bad Request" screen. Duplicate-order protection stays in
        # trade_now()/_validate_trade_now_status(), which still 400s.
        if strategy.status == LiveStatus.EXIT_PROCESSING:
            return _reconcile_stale_processing(db, strategy)

        # ── Stale recovery: EXIT_PENDING_USER_APPROVAL with 0 fills (user abandoned Kite) ──
        if strategy.status == LiveStatus.EXIT_PENDING_USER_APPROVAL:
            filled = db.query(LiveSellStock).filter(
                LiveSellStock.automate_equity_ra_id == strategy.id,
                LiveSellStock.method == "EXIT",
                LiveSellStock.actual_qty > 0,
            ).count()
            if filled == 0:
                # Cancel stale baskets + delete unfilled exit sell rows
                db.query(LivePublisherBasket).filter(
                    LivePublisherBasket.automate_equity_ra_id == strategy.id,
                    LivePublisherBasket.status.in_(["PENDING_USER_APPROVAL", "REDIRECT_RECEIVED"]),
                ).update({"status": "CANCELLED"}, synchronize_session=False)
                db.query(LiveSellStock).filter(
                    LiveSellStock.automate_equity_ra_id == strategy.id,
                    LiveSellStock.method == "EXIT",
                    or_(LiveSellStock.actual_qty.is_(None), LiveSellStock.actual_qty == 0),
                    LiveSellStock.updated_in_tradelog == False,
                ).delete(synchronize_session=False)
                strategy.status = LiveStatus.ACTIVE  # Restore to ACTIVE so exit preview can proceed
                db.commit()
                logger.info("[ExitPreview] Stale recovery for strategy %s — cancelled baskets + deleted unfilled exit rows, restored to ACTIVE", strategy.id)
            else:
                raise HTTPException(status_code=400, detail="Exit orders were already filled. Wait for postback processing to complete.")
        elif strategy.status != LiveStatus.ACTIVE:
            current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
            raise HTTPException(status_code=400, detail=f"Exit preview can be generated only from ACTIVE. Current status={current}")
        _delete_unapproved_preview_rows(db, strategy.id, include_sell=True)

        # ── Same-day exit support ──
        # If user bought stocks today and wants to exit before 16:30 daily job,
        # the tradelog won't have entries yet. Process pending fills into tradelog
        # on-demand so exit preview can read them. Scoped to THIS strategy only.
        buy_df = model_df(db, LiveBuyStock, strategy.id)
        if not buy_df.empty:
            pending_buys = buy_df.loc[
                (buy_df["updated_in_tradelog"].fillna(False) == False) &
                (buy_df["actual_qty"].fillna(0) > 0)
            ]
            if not pending_buys.empty:
                logger.info("[ExitPreview] Found %d pending fills for strategy %s — processing into tradelog now",
                            len(pending_buys), strategy.id)
                _process_strategy_daily_update(db, strategy, TODAY)
                db.commit()

        tradelog_df = model_df(db, LiveTradelog, strategy.id)
        active_df = tradelog_df.loc[tradelog_df["active"] == True] if not tradelog_df.empty else pd.DataFrame()
        # Fetch live LTP for exit preview prices
        exit_symbols = list(active_df["tradingsymbol"]) if not active_df.empty else []
        ltp_map = _fetch_live_ltp(strategy, exit_symbols)

        rows = []
        for _, row in active_df.iterrows():
            remaining_qty = int(row["buy_qty"] or 0) - int(row["sell_qty"] or 0)
            if remaining_qty <= 0:
                continue
            # Use live LTP if available, fallback to tradelog ltp, then buy_price
            tradingsymbol = row["tradingsymbol"]
            price = float(ltp_map.get(tradingsymbol) or row.get("ltp") or row.get("buy_price") or 0)
            rows.append({
                "automate_equity_ra_id": strategy.id,
                "tradingsymbol": tradingsymbol,
                "isin": row.get("isin", ""),
                "date": TODAY,
                "qty": remaining_qty,
                "price": price,
                "amount": round(remaining_qty * price, 2),
                "actual_qty": 0,
                "actual_price": 0.0,
                "actual_amount": 0.0,
                "order_id": None,
                "method": "EXIT",
                "circuit": False,
                "updated_in_tradelog": False,
            })
        if rows:
            insert_df_to_db(db, pd.DataFrame(rows), LiveSellStock)
        # Status stays ACTIVE — only changes to EXIT_PENDING_USER_APPROVAL
        # when user actually clicks Trade Now (in assign_publisher_tags).
        # If user goes back, status remains ACTIVE → rebalance works normally.
        # Orphaned exit sell rows get cleaned up by _delete_unapproved_preview_rows
        # on next rebalance or next exit preview call.
        db.commit()
        db.refresh(strategy)
        return strategy

    # ── Redis for debounce (orderbook verification) ────────────────────────
    _redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

    @staticmethod
    def update_from_postback(db: Session, payload: Dict[str, Any], TODAY: Optional[date] = None) -> dict:
        """Process a single broker postback.

        Flow:
        1. Find order row by publisher_tag
        2. Store raw postback + order_id on order row (broker_raw_postback column)
        3. If terminal status → schedule debounced orderbook verification task
        4. Actual fill processing (actual_qty, broker_status, status transitions)
           happens in verify_and_process_from_orderbook(), triggered by the
           debounced celery task.

        Returns a dict with status info for the API response.
        """
        TODAY = TODAY or date.today()

        # Terminal statuses — only these should trigger orderbook verification
        TERMINAL_STATUSES = {"COMPLETE", "REJECTED", "CANCELLED"}

        logger.info("[Postback] Received | tag=%s order_id=%s status=%s symbol=%s txn=%s",
                    payload.get("tag"), payload.get("order_id"), payload.get("status"),
                    payload.get("tradingsymbol"), payload.get("transaction_type"))

        raw_tag = payload.get("tag")  # Our unique 8-char publisher_tag per order
        kite_order_id = payload.get("order_id")  # Kite's unique order ID
        postback_symbol = payload.get("tradingsymbol")

        if not raw_tag:
            raise HTTPException(status_code=400, detail="Postback missing tag")
        if not postback_symbol:
            raise HTTPException(status_code=400, detail="Postback missing tradingsymbol")

        # ── Step 1: Find the specific order row by publisher_tag (unique per order) ──
        row_obj = None
        order_table = None  # Track which table the order is in
        row_obj = db.query(LiveBuyStock).filter(LiveBuyStock.publisher_tag == raw_tag).first()
        if row_obj:
            order_table = "buy"
        else:
            row_obj = db.query(LiveSellStock).filter(LiveSellStock.publisher_tag == raw_tag).first()
            if row_obj:
                order_table = "sell"
            else:
                row_obj = db.query(LiveCircuitStock).filter(LiveCircuitStock.publisher_tag == raw_tag).first()
                if row_obj:
                    order_table = "circuit"

        if not row_obj:
            logger.warning("[Postback] No order found for publisher_tag=%s symbol=%s", raw_tag, postback_symbol)
            raise HTTPException(status_code=404, detail=f"No order found for tag '{raw_tag}'")

        # ── Step 2: Find the strategy from the matched order row ──
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == row_obj.automate_equity_ra_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found for order")

        # ── Step 3: Safety — verify tradingsymbol matches ──
        order_symbol = getattr(row_obj, "tradingsymbol", None)
        if postback_symbol and order_symbol and postback_symbol.upper() != order_symbol.upper():
            logger.warning("[Postback] SYMBOL MISMATCH | tag=%s expected=%s got=%s", raw_tag, order_symbol, postback_symbol)
            raise HTTPException(status_code=400, detail=f"Symbol mismatch: expected {order_symbol}, got {postback_symbol}")

        # Broker-specific normalization after strategy is known.
        adapter = get_publisher_adapter(strategy.broker)
        normalized = adapter.normalize_postback(payload)

        # ── Step 4: Store raw postback + order_id on order row ──
        if kite_order_id:
            row_obj.order_id = kite_order_id
        row_obj.broker_raw_postback = normalized
        logger.info("[Postback] Stored raw postback | tag=%s symbol=%s order_id=%s table=%s",
                    raw_tag, postback_symbol, kite_order_id, order_table)

        # ── Step 5: Client ID lock (early detection of wrong account) ────
        client_id = normalized.get("client_id") or normalized.get("user_id")
        if client_id:
            if not strategy.locked_client_id:
                broker_account = db.query(LiveBrokerAccount).filter(
                    LiveBrokerAccount.id == strategy.broker_account_id,
                ).first()
                if broker_account and broker_account.broker_user_id:
                    if client_id != broker_account.broker_user_id:
                        strategy.status = LiveStatus.ACCOUNT_MISMATCH
                        db.commit()
                        return {"status": "ok", "detail": "account_mismatch"}
                strategy.locked_client_id = client_id
            elif strategy.locked_client_id != client_id:
                strategy.status = LiveStatus.ACCOUNT_MISMATCH
                db.commit()
                return {"status": "ok", "detail": "account_mismatch"}

        # ── Step 5b: Upgrade to _PROCESSING on first postback (backup for redirect callback) ──
        # If redirect callback wasn't called (e.g., Exit redirects to portfolio list),
        # the first postback confirms orders are in-flight and locks the strategy.
        STATUS_TO_PROCESSING = {
            LiveStatus.PENDING_USER_APPROVAL: LiveStatus.INITIAL_PROCESSING,
            LiveStatus.REBALANCE_PENDING_USER_APPROVAL: LiveStatus.REBALANCE_PROCESSING,
            LiveStatus.REBALANCE_SELL_COMPLETE: LiveStatus.REBALANCE_PROCESSING,  # Buy phase
            LiveStatus.EXIT_PENDING_USER_APPROVAL: LiveStatus.EXIT_PROCESSING,
        }
        new_status = STATUS_TO_PROCESSING.get(strategy.status)
        if new_status:
            logger.info("[Postback] Upgrading strategy %s status %s → %s (first postback backup)",
                        strategy.id, strategy.status.value, new_status.value)
            strategy.status = new_status

        db.commit()

        # ── Step 6: If terminal → schedule debounced orderbook verification ──
        broker_status = (normalized.get("status") or "").upper()
        if broker_status not in TERMINAL_STATUSES:
            logger.info("[Postback] Non-terminal status=%s | tag=%s — stored, no verification needed", broker_status, raw_tag)
            return {"status": "ok", "detail": f"non_terminal_{broker_status.lower()}"}

        # Find the latest basket for this strategy to get the basket_id
        latest_basket = db.query(LivePublisherBasket).filter(
            LivePublisherBasket.automate_equity_ra_id == strategy.id,
        ).order_by(LivePublisherBasket.created_at.desc()).first()

        if not latest_basket:
            logger.warning("[Postback] No basket found for strategy %s — cannot schedule verification", strategy.id)
            return {"status": "ok", "detail": "no_basket_found"}

        # Debounce via Redis SETNX: only the FIRST terminal postback schedules the task.
        # Subsequent terminal postbacks (arriving within 300s) see the key exists and skip.
        redis_key = f"orderbook_verify:{latest_basket.id}"
        is_first = LiveInvestmentService._redis_client.set(redis_key, "1", nx=True, ex=300)

        if is_first:
            # Schedule the debounced celery task with 5-second delay
            from app.tasks.orderbook_sync_tasks import verify_basket_from_orderbook_task
            verify_basket_from_orderbook_task.apply_async(
                kwargs={"basket_id": str(latest_basket.id), "retry_count": 0},
                countdown=5,
            )
            logger.info("[Postback] Scheduled orderbook verification | basket=%s (5s delay)", latest_basket.id)
        else:
            logger.info("[Postback] Verification already scheduled | basket=%s tag=%s — skipping", latest_basket.id, raw_tag)

        return {"status": "ok", "detail": "stored_verification_scheduled"}

    # ── Orderbook verification (called by debounced celery task) ──────────

    @staticmethod
    def verify_and_process_from_orderbook(
        db: Session,
        basket_id: str,
        retry_count: int = 0,
        TODAY: Optional[date] = None,
    ) -> dict:
        """Verify all basket orders from kite.orders() and bulk-update if all terminal.

        Called by the debounced celery task after a terminal postback is received.

        Flow:
        1. Load basket → strategy → broker_account
        2. Decrypt access_token → call kite.orders()
        3. Filter orderbook by basket's publisher_tags
        4. If all orders terminal → bulk update actual_qty/price/status → status transition
        5. If not all terminal → reschedule (up to 20 retries × 5s = ~100s)
        6. If token/API fails → fall back to stored postback data

        Returns dict with processing result.
        """
        TODAY = TODAY or date.today()
        TERMINAL_STATUSES = {"COMPLETE", "REJECTED", "CANCELLED"}
        MAX_RETRIES = 20

        basket = db.query(LivePublisherBasket).filter(
            LivePublisherBasket.id == basket_id,
        ).first()
        if not basket:
            logger.error("[OrderbookVerify] Basket %s not found", basket_id)
            return {"status": "error", "detail": "basket_not_found"}

        # Early guard: skip already-processed baskets (saves a wasted kite.orders() call)
        if basket.status in ("COMPLETE", "ALL_REJECTED"):
            logger.info("[OrderbookVerify] Basket %s already %s — skipping", basket_id, basket.status)
            return {"status": "ok", "detail": "already_processed"}

        strategy = db.query(LiveStrategy).filter(
            LiveStrategy.id == basket.automate_equity_ra_id,
        ).first()
        if not strategy:
            logger.error("[OrderbookVerify] Strategy not found for basket %s", basket_id)
            return {"status": "error", "detail": "strategy_not_found"}

        # Early guard: don't revert EXITED strategies
        if strategy.status == LiveStatus.EXITED:
            logger.info("[OrderbookVerify] Strategy %s already EXITED — skipping", strategy.id)
            return {"status": "ok", "detail": "already_exited"}

        # Extract basket's publisher_tags
        basket_tags = set()
        if basket.publisher_payload:
            basket_tags = {
                o.get("tag") for o in (basket.publisher_payload.get("basket") or [])
                if o.get("tag")
            }
        if not basket_tags:
            logger.warning("[OrderbookVerify] No tags in basket %s — nothing to verify", basket_id)
            return {"status": "ok", "detail": "no_tags"}

        # ── Try fetching orderbook from broker API ──
        orderbook_orders = None  # Will be a dict: {tag: order_dict}
        broker_account = db.query(LiveBrokerAccount).filter(
            LiveBrokerAccount.id == strategy.broker_account_id,
        ).first()

        if broker_account and broker_account.access_token_encrypted and broker_account.token_date == TODAY:
            try:
                access_token = decrypt_token(broker_account.access_token_encrypted)
                if access_token:
                    adapter = get_publisher_adapter(strategy.broker)
                    full_orderbook = adapter.fetch_orderbook(access_token)

                    # Filter by our basket's tags only
                    orderbook_orders = {}
                    for order in full_orderbook:
                        tag = order.get("tag")
                        if tag and tag in basket_tags:
                            orderbook_orders[tag] = order

                    logger.info("[OrderbookVerify] Fetched %d total orders, %d match our tags | basket=%s",
                                len(full_orderbook), len(orderbook_orders), basket_id)
            except Exception as e:
                logger.error("[OrderbookVerify] Failed to fetch orderbook for basket %s: %s", basket_id, e)
                orderbook_orders = None  # Fall through to fallback

        # ── Check if all basket orders are terminal ──
        if orderbook_orders is not None:
            # We have orderbook data — check if all tagged orders are terminal
            found_tags = set(orderbook_orders.keys())
            missing_tags = basket_tags - found_tags

            all_terminal = True
            for tag in found_tags:
                status = (orderbook_orders[tag].get("status") or "").upper()
                if status == "CANCEL":
                    status = "CANCELLED"  # Kite sends "CANCEL" sometimes
                if status not in TERMINAL_STATUSES:
                    all_terminal = False
                    break

            # Missing tags = orders not yet in orderbook (not placed yet or API lag)
            if missing_tags:
                all_terminal = False

            if not all_terminal:
                if retry_count < MAX_RETRIES:
                    # Reschedule — not all orders are terminal yet
                    from app.tasks.orderbook_sync_tasks import verify_basket_from_orderbook_task
                    verify_basket_from_orderbook_task.apply_async(
                        kwargs={"basket_id": str(basket_id), "retry_count": retry_count + 1},
                        countdown=5,
                    )
                    logger.info("[OrderbookVerify] Not all terminal (%d/%d found, %d missing) — retry %d/%d | basket=%s",
                                len(found_tags), len(basket_tags), len(missing_tags), retry_count + 1, MAX_RETRIES, basket_id)
                    return {"status": "ok", "detail": "rescheduled", "retry": retry_count + 1}
                else:
                    # ── Max retries reached: treat missing/non-terminal tags as CANCELLED ──
                    # Orders that never appeared in the orderbook were never placed by the
                    # broker. Waiting longer won't help — proceed with what we have so the
                    # strategy isn't stuck forever. Next rebalance will pick up any missing stocks.
                    logger.warning("[OrderbookVerify] Max retries (%d) reached — treating %d missing tags as CANCELLED | basket=%s",
                                   MAX_RETRIES, len(missing_tags), basket_id)
                    for mtag in missing_tags:
                        orderbook_orders[mtag] = {
                            "tag": mtag,
                            "order_id": "",
                            "status": "CANCELLED",
                            "filled_quantity": 0,
                            "average_price": 0,
                            "tradingsymbol": "",
                            "transaction_type": "",
                            "status_message": "Order never placed — not found in orderbook after max retries",
                        }
                    # Also handle found-but-non-terminal orders (e.g. stuck in OPEN)
                    for tag in found_tags:
                        status = (orderbook_orders[tag].get("status") or "").upper()
                        if status == "CANCEL":
                            status = "CANCELLED"
                        if status not in TERMINAL_STATUSES:
                            logger.warning("[OrderbookVerify] Tag %s still non-terminal (%s) after max retries — treating as CANCELLED | basket=%s",
                                           tag, status, basket_id)
                            orderbook_orders[tag]["status"] = "CANCELLED"
                            orderbook_orders[tag]["filled_quantity"] = 0
                            orderbook_orders[tag]["status_message"] = f"Order stuck in {status} — treated as CANCELLED after max retries"
                    # Fall through to process all orders

            # ── ALL TERMINAL — Bulk update from orderbook ──
            source = "orderbook"
            orders_data = orderbook_orders  # {tag: order_dict}
        else:
            # ── FALLBACK: No orderbook data available — use stored postback data ──
            logger.warning("[OrderbookVerify] No orderbook data — falling back to postback data | basket=%s", basket_id)
            source = "postback_fallback"
            orders_data = {}  # {tag: order_dict from postback}

            for tag in basket_tags:
                # Try to find stored postback data on each order row
                row = db.query(LiveBuyStock).filter(LiveBuyStock.publisher_tag == tag).first()
                if not row:
                    row = db.query(LiveSellStock).filter(LiveSellStock.publisher_tag == tag).first()
                if not row:
                    row = db.query(LiveCircuitStock).filter(LiveCircuitStock.publisher_tag == tag).first()

                if row and row.broker_raw_postback:
                    pb = row.broker_raw_postback
                    orders_data[tag] = {
                        "tag": tag,
                        "order_id": pb.get("order_id") or getattr(row, "order_id", None),
                        "status": pb.get("status", ""),
                        "filled_quantity": pb.get("filled_quantity", 0),
                        "average_price": pb.get("average_price", 0),
                        "tradingsymbol": pb.get("tradingsymbol", ""),
                        "transaction_type": pb.get("transaction_type", ""),
                        "placed_by": pb.get("client_id") or pb.get("user_id", ""),
                        "status_message": pb.get("status_message"),
                    }

            # Check if all fallback orders are terminal
            all_fallback_terminal = all(
                (orders_data.get(tag, {}).get("status") or "").upper() in TERMINAL_STATUSES
                for tag in basket_tags
                if tag in orders_data
            )
            if not all_fallback_terminal or len(orders_data) < len(basket_tags):
                if retry_count < MAX_RETRIES:
                    from app.tasks.orderbook_sync_tasks import verify_basket_from_orderbook_task
                    verify_basket_from_orderbook_task.apply_async(
                        kwargs={"basket_id": str(basket_id), "retry_count": retry_count + 1},
                        countdown=5,
                    )
                    logger.info("[OrderbookVerify] Fallback: not all terminal — retry %d/%d | basket=%s",
                                retry_count + 1, MAX_RETRIES, basket_id)
                    return {"status": "ok", "detail": "fallback_rescheduled"}
                else:
                    # ── Max retries reached: treat missing tags as CANCELLED ──
                    # Same logic as orderbook path — orders without postback data
                    # were never placed. Proceed with what we have.
                    missing_fallback_tags = basket_tags - set(orders_data.keys())
                    logger.warning("[OrderbookVerify] Fallback: max retries reached — treating %d missing tags as CANCELLED | basket=%s",
                                   len(missing_fallback_tags), basket_id)
                    for mtag in missing_fallback_tags:
                        orders_data[mtag] = {
                            "tag": mtag,
                            "order_id": "",
                            "status": "CANCELLED",
                            "filled_quantity": 0,
                            "average_price": 0,
                            "tradingsymbol": "",
                            "transaction_type": "",
                            "status_message": "Order never placed — no postback received after max retries",
                        }
                    # Also handle non-terminal postback orders
                    for tag in list(orders_data.keys()):
                        status = (orders_data[tag].get("status") or "").upper()
                        if status not in TERMINAL_STATUSES:
                            logger.warning("[OrderbookVerify] Fallback: tag %s non-terminal (%s) — treating as CANCELLED | basket=%s",
                                           tag, status, basket_id)
                            orders_data[tag]["status"] = "CANCELLED"
                            orders_data[tag]["filled_quantity"] = 0
                            orders_data[tag]["status_message"] = f"Order stuck in {status} — treated as CANCELLED after max retries"
                    # Fall through to process all orders

        # ── Process all orders — bulk update from orderbook/postback data ──
        _process_basket_orders(db, strategy, basket, orders_data, basket_tags, source, TODAY)

        # Clear Redis debounce key
        LiveInvestmentService._redis_client.delete(f"orderbook_verify:{basket_id}")

        logger.info("[OrderbookVerify] Basket %s processed from %s | strategy=%s status=%s",
                    basket_id, source, strategy.id,
                    strategy.status.value if hasattr(strategy.status, 'value') else strategy.status)

        return {
            "status": "ok",
            "detail": f"processed_from_{source}",
            "strategy_status": strategy.status.value if hasattr(strategy.status, 'value') else str(strategy.status),
        }

    @staticmethod
    def duplicate_strategy(db: Session, strategy_id, *, broker_account_id, strategy_name: Optional[str], aum: Optional[float]) -> LiveStrategy:
        old = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not old:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        return LiveInvestmentService.create_go_live(
            db,
            user_id=old.user_id,
            screener_version_id=old.screener_version_id,
            strategy_name=strategy_name or f"{old.strategy_name or 'Strategy'} Copy",
            broker_account_id=broker_account_id,
            portfolio_size=old.portfolio_size,
            wrh=old.worst_hold_rank,
            rebalance_frequency=old.rebalance_frequency,
            aum=aum or old.initial_aum,
        )

    @staticmethod
    def prepare_due_rebalances(db: Session, TODAY: Optional[date] = None) -> int:
        TODAY = TODAY or date.today()
        strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status == LiveStatus.ACTIVE,
            LiveStrategy.subscription_active == True,
        ).all()
        count = 0
        for strategy in strategies:
            if should_prepare_rebalance(TODAY, strategy.rebalance_frequency):
                try:
                    logger.info("[Rebalance] Preparing strategy %s | freq=%s", strategy.id, strategy.rebalance_frequency)
                    LiveInvestmentService.prepare_rebalance(db, strategy.id, TODAY, send_email=True)
                    count += 1
                except Exception:
                    logger.exception("[Rebalance] Error preparing strategy %s", strategy.id)
                    db.rollback()
                    continue
        logger.info("[Rebalance] Prepared %d strategies for %s", count, TODAY)
        return count

    @staticmethod
    def send_pending_rebalance_reminders(db: Session, TODAY: Optional[date] = None) -> int:
        """Morning reminder — re-send rebalance email for strategies still in REBALANCE_READY.

        Run this from a cronjob at ~8:30 AM IST on trading days.
        It finds all strategies where the user hasn't executed the rebalance yet
        and sends them the same notification email as a reminder.
        """
        TODAY = TODAY or date.today()
        strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status.in_([LiveStatus.REBALANCE_READY, LiveStatus.REBALANCE_PENDING_USER_APPROVAL]),
            LiveStrategy.subscription_active == True,
        ).all()
        count = 0
        for strategy in strategies:
            try:
                logger.info("[Reminder] Sending rebalance reminder for strategy %s", strategy.id)
                _gather_and_notify_rebalance(db, strategy, TODAY)
                count += 1
            except Exception:
                logger.exception("[Reminder] Error sending reminder for strategy %s", strategy.id)
                continue
        logger.info("[Reminder] Sent %d rebalance reminders for %s", count, TODAY)
        return count

    @staticmethod
    def daily_equity_curve_update(db: Session, TODAY: Optional[date] = None) -> int:
        """16:30 IST daily MTM — process pending fills + refresh LTP + equity curve.

        This is the Strategy Builder equivalent of equitycase_logic's daily
        update_tradelog + update_equitycurve cron that runs at 16:30.

        Flow:
        1. Process buy_stock/sell_stock/circuit_stock fills into tradelog entries
           (rows where actual_qty > 0 and updated_in_tradelog = False)
        2. Refresh LTP for all active holdings (mark-to-market)
        3. Append new equity curve row
        4. Sync strategy AUM fields
        """
        TODAY = TODAY or date.today()
        # Include stuck rebalance statuses for auto-skip
        valid_statuses = [
            LiveStatus.ACTIVE,
            LiveStatus.PENDING_USER_APPROVAL,
            LiveStatus.INITIAL_PROCESSING,
            LiveStatus.REBALANCE_READY,
            LiveStatus.REBALANCE_SELL_COMPLETE,
            LiveStatus.REBALANCE_PENDING_USER_APPROVAL,
            LiveStatus.REBALANCE_PROCESSING,
            LiveStatus.EXIT_PENDING_USER_APPROVAL,
            LiveStatus.EXIT_PROCESSING,
        ]
        strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status.in_(valid_statuses),
            LiveStrategy.subscription_active == True,
        ).all()
        count = 0
        for strategy in strategies:
            try:
                has_pending_fills = _process_strategy_daily_update(db, strategy, TODAY)

                # Auto-skip ignored or stuck rebalances (only if zero fills)
                stuck_statuses = {
                    LiveStatus.REBALANCE_READY,
                    LiveStatus.REBALANCE_PENDING_USER_APPROVAL,
                    LiveStatus.REBALANCE_PROCESSING,
                }
                if strategy.status in stuck_statuses:
                    # Check if any orders were filled before auto-skipping
                    filled_sells = db.query(LiveSellStock).filter(
                        LiveSellStock.automate_equity_ra_id == strategy.id,
                        LiveSellStock.actual_qty > 0,
                    ).count()
                    filled_buys = db.query(LiveBuyStock).filter(
                        LiveBuyStock.automate_equity_ra_id == strategy.id,
                        LiveBuyStock.actual_qty > 0,
                    ).count()
                    if filled_sells == 0 and filled_buys == 0:
                        logger.info("[Auto-Skip] Strategy %s missed rebalance (status=%s, 0 fills), resetting to ACTIVE", strategy.id, strategy.status.value if hasattr(strategy.status, "value") else strategy.status)
                        _delete_unapproved_preview_rows(db, strategy.id, include_sell=True)
                        strategy.status = LiveStatus.ACTIVE
                        strategy.next_rebalance_date = next_trading_day(next_rebalance_prepare_date(TODAY, strategy.rebalance_frequency))
                    else:
                        logger.warning("[Auto-Skip] Strategy %s has %d fills (sells=%d, buys=%d) — NOT auto-skipping from %s",
                                       strategy.id, filled_sells + filled_buys, filled_sells, filled_buys,
                                       strategy.status.value if hasattr(strategy.status, "value") else strategy.status)

                # REBALANCE_SELL_COMPLETE: sells were already filled by broker.
                # Don't auto-skip — user must manually send buy basket or call /rebalance/skip.
                if strategy.status == LiveStatus.REBALANCE_SELL_COMPLETE:
                    logger.warning("[Auto-Skip] Strategy %s in REBALANCE_SELL_COMPLETE — sells done but buys pending. NOT auto-skipping.", strategy.id)

                db.commit()
                logger.info("[DailyMTM] Updated strategy %s | aum=%.2f | pending_fills=%s",
                            strategy.id, float(strategy.final_aum or 0), has_pending_fills)
                count += 1
            except Exception:
                logger.exception("[DailyMTM] Error updating strategy %s", strategy.id)
                db.rollback()
                continue
        logger.info("[DailyMTM] Updated %d strategies for %s", count, TODAY)

        # ── Process recently-EXITED strategies with pending fills ─────────
        # When a user exits during market hours, strategy immediately becomes
        # EXITED + subscription_active=False (for frontend portfolio page).
        # But the exit sell fills still need tradelog + equity curve processing.
        # This block catches them using the same helper — no code duplication.
        exited_strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status == LiveStatus.EXITED,
            LiveStrategy.subscription_active == False,
        ).all()

        exited_count = 0
        for strategy in exited_strategies:
            try:
                # Quick check: are there ANY pending fills for this strategy?
                pending_fills = db.query(LiveBuyStock).filter(
                    LiveBuyStock.automate_equity_ra_id == strategy.id,
                    LiveBuyStock.updated_in_tradelog == False,
                    LiveBuyStock.actual_qty > 0,
                ).count() + db.query(LiveSellStock).filter(
                    LiveSellStock.automate_equity_ra_id == strategy.id,
                    LiveSellStock.updated_in_tradelog == False,
                    LiveSellStock.actual_qty > 0,
                ).count()

                if pending_fills == 0:
                    continue  # No pending fills — skip (costs <1ms)

                logger.info("[DailyMTM] Processing EXITED strategy %s — %d pending fills", strategy.id, pending_fills)
                _process_strategy_daily_update(db, strategy, TODAY)
                db.commit()
                exited_count += 1
                logger.info("[DailyMTM] Processed EXITED strategy %s | final_aum=%.2f", strategy.id, float(strategy.final_aum or 0))
            except Exception:
                logger.exception("[DailyMTM] Error processing EXITED strategy %s", strategy.id)
                db.rollback()
                continue

        if exited_count:
            logger.info("[DailyMTM] Processed %d EXITED strategies with pending fills", exited_count)

        return count

    @staticmethod
    def safety_net_verify_pending_strategies(db: Session, TODAY: Optional[date] = None) -> int:
        """Safety net: verify strategies stuck in pending states via kite.orders().

        Run as part of the 16:30 daily celery job. Catches cases where:
        - Postback was lost/never arrived
        - Orderbook verification task failed or timed out
        - Token was not available during market hours but is now

        Calls verify_and_process_from_orderbook for each stuck strategy.
        """
        TODAY = TODAY or date.today()
        pending_statuses = [
            LiveStatus.PENDING_USER_APPROVAL,
            LiveStatus.INITIAL_PROCESSING,
            LiveStatus.REBALANCE_PENDING_USER_APPROVAL,
            LiveStatus.REBALANCE_PROCESSING,
            LiveStatus.REBALANCE_SELL_COMPLETE,  # Buy basket may have been sent
            LiveStatus.EXIT_PENDING_USER_APPROVAL,
            LiveStatus.EXIT_PROCESSING,
        ]
        stuck_strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status.in_(pending_statuses),
        ).all()  # Removed subscription_active filter — initial PENDING has subscription_active=False

        count = 0
        for strategy in stuck_strategies:
            try:
                # Find the latest basket for this strategy
                basket = db.query(LivePublisherBasket).filter(
                    LivePublisherBasket.automate_equity_ra_id == strategy.id,
                ).order_by(LivePublisherBasket.created_at.desc()).first()

                if not basket:
                    continue

                logger.info("[SafetyNet] Verifying stuck strategy %s (status=%s) via orderbook",
                            strategy.id, strategy.status.value)
                result = LiveInvestmentService.verify_and_process_from_orderbook(
                    db, basket_id=str(basket.id), retry_count=0, TODAY=TODAY,
                )
                if result.get("detail") in ("processed_from_orderbook", "processed_from_postback_fallback"):
                    count += 1
                    logger.info("[SafetyNet] Resolved strategy %s → %s", strategy.id, result.get("strategy_status"))
            except Exception:
                logger.exception("[SafetyNet] Error verifying strategy %s", strategy.id)
                db.rollback()
                continue

        logger.info("[SafetyNet] Resolved %d stuck strategies for %s", count, TODAY)
        return count

    @staticmethod
    def store_daily_orderbook_backup(db: Session, TODAY: Optional[date] = None) -> int:
        """Store filtered orderbook JSON backup for broker_accounts that had orders today.

        Run as part of the 16:30 daily celery job. For each broker_account that
        had active baskets today:
        1. Call kite.orders() one final time
        2. Filter out non-tagged orders (user's personal trades)
        3. Store filtered JSON in broker_orderbook_daily (one row per account per date)

        Only stores OUR tagged orders for privacy + storage efficiency.
        """
        from app.models.live_investment import BrokerOrderbookDaily

        TODAY = TODAY or date.today()

        # Find broker_accounts that had baskets created today
        baskets_today = db.query(LivePublisherBasket).filter(
            func.date(LivePublisherBasket.created_at) == TODAY,
        ).all()

        if not baskets_today:
            logger.info("[OrderbookBackup] No baskets today — skipping backup")
            return 0

        # Collect unique broker_account_ids and all their tags
        account_tags = {}  # {broker_account_id: set(tags)}
        account_strategy_map = {}  # {broker_account_id: strategy}
        for basket in baskets_today:
            strategy = db.query(LiveStrategy).filter(
                LiveStrategy.id == basket.automate_equity_ra_id,
            ).first()
            if not strategy:
                continue

            ba_id = strategy.broker_account_id
            if ba_id not in account_tags:
                account_tags[ba_id] = set()
                account_strategy_map[ba_id] = strategy

            if basket.publisher_payload:
                for o in (basket.publisher_payload.get("basket") or []):
                    tag = o.get("tag")
                    if tag:
                        account_tags[ba_id].add(tag)

        count = 0
        for ba_id, tags in account_tags.items():
            if not tags:
                continue

            try:
                broker_account = db.query(LiveBrokerAccount).filter(
                    LiveBrokerAccount.id == ba_id,
                ).first()
                if not broker_account or not broker_account.access_token_encrypted:
                    logger.warning("[OrderbookBackup] No valid token for broker_account %s — skipping", ba_id)
                    continue
                if broker_account.token_date != TODAY:
                    logger.warning("[OrderbookBackup] Token not from today for broker_account %s — skipping", ba_id)
                    continue

                access_token = decrypt_token(broker_account.access_token_encrypted)
                if not access_token:
                    continue

                strategy = account_strategy_map[ba_id]
                adapter = get_publisher_adapter(strategy.broker)
                full_orderbook = adapter.fetch_orderbook(access_token)

                # Filter to only our tagged orders
                filtered = [
                    order for order in full_orderbook
                    if order.get("tag") in tags
                ]

                # Serialize datetime objects for JSON storage
                import json
                filtered_serializable = json.loads(
                    json.dumps(filtered, default=str)
                )

                # Upsert into broker_orderbook_daily
                existing = db.query(BrokerOrderbookDaily).filter(
                    BrokerOrderbookDaily.broker_account_id == ba_id,
                    BrokerOrderbookDaily.date == TODAY,
                ).first()

                if existing:
                    existing.order_count = len(filtered_serializable)
                    existing.filtered_orderbook = filtered_serializable
                else:
                    row = BrokerOrderbookDaily(
                        broker_account_id=ba_id,
                        broker_user_id=broker_account.broker_user_id,
                        date=TODAY,
                        order_count=len(filtered_serializable),
                        filtered_orderbook=filtered_serializable,
                    )
                    db.add(row)

                db.commit()
                count += 1
                logger.info("[OrderbookBackup] Stored %d filtered orders for broker_account %s on %s",
                            len(filtered_serializable), ba_id, TODAY)

            except Exception:
                logger.exception("[OrderbookBackup] Error storing backup for broker_account %s", ba_id)
                db.rollback()
                continue

        logger.info("[OrderbookBackup] Stored backups for %d broker_accounts on %s", count, TODAY)
        return count

    @staticmethod
    def resolve_account_mismatch(db: Session, strategy_id) -> LiveStrategy:
        """Resolve ACCOUNT_MISMATCH by re-locking correct client_id and restoring ACTIVE.

        Called when a postback arrived from the wrong broker account.
        Re-locks the client_id from the broker_account table (the expected one)
        so the user can retry with the correct account.
        """
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        if strategy.status != LiveStatus.ACCOUNT_MISMATCH:
            current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
            raise HTTPException(status_code=400, detail=f"Can only resolve from ACCOUNT_MISMATCH. Current status={current}")

        # Re-lock client_id from broker_account (the expected one)
        broker_account = db.query(LiveBrokerAccount).filter(
            LiveBrokerAccount.id == strategy.broker_account_id,
        ).first()
        strategy.locked_client_id = broker_account.broker_user_id if broker_account else None
        strategy.status = LiveStatus.ACTIVE
        db.commit()
        db.refresh(strategy)
        logger.info("[ResolveMismatch] Strategy %s restored to ACTIVE | locked_client_id=%s",
                    strategy.id, strategy.locked_client_id)
        return strategy

    @staticmethod
    def auto_timeout_stale_strategies(db: Session, TODAY: Optional[date] = None) -> int:
        """Auto-recover strategies stuck in pending states with no fills.

        Run from the daily cron (alongside daily_equity_curve_update).

        Handles two cases:
        1. PENDING_USER_APPROVAL (initial) — user never completed Kite flow
           → auto-cancel (strategy was never activated, no holdings)
        2. EXIT_PENDING_USER_APPROVAL — user clicked exit trade-now but closed Kite
           → go back to ACTIVE (strategy has real holdings, needs rebalance/MTM)
        """
        TODAY = TODAY or date.today()
        count = 0

        # Case 1: PENDING_USER_APPROVAL / INITIAL_PROCESSING → CANCELLED
        pending_strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status.in_([LiveStatus.PENDING_USER_APPROVAL, LiveStatus.INITIAL_PROCESSING]),
            LiveStrategy.updated_at < TODAY,  # Stuck since before today
        ).all()
        for strategy in pending_strategies:
            filled = db.query(LiveBuyStock).filter(
                LiveBuyStock.automate_equity_ra_id == strategy.id,
                LiveBuyStock.actual_qty > 0,
            ).count()
            if filled > 0:
                continue  # Has fills — don't auto-cancel, wait for remaining postbacks

            strategy.status = LiveStatus.CANCELLED
            strategy.subscription_active = False
            # Clean up orphaned rows and cancel baskets
            db.query(LiveBuyStock).filter(
                LiveBuyStock.automate_equity_ra_id == strategy.id,
                or_(LiveBuyStock.actual_qty.is_(None), LiveBuyStock.actual_qty == 0),
            ).delete(synchronize_session=False)
            db.query(LivePublisherBasket).filter(
                LivePublisherBasket.automate_equity_ra_id == strategy.id,
                LivePublisherBasket.status.in_(["PENDING_USER_APPROVAL", "REDIRECT_RECEIVED"]),
            ).update({"status": "CANCELLED"}, synchronize_session=False)
            logger.info("[AutoTimeout] Strategy %s auto-cancelled from %s (no fills, stuck since %s)",
                        strategy.id, strategy.status.value, strategy.updated_at)
            count += 1

        # Case 2: EXIT_PENDING_USER_APPROVAL / EXIT_PROCESSING → ACTIVE
        exit_pending_strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status.in_([LiveStatus.EXIT_PENDING_USER_APPROVAL, LiveStatus.EXIT_PROCESSING]),
            LiveStrategy.updated_at < TODAY,  # Stuck since before today
        ).all()
        for strategy in exit_pending_strategies:
            # Check if any exit sell orders were filled
            filled = db.query(LiveSellStock).filter(
                LiveSellStock.automate_equity_ra_id == strategy.id,
                LiveSellStock.method == "EXIT",
                LiveSellStock.actual_qty > 0,
            ).count()
            if filled > 0:
                continue  # Has fills — don't revert, wait for remaining postbacks

            # Clean up orphaned exit sell rows
            db.query(LiveSellStock).filter(
                LiveSellStock.automate_equity_ra_id == strategy.id,
                LiveSellStock.method == "EXIT",
                LiveSellStock.updated_in_tradelog == False,
            ).delete(synchronize_session=False)
            # Cancel stale baskets
            db.query(LivePublisherBasket).filter(
                LivePublisherBasket.automate_equity_ra_id == strategy.id,
                LivePublisherBasket.status.in_(["PENDING_USER_APPROVAL", "REDIRECT_RECEIVED"]),
            ).update({"status": "CANCELLED"}, synchronize_session=False)

            strategy.status = LiveStatus.ACTIVE
            logger.info("[AutoTimeout] Strategy %s restored to ACTIVE from %s (no fills, stuck since %s)",
                        strategy.id, strategy.status.value, strategy.updated_at)
            count += 1

        # Case 3: REBALANCE_SELL_COMPLETE → ACTIVE (auto-skip stale buy window)
        # Sells already executed (money in account), but buy rows are stale
        # (screener rankings/prices from previous day). Better to skip and let
        # next rebalance cycle use fresh data.
        rebal_stuck_strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status == LiveStatus.REBALANCE_SELL_COMPLETE,
            LiveStrategy.updated_at < TODAY,  # Stuck since before today
        ).all()
        for strategy in rebal_stuck_strategies:
            # Clean up orphaned buy rows (never sent to broker)
            _delete_unapproved_preview_rows(db, strategy.id, include_sell=False)

            strategy.status = LiveStatus.ACTIVE
            strategy.next_rebalance_date = next_trading_day(
                next_rebalance_prepare_date(TODAY, strategy.rebalance_frequency)
            )
            logger.info("[AutoTimeout] Strategy %s auto-skipped from REBALANCE_SELL_COMPLETE (buy window expired, stuck since %s)",
                        strategy.id, strategy.updated_at)
            count += 1

        if count:
            db.commit()
        logger.info("[AutoTimeout] Auto-recovered %d stale strategies", count)
        return count

    # ── Publisher redirect-callback (token exchange) ──────────────────────

    @staticmethod
    def handle_publisher_redirect_callback(
        db: Session,
        *,
        user_id,
        live_id,
        request_token: str,
    ) -> dict:
        """Handle the frontend callback after Kite Publisher redirect.

        1. Verifies the strategy belongs to the calling user.
        2. Finds the latest basket for the strategy.
        3. Exchanges request_token for access_token via the broker adapter.
        4. Encrypts and stores the access_token in broker_account.
        5. Verifies the broker user_id matches the locked strategy account.

        Returns a dict with status information for the frontend.
        """
        from app.core.encryption import encrypt_token

        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == live_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")

        # Verify the strategy belongs to this user
        if str(strategy.user_id) != str(user_id):
            raise HTTPException(status_code=403, detail="Strategy does not belong to this user")

        # Find the latest PENDING basket for this strategy (don't revive cancelled ones)
        basket = db.query(LivePublisherBasket).filter(
            LivePublisherBasket.automate_equity_ra_id == live_id,
            LivePublisherBasket.status.in_(["PENDING_USER_APPROVAL", "CREATED"]),
        ).order_by(LivePublisherBasket.created_at.desc()).first()
        if not basket:
            raise HTTPException(status_code=404, detail="Publisher basket not found")

        broker_account = db.query(LiveBrokerAccount).filter(
            LiveBrokerAccount.id == strategy.broker_account_id
        ).first()
        if not broker_account:
            raise HTTPException(status_code=404, detail="Broker account not found")

        # If already connected today, skip exchange (request_token is one-time use)
        if (
            broker_account.auth_status == "CONNECTED"
            and broker_account.token_date == date.today()
        ):
            logger.info("[RedirectCallback] Already connected today for strategy %s, skipping token exchange", live_id)
            # IMPORTANT: even when we skip the token exchange, the user has still
            # returned from a Kite redirect — orders are in-flight. We MUST still mark
            # the basket REDIRECT_RECEIVED and lock the strategy into *_PROCESSING.
            # Otherwise the status stays PENDING, and the preview endpoint then sees
            # "orders sent" and (previously) 400'd → the frontend Bad Request page.
            basket.status = "REDIRECT_RECEIVED"
            STATUS_TO_PROCESSING = {
                LiveStatus.PENDING_USER_APPROVAL: LiveStatus.INITIAL_PROCESSING,
                LiveStatus.REBALANCE_PENDING_USER_APPROVAL: LiveStatus.REBALANCE_PROCESSING,
                LiveStatus.REBALANCE_SELL_COMPLETE: LiveStatus.REBALANCE_PROCESSING,  # Buy phase
                LiveStatus.EXIT_PENDING_USER_APPROVAL: LiveStatus.EXIT_PROCESSING,
            }
            new_status = STATUS_TO_PROCESSING.get(strategy.status)
            if new_status:
                logger.info("[RedirectCallback] Strategy %s status %s → %s (already-connected path)",
                            live_id, strategy.status.value, new_status.value)
                strategy.status = new_status
            db.commit()
            return {
                "status": "ok",
                "detail": "already_connected_for_today",
                "broker_user_id": broker_account.broker_user_id,
                "strategy_status": strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status),
            }

        # Exchange request_token → access_token via broker adapter
        try:
            adapter = get_publisher_adapter(strategy.broker)
            token_data = adapter.exchange_token(request_token)
        except Exception as e:
            logger.error("[RedirectCallback] Token exchange failed for strategy %s: %s", live_id, e)
            broker_account.auth_status = "TOKEN_EXCHANGE_FAILED"
            db.commit()
            return {
                "status": "error",
                "detail": f"token_exchange_failed: {e}",
                "strategy_status": strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status),
            }

        # Verify broker user_id matches (prevent accidental login with wrong account)
        zerodha_user_id = token_data.get("user_id")
        if broker_account.broker_user_id and zerodha_user_id != broker_account.broker_user_id:
            strategy.status = LiveStatus.ACCOUNT_MISMATCH
            broker_account.auth_status = "ACCOUNT_MISMATCH"
            db.commit()
            return {
                "status": "account_mismatch",
                "expected": broker_account.broker_user_id,
                "actual": zerodha_user_id,
            }

        # Store encrypted access_token + metadata in broker_account
        broker_account.access_token_encrypted = encrypt_token(token_data["access_token"])
        broker_account.token_date = date.today()
        broker_account.token_expires_at = token_data.get("expires_at")
        broker_account.last_request_token = request_token
        broker_account.last_authorised_at = datetime.utcnow()
        broker_account.auth_status = "CONNECTED"
        broker_account.broker_profile = token_data.get("profile")

        basket.status = "REDIRECT_RECEIVED"

        # ── Upgrade to _PROCESSING status (locks strategy during order processing) ──
        STATUS_TO_PROCESSING = {
            LiveStatus.PENDING_USER_APPROVAL: LiveStatus.INITIAL_PROCESSING,
            LiveStatus.REBALANCE_PENDING_USER_APPROVAL: LiveStatus.REBALANCE_PROCESSING,
            LiveStatus.REBALANCE_SELL_COMPLETE: LiveStatus.REBALANCE_PROCESSING,  # Buy phase
            LiveStatus.EXIT_PENDING_USER_APPROVAL: LiveStatus.EXIT_PROCESSING,
        }
        new_status = STATUS_TO_PROCESSING.get(strategy.status)
        if new_status:
            logger.info("[RedirectCallback] Strategy %s status %s → %s",
                        live_id, strategy.status.value, new_status.value)
            strategy.status = new_status

        db.commit()

        logger.info("[RedirectCallback] Token exchanged successfully for strategy %s, broker_user=%s",
                     live_id, zerodha_user_id)

        return {
            "status": "ok",
            "detail": "broker_connected_for_today",
            "broker_user_id": zerodha_user_id,
        }


