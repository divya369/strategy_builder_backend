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

import logging
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Optional

import pandas as pd
import numpy as np
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import or_

from app.core.database import equity_engine
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
from app.services.screener_execution_service import screener_execution_service
from app.core.trading_calendar import next_trading_day, should_prepare_rebalance, next_rebalance_prepare_date
from app.services.broker_publishers import get_publisher_adapter
from app.core.config import settings

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Small DB dataframe helpers equivalent to database_equity.py style
# -----------------------------------------------------------------------------

def model_df(db: Session, model, automate_equity_ra_id) -> pd.DataFrame:
    q = db.query(model).filter(model.automate_equity_ra_id == automate_equity_ra_id).order_by(model.id.asc())
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
                "method": "WHR",
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
    existing_basket = db.query(LivePublisherBasket).filter(
        LivePublisherBasket.automate_equity_ra_id == strategy.id,
        LivePublisherBasket.status == "PENDING_USER_APPROVAL",
        LivePublisherBasket.basket_type == effective_basket_type,
    ).order_by(LivePublisherBasket.created_at.desc()).first()
    if existing_basket:
        return existing_basket

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

    if effective_basket_type in ("REBALANCE", "REBALANCE_SELL", "REBALANCE_BUY"):
        strategy.status = LiveStatus.REBALANCE_PENDING_USER_APPROVAL
    elif effective_basket_type == "EXIT":
        strategy.status = LiveStatus.EXIT_PENDING_USER_APPROVAL
    else:
        strategy.status = LiveStatus.PENDING_USER_APPROVAL
        strategy.subscription_active = True  # Card visible on portfolio page immediately
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
            LiveSellStock.order_id.is_(None),
            or_(LiveSellStock.actual_qty.is_(None), LiveSellStock.actual_qty == 0),
            LiveSellStock.updated_in_tradelog == False,
        ).delete(synchronize_session=False)

        db.query(LiveSellStock).filter(
            LiveSellStock.automate_equity_ra_id == strategy_id,
            LiveSellStock.broker_status.in_(["REJECTED", "CANCELLED"]),
            or_(LiveSellStock.actual_qty.is_(None), LiveSellStock.actual_qty == 0),
        ).delete(synchronize_session=False)

    db.commit()


def _validate_trade_now_status(strategy: LiveStrategy, basket_type: str, side: str = "ALL") -> None:
    if basket_type == "REBALANCE" and side == "BUY":
        expected = {LiveStatus.REBALANCE_SELL_COMPLETE, LiveStatus.REBALANCE_READY}
    elif basket_type == "REBALANCE" and side == "SELL":
        expected = {LiveStatus.REBALANCE_READY}
    else:
        expected = {
            "INITIAL": {LiveStatus.PREVIEW_READY, LiveStatus.ALL_REJECTED},
            "REBALANCE": {LiveStatus.REBALANCE_READY},
            "EXIT": {LiveStatus.EXIT_PENDING_USER_APPROVAL},
        }.get((basket_type or "INITIAL").upper(), set())
    if expected and strategy.status not in expected:
        current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
        expected_str = ", ".join(s.value for s in expected)
        raise HTTPException(status_code=400, detail=f"Cannot create {basket_type} basket when status={current}. Expected one of: {expected_str}.")


# -----------------------------------------------------------------------------
# Notification helpers
# -----------------------------------------------------------------------------

def _gather_and_notify_rebalance(db: Session, strategy: LiveStrategy, TODAY: date) -> None:
    """
    Gather rebalance details from buy/sell tables and fire notification.
    This is fire-and-forget — failures are logged but never raised.
    """
    try:
        from app.models.user import User
        from app.services.notifications import notify_all

        user = db.query(User).filter(User.id == strategy.user_id).first()
        if not user or not user.email:
            logger.warning("[Notify] No user/email found for strategy %s — skipping notification", strategy.id)
            return

        # Sells = stocks being REMOVED in this rebalance
        sells = db.query(LiveSellStock).filter(
            LiveSellStock.automate_equity_ra_id == strategy.id,
            LiveSellStock.date == TODAY,
            LiveSellStock.updated_in_tradelog == False,
        ).all()

        # Buys = stocks being ADDED in this rebalance
        buys = db.query(LiveBuyStock).filter(
            LiveBuyStock.automate_equity_ra_id == strategy.id,
            LiveBuyStock.date == TODAY,
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
            logger.info("[Notify] No buy/sell changes for strategy %s — skipping notification", strategy.id)
            return

        from datetime import datetime
        import pytz
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
            user_email=user.email,
            user_name=user.full_name,
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
        if strategy.status not in {LiveStatus.DRAFT, LiveStatus.PREVIEW_READY, LiveStatus.ALL_REJECTED}:
            current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
            raise HTTPException(status_code=400, detail=f"Preview can be generated only from DRAFT/PREVIEW_READY/ALL_REJECTED. Current status={current}")
        _delete_unapproved_preview_rows(db, strategy.id, include_sell=False)
        screener_df = get_strategy_builder_screener_df(db, strategy.screener_version_id, strategy.portfolio_size)
        tradelog_df = model_df(db, LiveTradelog, strategy.id)
        buy_screener_df = screener_df.head(strategy.portfolio_size).copy()
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

        return assign_publisher_tags(db, strategy, basket_type, side)

    @staticmethod
    def prepare_rebalance(db: Session, strategy_id, TODAY: Optional[date] = None) -> LiveStrategy:
        TODAY = TODAY or date.today()
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        if strategy.status in {LiveStatus.REBALANCE_SELL_COMPLETE, LiveStatus.REBALANCE_PENDING_USER_APPROVAL}:
            # Sell basket already sent/completed — buy/sell rows already exist.
            # Just return the strategy so frontend can render the preview page.
            return strategy
        if strategy.status not in {LiveStatus.ACTIVE, LiveStatus.REBALANCE_READY}:
            raise HTTPException(status_code=400, detail=f"Strategy must be ACTIVE or REBALANCE_READY to prepare rebalance. Current status={strategy.status.value}")
        _delete_unapproved_preview_rows(db, strategy.id, include_sell=True)

        screener_df = get_strategy_builder_screener_df(db, strategy.screener_version_id, max(strategy.portfolio_size, strategy.worst_hold_rank))
        tradelog_df = model_df(db, LiveTradelog, strategy.id)
        equitycurve_df = model_df(db, LiveEquityCurve, strategy.id)
        latest = equitycurve_df.iloc[-1] if not equitycurve_df.empty else {"aum": strategy.final_aum, "cash": strategy.cash}
        aum = float(latest.get("aum") or strategy.final_aum or strategy.initial_aum)
        cash = float(latest.get("cash") or strategy.cash or 0.0)

        sell_df = get_sell_df_investment(TODAY, strategy, f"({strategy.id})", screener_df, tradelog_df, strategy.worst_hold_rank)
        insert_df_to_db(db, sell_df, LiveSellStock)
        amount_to_sell_tomorrow = float(sell_df["amount"].sum()) if not sell_df.empty else 0.0
        cash_available = (cash + (amount_to_sell_tomorrow * 0.80)) * 0.98
        buy_df = get_buy_df_investment(
            TODAY, strategy, f"({strategy.id})", screener_df.head(strategy.portfolio_size), tradelog_df,
            sell_stock_count=len(sell_df),
            portfolio_size=strategy.portfolio_size,
            cash_available=cash_available,
            aum=aum,
        )
        insert_df_to_db(db, buy_df, LiveBuyStock)
        strategy.status = LiveStatus.REBALANCE_READY
        db.commit()
        db.refresh(strategy)

        # Fire-and-forget rebalance notification (email)
        _gather_and_notify_rebalance(db, strategy, TODAY)

        return strategy

    @staticmethod
    def create_exit_preview(db: Session, strategy_id, TODAY: Optional[date] = None) -> LiveStrategy:
        TODAY = TODAY or date.today()
        strategy = db.query(LiveStrategy).filter(LiveStrategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail="Live strategy not found")
        if strategy.status not in {LiveStatus.ACTIVE, LiveStatus.EXIT_PENDING_USER_APPROVAL}:
            current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
            raise HTTPException(status_code=400, detail=f"Exit preview can be generated only from ACTIVE or EXIT_PENDING_USER_APPROVAL. Current status={current}")
        _delete_unapproved_preview_rows(db, strategy.id, include_sell=True)
        tradelog_df = model_df(db, LiveTradelog, strategy.id)
        active_df = tradelog_df.loc[tradelog_df["active"] == True] if not tradelog_df.empty else pd.DataFrame()
        rows = []
        for _, row in active_df.iterrows():
            remaining_qty = int(row["buy_qty"] or 0) - int(row["sell_qty"] or 0)
            if remaining_qty <= 0:
                continue
            price = float(row.get("ltp") or row.get("buy_price") or 0)
            rows.append({
                "automate_equity_ra_id": strategy.id,
                "tradingsymbol": row["tradingsymbol"],
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
        strategy.status = LiveStatus.EXIT_PENDING_USER_APPROVAL
        db.commit()
        db.refresh(strategy)
        return strategy

    @staticmethod
    def update_from_postback(db: Session, payload: Dict[str, Any], TODAY: Optional[date] = None) -> dict:
        """Process a single broker postback. Only updates buy/sell/circuit stock tables.

        Tradelog and equitycurve are NOT updated here — they are updated by the
        daily 16:30 celery job (daily_equity_curve_update).

        Returns a dict with status info for the API response.
        """
        TODAY = TODAY or date.today()

        # Terminal statuses — only these should trigger fill processing
        TERMINAL_STATUSES = {"COMPLETE", "REJECTED", "CANCELLED"}

        logger.info("[Postback] Received | tag=%s order_id=%s status=%s symbol=%s txn=%s",
                    payload.get("tag"), payload.get("order_id"), payload.get("status"),
                    payload.get("tradingsymbol"), payload.get("transaction_type"))

        raw_tag = payload.get("tag")  # Our unique 8-char publisher_tag per order
        kite_order_id = payload.get("order_id")  # Kite's unique order ID
        postback_symbol = payload.get("tradingsymbol")

        if not raw_tag and not kite_order_id:
            raise HTTPException(status_code=400, detail="Postback missing both tag and order_id")
        if not postback_symbol:
            raise HTTPException(status_code=400, detail="Postback missing tradingsymbol")

        # ── Step 1: Find the specific order row by publisher_tag (unique per order) ──
        row_obj = None
        order_table = None  # Track which table the order is in
        if raw_tag:
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
        payload = adapter.normalize_postback(payload)

        # ── VERIFICATION GUARD 1: Checksum (anti-tampering) ──────────────
        checksum_result = adapter.verify_checksum(payload.get("raw") or payload)
        if checksum_result is False:
            logger.warning("[Postback] CHECKSUM FAILED | tag=%s strategy=%s — rejecting", raw_tag, strategy.id)
            raise HTTPException(status_code=403, detail="Postback checksum verification failed — possible tampering")

        # ── VERIFICATION GUARD 2: Duplicate postback protection ──────────
        if hasattr(row_obj, "broker_status") and row_obj.broker_status in TERMINAL_STATUSES:
            logger.info("[Postback] DUPLICATE skipped | tag=%s symbol=%s — already has terminal status=%s",
                        raw_tag, postback_symbol, row_obj.broker_status)
            return {"status": "ok", "detail": "already_processed"}

        # ── Step 4: Store postback data on the order row ─────────────────
        if kite_order_id:
            row_obj.order_id = kite_order_id
        row_obj.broker_status = payload.get("status")
        row_obj.broker_status_message = payload.get("status_message")
        row_obj.broker_raw_postback = payload

        # ── VERIFICATION GUARD 3: Client ID lock ─────────────────────────
        client_id = payload.get("client_id") or payload.get("user_id")
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

        # ── Step 5: Non-terminal status — just store data, don't process ─
        broker_status = (payload.get("status") or "").upper()
        if broker_status not in TERMINAL_STATUSES:
            logger.info("[Postback] Non-terminal status=%s | tag=%s — stored, not processed", broker_status, raw_tag)
            db.commit()
            return {"status": "ok", "detail": f"non_terminal_{broker_status.lower()}"}

        # ── Step 6: Process terminal status — update fill data ───────────
        filled_qty = int(payload.get("filled_quantity") or 0)
        avg_price = float(payload.get("average_price") or 0)

        if broker_status == "COMPLETE" and filled_qty > 0:
            row_obj.actual_qty = filled_qty
            row_obj.actual_price = avg_price
            row_obj.actual_amount = round(filled_qty * avg_price, 2)
            row_obj.circuit = filled_qty < int(row_obj.qty)
            row_obj.updated_in_tradelog = False  # Will be processed by daily celery job
            logger.info("[Postback] FILLED | tag=%s symbol=%s qty=%d price=%.2f",
                        raw_tag, postback_symbol, filled_qty, avg_price)

        elif broker_status == "REJECTED":
            row_obj.actual_qty = 0
            row_obj.actual_price = 0.0
            row_obj.actual_amount = 0.0
            row_obj.circuit = False  # REJECTED = don't retry via circuit
            row_obj.updated_in_tradelog = True  # Mark as "done" so pending count drops
            logger.info("[Postback] REJECTED | tag=%s symbol=%s reason=%s",
                        raw_tag, postback_symbol, payload.get("status_message"))

        elif broker_status == "CANCELLED":
            partial_qty = filled_qty if filled_qty > 0 else 0
            if partial_qty > 0:
                # Partial fill before cancellation
                row_obj.actual_qty = partial_qty
                row_obj.actual_price = avg_price
                row_obj.actual_amount = round(partial_qty * avg_price, 2)
                row_obj.circuit = True  # Partial fill → circuit for remaining
                row_obj.updated_in_tradelog = False
            else:
                # Fully cancelled with no fill
                row_obj.actual_qty = 0
                row_obj.actual_price = 0.0
                row_obj.actual_amount = 0.0
                row_obj.circuit = True  # Cancelled → circuit for retry
                row_obj.updated_in_tradelog = True
            logger.info("[Postback] CANCELLED | tag=%s symbol=%s partial_qty=%d",
                        raw_tag, postback_symbol, partial_qty)

        # Create circuit stock row if needed (only for circuit=True)
        if row_obj.circuit and order_table in ("buy", "sell"):
            lower_upper = "upper" if order_table == "buy" else "lower"
            circuit_row = LiveCircuitStock(
                automate_equity_ra_id=strategy.id,
                tradingsymbol=row_obj.tradingsymbol,
                isin=getattr(row_obj, "isin", ""),
                date=TODAY,
                qty=int(row_obj.qty) - int(row_obj.actual_qty or 0),  # Remaining unfilled qty
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

        db.flush()

        # ── Step 7: Check basket completion — count ONLY current basket orders ─
        # Get the latest basket and extract its tags to scope counts correctly.
        # Without this, rebalance would count old initial basket rows too.
        latest_basket = db.query(LivePublisherBasket).filter(
            LivePublisherBasket.automate_equity_ra_id == strategy.id,
        ).order_by(LivePublisherBasket.created_at.desc()).first()

        basket_tags = []
        if latest_basket and latest_basket.publisher_payload:
            basket_tags = [
                o.get("tag") for o in (latest_basket.publisher_payload.get("basket") or [])
                if o.get("tag")
            ]

        if not basket_tags:
            # No tags found — can't determine basket scope, commit and return
            db.commit()
            db.refresh(strategy)
            return {"status": "ok", "strategy_status": strategy.status.value if hasattr(strategy.status, 'value') else str(strategy.status)}

        # Count pending orders — ONLY among current basket's tags
        pending_buy = db.query(LiveBuyStock).filter(
            LiveBuyStock.publisher_tag.in_(basket_tags),
            LiveBuyStock.broker_status.is_(None),
        ).count()
        pending_sell = db.query(LiveSellStock).filter(
            LiveSellStock.publisher_tag.in_(basket_tags),
            LiveSellStock.broker_status.is_(None),
        ).count()

        # Also count orders with non-terminal status (UPDATE, OPEN, etc.)
        pending_buy += db.query(LiveBuyStock).filter(
            LiveBuyStock.publisher_tag.in_(basket_tags),
            LiveBuyStock.broker_status.isnot(None),
            ~LiveBuyStock.broker_status.in_(list(TERMINAL_STATUSES)),
        ).count()
        pending_sell += db.query(LiveSellStock).filter(
            LiveSellStock.publisher_tag.in_(basket_tags),
            LiveSellStock.broker_status.isnot(None),
            ~LiveSellStock.broker_status.in_(list(TERMINAL_STATUSES)),
        ).count()

        if pending_buy == 0 and pending_sell == 0:
            # All orders in current basket have terminal status — basket is complete
            filled_buy = db.query(LiveBuyStock).filter(
                LiveBuyStock.publisher_tag.in_(basket_tags),
                LiveBuyStock.broker_status == "COMPLETE",
            ).count()
            filled_sell = db.query(LiveSellStock).filter(
                LiveSellStock.publisher_tag.in_(basket_tags),
                LiveSellStock.broker_status == "COMPLETE",
            ).count()
            total_filled = filled_buy + filled_sell

            # Get rejection reasons for frontend
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

            previous_status = strategy.status

            if total_filled == 0:
                # ALL orders rejected/cancelled — no fills at all
                # Transition depends on what basket type was running:
                if previous_status == LiveStatus.EXIT_PENDING_USER_APPROVAL:
                    # Stay in EXIT_PENDING so user can retry exit
                    pass  # status stays EXIT_PENDING_USER_APPROVAL
                elif previous_status == LiveStatus.REBALANCE_PENDING_USER_APPROVAL:
                    # Go back to appropriate retry status based on basket type
                    if latest_basket and latest_basket.basket_type == "REBALANCE_SELL":
                        # Sell basket all rejected → go back to REBALANCE_READY so user can retry sell
                        strategy.status = LiveStatus.REBALANCE_READY
                    elif latest_basket and latest_basket.basket_type == "REBALANCE_BUY":
                        # Buy basket all rejected → go back to REBALANCE_SELL_COMPLETE so user can retry buy
                        strategy.status = LiveStatus.REBALANCE_SELL_COMPLETE
                    else:
                        # Old-style REBALANCE (ALL) basket → go back to REBALANCE_READY
                        strategy.status = LiveStatus.REBALANCE_READY
                else:
                    # Initial basket — go to ALL_REJECTED (user can retry Trade Now)
                    strategy.status = LiveStatus.ALL_REJECTED
                logger.warning("[Postback] ALL orders failed | strategy=%s prev_status=%s new_status=%s reasons=%s",
                               strategy.id, previous_status.value, strategy.status.value, rejected_orders)
            elif previous_status == LiveStatus.EXIT_PENDING_USER_APPROVAL:
                strategy.status = LiveStatus.EXITED
                strategy.subscription_active = False
            elif latest_basket and latest_basket.basket_type == "REBALANCE_SELL":
                # Sell basket complete → transition to REBALANCE_SELL_COMPLETE (enable Buy button)
                strategy.status = LiveStatus.REBALANCE_SELL_COMPLETE
                logger.info("[Postback] REBALANCE_SELL complete | strategy=%s — now awaiting BUY basket",
                            strategy.id)
            else:
                # INITIAL, REBALANCE (ALL), REBALANCE_BUY, EXIT — go to ACTIVE
                strategy.status = LiveStatus.ACTIVE
                strategy.subscription_active = True
                strategy.next_rebalance_date = next_trading_day(next_rebalance_prepare_date(TODAY, strategy.rebalance_frequency))

            # Update Publisher basket status
            if latest_basket:
                latest_basket.raw_postback = payload
                if total_filled == 0:
                    latest_basket.status = "ALL_REJECTED"
                else:
                    latest_basket.status = "COMPLETE"

        db.commit()
        db.refresh(strategy)
        logger.info("[Postback] Processed | tag=%s strategy=%s status=%s",
                    raw_tag, strategy.id, strategy.status.value if hasattr(strategy.status, 'value') else strategy.status)
        return {"status": "ok", "strategy_status": strategy.status.value if hasattr(strategy.status, 'value') else str(strategy.status)}

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
                    LiveInvestmentService.prepare_rebalance(db, strategy.id, TODAY)
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
            LiveStrategy.status == LiveStatus.REBALANCE_READY,
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
        strategies = db.query(LiveStrategy).filter(
            LiveStrategy.status == LiveStatus.ACTIVE,
            LiveStrategy.subscription_active == True,
        ).all()
        count = 0
        for strategy in strategies:
            try:
                buy_df = model_df(db, LiveBuyStock, strategy.id)
                sell_df = model_df(db, LiveSellStock, strategy.id)
                circuit_df = model_df(db, LiveCircuitStock, strategy.id)
                tradelog_df = model_df(db, LiveTradelog, strategy.id)
                equitycurve_df = model_df(db, LiveEquityCurve, strategy.id)

                # Step 1: Process pending fills into tradelog entries
                # update_tradelog_buy_df skips rows where:
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

                # Step 2: Save tradelog first (LTP already refreshed inside update_tradelog)
                upsert_df_to_db(db, tradelog_df, LiveTradelog, commit=False)

                # Mark buy/sell/circuit as processed (updated_in_tradelog = True)
                if has_pending_fills:
                    update_df_to_db(db, buy_df, LiveBuyStock, commit=False)
                    update_df_to_db(db, sell_df, LiveSellStock, commit=False)
                    update_df_to_db(db, circuit_df, LiveCircuitStock, commit=False)

                if tradelog_df.empty:
                    logger.info("[DailyMTM] Skipping equity curve for strategy %s — no tradelog rows", strategy.id)
                    db.commit()
                    count += 1
                    continue

                # Step 3: Append today's equity curve row
                rebalance = has_pending_fills  # Cash delta only on fill days
                equitycurve_df = update_equitycurve(TODAY, strategy, tradelog_df, equitycurve_df, today_cash, rebalance)
                insert_single_row_to_db(db, equitycurve_df.iloc[-1].to_dict(), LiveEquityCurve, commit=False)

                # Step 4: Sync strategy AUM fields — single atomic commit below
                last = equitycurve_df.iloc[-1]
                strategy.cash = float(last["cash"] or 0)
                strategy.stock_value = float(last["stocks_value"] or 0)
                strategy.final_aum = float(last["aum"] or 0)
                strategy.pnl = float(last["total_pnl"] or 0)
                strategy.todays_pnl = float(last["strategy_daily_return"] or 0)
                db.commit()
                logger.info("[DailyMTM] Updated strategy %s | aum=%.2f | pending_fills=%s",
                            strategy.id, float(strategy.final_aum or 0), has_pending_fills)
                count += 1
            except Exception:
                logger.exception("[DailyMTM] Error updating strategy %s", strategy.id)
                db.rollback()
                continue
        logger.info("[DailyMTM] Updated %d strategies for %s", count, TODAY)
        return count

