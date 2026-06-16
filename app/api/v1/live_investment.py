import json
import logging
from datetime import date
from typing import Any, Dict, List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.live_investment import (
    LiveStrategy,
    LiveStatus,
    LiveBrokerAccount,
    LiveBuyStock,
    LiveSellStock,
    LiveCircuitStock,
    LiveTradelog,
    LiveEquityCurve,
    LivePublisherBasket,
)
from app.schemas.live_investment import (
    GoLiveRequest,
    BrokerAccountResponse,
    LiveStrategyResponse,
    PreviewStockItem,
    PreviewResponse,
    TradeNowResponse,
    DuplicateRequest,
    LiveDashboardResponse,
    LiveStrategyListItem,
    TradelogHoldingResponse,
    EquityCurvePointResponse,
    EquityCurveGraphPoint,
    PendingBasketResponse,
    PortfolioSummaryResponse,
    OrderStatusResponse,
    OrderDetail,
)
from app.services.live_investment_service import LiveInvestmentService
from app.core.trading_calendar import require_market_open

router = APIRouter()

logger = logging.getLogger(__name__)


@router.post("/publisher/postback")
async def publisher_postback(request: Request, db: Session = Depends(get_db)):
    """Receive raw Kite Publisher / broker postback.

    Per Kite docs: 'The JSON payload is posted as a raw HTTP POST body.
    You will have to read the raw body and then decode it.'
    Kite may NOT set Content-Type header, so we always read raw body.
    """
    body = await request.body()
    logger.info("[Postback] Raw body received (%d bytes): %s", len(body), body[:500])

    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error("[Postback] Failed to decode JSON from raw body: %s", e)
        raise HTTPException(status_code=400, detail=f"Invalid JSON in postback body: {e}")

    logger.info("[Postback] Decoded payload: %s", payload)
    result = LiveInvestmentService.update_from_postback(db, payload, date.today())
    return result


def serialize_broker_account(obj: LiveBrokerAccount) -> BrokerAccountResponse:
    return BrokerAccountResponse(
        id=obj.id,
        broker=obj.broker,
        label=obj.broker_account_label,
        expected_client_id=obj.broker_user_id,
        is_active=obj.is_active,
    )

# Please do not scan this and edit.
# @router.get("/broker-accounts", response_model=list[BrokerAccountResponse])
# def list_broker_accounts(db: Session = Depends(get_db)):
#     rows = db.query(LiveBrokerAccount).filter(
#         LiveBrokerAccount.user_id == SYSTEM_USER_ID,
#         LiveBrokerAccount.is_active == True,
#     ).order_by(LiveBrokerAccount.broker.asc(), LiveBrokerAccount.broker_account_label.asc()).all()
#     return [serialize_broker_account(x) for x in rows]


def serialize_strategy(obj: LiveStrategy) -> LiveStrategyResponse:
    return LiveStrategyResponse(
        id=obj.id,
        version_number=obj.screener_version.version_number if obj.screener_version else 0,
        status=obj.status.value if hasattr(obj.status, "value") else str(obj.status),
        strategy_name=obj.strategy_name,
        portfolio_size=obj.portfolio_size,
        wrh=obj.worst_hold_rank,
        rebalance_frequency=obj.rebalance_frequency,
        initial_aum=obj.initial_aum,
        final_aum=obj.final_aum,
        broker=obj.broker,
        broker_user_id=obj.locked_client_id,
        broker_account_label=obj.broker_account_label,
        next_rebalance_date=obj.next_rebalance_date,
    )


def _build_preview_response(db: Session, obj: LiveStrategy) -> PreviewResponse:
    """Build preview response with pending buy/sell stock lists + remaining cash."""
    buy_rows = db.query(LiveBuyStock).filter(
        LiveBuyStock.automate_equity_ra_id == obj.id,
        LiveBuyStock.updated_in_tradelog == False,
    ).all()
    sell_rows = db.query(LiveSellStock).filter(
        LiveSellStock.automate_equity_ra_id == obj.id,
        LiveSellStock.updated_in_tradelog == False,
    ).all()

    buy_stocks = [
        PreviewStockItem(
            tradingsymbol=r.tradingsymbol,
            isin=r.isin,
            qty=r.qty,
            price=float(r.price or 0),
            amount=float(r.amount or 0),
        ) for r in buy_rows
    ]
    sell_stocks = [
        PreviewStockItem(
            tradingsymbol=r.tradingsymbol,
            isin=r.isin,
            qty=r.qty,
            price=float(r.price or 0),
            amount=float(r.amount or 0),
        ) for r in sell_rows
    ]

    total_buy = sum(s.amount for s in buy_stocks)
    total_sell = sum(s.amount for s in sell_stocks)
    remaining_cash = float(obj.cash or 0) - total_buy + total_sell

    return PreviewResponse(
        strategy=serialize_strategy(obj),
        buy_stocks=buy_stocks,
        sell_stocks=sell_stocks,
        total_buy_amount=round(total_buy, 2),
        total_sell_amount=round(total_sell, 2),
        remaining_cash=round(remaining_cash, 2),
        broker=obj.broker,
        broker_account_label=obj.broker_account_label,
        broker_user_id=obj.locked_client_id,
    )


@router.post("/go-live", response_model=LiveStrategyResponse)
def go_live(payload: GoLiveRequest, db: Session = Depends(get_db), _mkt=Depends(require_market_open)):
    """
    User clicks Go Live.

    Collects strategy params (portfolio_size, wrh, rebalance_frequency, aum)
    and broker info (broker, broker_user_id, broker_account_label) in one step.
    Broker account is created-or-found inline.
    """
    # ── Inline broker account upsert (replaces old POST /broker-accounts) ──
    broker_value = payload.broker
    existing_account = db.query(LiveBrokerAccount).filter(
        LiveBrokerAccount.user_id == payload.user_id,
        LiveBrokerAccount.broker == broker_value,
        LiveBrokerAccount.broker_account_label == payload.broker_account_label,
    ).first()
    if existing_account:
        broker_account = existing_account
    else:
        broker_account = LiveBrokerAccount(
            user_id=payload.user_id,
            broker=broker_value,
            broker_account_label=payload.broker_account_label,
            broker_user_id=payload.broker_user_id,
        )
        db.add(broker_account)
        db.commit()
        db.refresh(broker_account)

    # ── Guard: same broker_user_id + screener_version_id already running? ──
    # Only truly running strategies should block. DRAFT/PREVIEW_READY/CANCELLED
    # are not running — user may have abandoned the flow.
    running_statuses = {
        LiveStatus.PENDING_USER_APPROVAL, LiveStatus.ACTIVE,
        LiveStatus.REBALANCE_READY, LiveStatus.REBALANCE_PENDING_USER_APPROVAL,
        LiveStatus.REBALANCE_SELL_COMPLETE,
        LiveStatus.EXIT_PENDING_USER_APPROVAL,
    }
    duplicate = db.query(LiveStrategy).filter(
        LiveStrategy.screener_version_id == payload.screener_version_id,
        LiveStrategy.locked_client_id == payload.broker_user_id,
        LiveStrategy.status.in_(running_statuses),
    ).first()
    if duplicate:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "This broker user id is already running this strategy version. Please enter another broker user id.",
                "broker_user_id": payload.broker_user_id,
            },
        )

    obj = LiveInvestmentService.create_go_live(
        db,
        user_id=payload.user_id,
        screener_version_id=payload.screener_version_id,
        strategy_name=payload.strategy_name,
        broker_account_id=broker_account.id,
        portfolio_size=payload.portfolio_size,
        wrh=payload.wrh,
        rebalance_frequency=payload.rebalance_frequency,
        aum=payload.aum,
        TODAY=date.today(),
    )
    return serialize_strategy(obj)


@router.post("/{live_id}/preview", response_model=PreviewResponse)
def create_initial_preview(live_id: UUID, db: Session = Depends(get_db), _mkt=Depends(require_market_open)):
    obj = LiveInvestmentService.create_initial_preview(db, live_id, date.today())
    return _build_preview_response(db, obj)


@router.post("/{live_id}/trade-now", response_model=TradeNowResponse)
def trade_now(live_id: UUID, db: Session = Depends(get_db), _mkt=Depends(require_market_open)):
    basket = LiveInvestmentService.trade_now(db, live_id, "INITIAL")
    strategy = db.query(LiveStrategy).filter(LiveStrategy.id == live_id).first()
    return TradeNowResponse(
        live_id=live_id,
        basket_key=basket.basket_key,
        basket_type=basket.basket_type,
        status=basket.status,
        broker=strategy.broker,
        broker_account_label=strategy.broker_account_label,
        publisher_payload=basket.publisher_payload,
    )


@router.post("/{live_id}/cancel")
def cancel_strategy(live_id: UUID, db: Session = Depends(get_db)):
    """Cancel a strategy that hasn't been fully activated yet.

    Called by frontend when Kite redirects back with status=cancelled.
    This marks the strategy as CANCELLED so the user can retry Go Live
    with the same screener + broker.
    """
    strategy = db.query(LiveStrategy).filter(LiveStrategy.id == live_id).first()
    if not strategy:
        raise HTTPException(status_code=404, detail="Live strategy not found")

    # Only allow cancel for strategies that are not yet active
    cancellable_statuses = {
        LiveStatus.DRAFT,
        LiveStatus.PREVIEW_READY,
        LiveStatus.PENDING_USER_APPROVAL,
        LiveStatus.ALL_REJECTED,
    }
    if strategy.status not in cancellable_statuses:
        current = strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status)
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel strategy in status={current}. Only {', '.join(s.value for s in cancellable_statuses)} can be cancelled.",
        )

    # Guard: if any orders were already filled, don't allow cancel — partial fills exist
    filled_count = db.query(LiveBuyStock).filter(
        LiveBuyStock.automate_equity_ra_id == live_id,
        LiveBuyStock.actual_qty > 0,
    ).count()
    filled_count += db.query(LiveSellStock).filter(
        LiveSellStock.automate_equity_ra_id == live_id,
        LiveSellStock.actual_qty > 0,
    ).count()
    if filled_count > 0:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel — {filled_count} orders were already filled by broker. Wait for all postbacks to complete.",
        )

    # Clean up: delete only unfilled preview/pending rows
    db.query(LiveBuyStock).filter(LiveBuyStock.automate_equity_ra_id == live_id).delete(synchronize_session=False)
    db.query(LiveSellStock).filter(LiveSellStock.automate_equity_ra_id == live_id).delete(synchronize_session=False)
    db.query(LiveCircuitStock).filter(LiveCircuitStock.automate_equity_ra_id == live_id).delete(synchronize_session=False)

    # Cancel any pending publisher baskets
    db.query(LivePublisherBasket).filter(
        LivePublisherBasket.automate_equity_ra_id == live_id,
        LivePublisherBasket.status == "PENDING_USER_APPROVAL",
    ).update({"status": "CANCELLED"}, synchronize_session=False)

    strategy.status = LiveStatus.CANCELLED
    strategy.subscription_active = False
    db.commit()
    db.refresh(strategy)
    return {"status": "ok", "strategy_status": "CANCELLED"}


@router.post("/{live_id}/rebalance/preview", response_model=PreviewResponse)
def prepare_rebalance(live_id: UUID, db: Session = Depends(get_db), _mkt=Depends(require_market_open)):
    obj = LiveInvestmentService.prepare_rebalance(db, live_id, date.today())
    return _build_preview_response(db, obj)


@router.post("/{live_id}/rebalance/skip", response_model=LiveStrategyResponse)
def skip_empty_rebalance(live_id: UUID, db: Session = Depends(get_db)):
    """Skip an empty or midway rebalance and return strategy to ACTIVE status."""
    obj = LiveInvestmentService.skip_empty_rebalance(db, live_id, date.today())
    return serialize_strategy(obj)


@router.post("/{live_id}/rebalance/trade-now", response_model=TradeNowResponse)
def rebalance_trade_now(
    live_id: UUID,
    side: str = Query("SELL", pattern="^(ALL|SELL|BUY)$"),
    db: Session = Depends(get_db),
    _mkt=Depends(require_market_open),
):
    basket = LiveInvestmentService.trade_now(db, live_id, "REBALANCE", side=side)
    strategy = db.query(LiveStrategy).filter(LiveStrategy.id == live_id).first()
    return TradeNowResponse(
        live_id=live_id,
        basket_key=basket.basket_key,
        basket_type=basket.basket_type,
        status=basket.status,
        broker=strategy.broker,
        broker_account_label=strategy.broker_account_label,
        publisher_payload=basket.publisher_payload,
    )


@router.post("/{live_id}/exit/preview", response_model=PreviewResponse)
def exit_preview(live_id: UUID, db: Session = Depends(get_db), _mkt=Depends(require_market_open)):
    obj = LiveInvestmentService.create_exit_preview(db, live_id, date.today())
    return _build_preview_response(db, obj)


@router.post("/{live_id}/exit/trade-now", response_model=TradeNowResponse)
def exit_trade_now(live_id: UUID, db: Session = Depends(get_db), _mkt=Depends(require_market_open)):
    basket = LiveInvestmentService.trade_now(db, live_id, "EXIT")
    strategy = db.query(LiveStrategy).filter(LiveStrategy.id == live_id).first()
    return TradeNowResponse(
        live_id=live_id,
        basket_key=basket.basket_key,
        basket_type=basket.basket_type,
        status=basket.status,
        broker=strategy.broker,
        broker_account_label=strategy.broker_account_label,
        publisher_payload=basket.publisher_payload,
    )


@router.post("/{live_id}/duplicate", response_model=LiveStrategyResponse)
def duplicate_strategy(live_id: UUID, payload: DuplicateRequest, db: Session = Depends(get_db)):
    obj = LiveInvestmentService.duplicate_strategy(
        db,
        live_id,
        broker_account_id=payload.broker_account_id,
        strategy_name=payload.strategy_name,
        aum=payload.aum,
    )
    return serialize_strategy(obj)


# ── GET endpoints ─────────────────────────────────────────────────────────────

@router.get("/strategies/{user_id}", response_model=List[LiveStrategyListItem])
def list_strategies(
    user_id: str,
    status: Optional[str] = Query(None, description="Filter by status (e.g. ACTIVE, DRAFT, EXITED)"),
    db: Session = Depends(get_db),
):
    """List all live strategies for the current user.

    By default returns only running strategies (subscription_active=True).
    Use ?status=ALL to see everything, or ?status=EXITED for exited only.
    """
    query = db.query(LiveStrategy).filter(
        LiveStrategy.user_id == user_id,
    )
    if status and status.upper() == "ALL":
        pass  # no filter — show everything
    elif status:
        query = query.filter(LiveStrategy.status == status.upper())
    else:
        # Default: show running strategies (subscription_active=True)
        query = query.filter(LiveStrategy.subscription_active == True)
    rows = query.order_by(LiveStrategy.created_at.desc()).all()
    return [
        LiveStrategyListItem(
            id=r.id,
            version_number=r.screener_version.version_number if r.screener_version else 0,
            status=r.status.value if hasattr(r.status, "value") else str(r.status),
            strategy_name=r.strategy_name,
            broker_user_id=r.locked_client_id,
            broker_account_label=r.broker_account_label,
            initial_aum=float(r.initial_aum or 0),
            final_aum=float(r.final_aum or 0),
            pnl=float(r.pnl or 0),
            subscription_active=r.subscription_active,
        )
        for r in rows
    ]


@router.get("/portfolio-summary/{user_id}", response_model=PortfolioSummaryResponse)
def portfolio_summary(user_id: str, db: Session = Depends(get_db)):
    """Aggregate summary across all running strategies (subscription_active=True).

    Returns: total_investment, current_investment, total_pnl, todays_pnl, strategy_count.
    """
    rows = db.query(LiveStrategy).filter(
        LiveStrategy.user_id == user_id,
        LiveStrategy.subscription_active == True,
    ).all()

    total_investment = sum(float(r.initial_aum or 0) for r in rows)
    current_investment = sum(float(r.final_aum or 0) for r in rows)
    total_pnl = sum(float(r.pnl or 0) for r in rows)
    todays_pnl = sum(float(r.todays_pnl or 0) for r in rows)

    return PortfolioSummaryResponse(
        total_investment=round(total_investment, 2),
        current_investment=round(current_investment, 2),
        total_pnl=round(total_pnl, 2),
        todays_pnl=round(todays_pnl, 2),
        strategy_count=len(rows),
    )


@router.get("/{live_id}/order-status", response_model=OrderStatusResponse)
def get_order_status(live_id: UUID, db: Session = Depends(get_db)):
    """Per-order status for the current basket.

    Returns filled/rejected/pending/cancelled counts and per-order details
    with rejection reasons. Frontend uses this to:
    - Show progress while orders are processing (poll every 5s)
    - Show rejection reasons when ALL_REJECTED
    - Show partial fill details
    """
    strategy = db.query(LiveStrategy).filter(LiveStrategy.id == live_id).first()
    if not strategy:
        raise HTTPException(status_code=404, detail="Live strategy not found")

    # Get current basket's tags to scope to latest basket only
    latest_basket = db.query(LivePublisherBasket).filter(
        LivePublisherBasket.automate_equity_ra_id == live_id,
    ).order_by(LivePublisherBasket.created_at.desc()).first()

    basket_tags = set()
    if latest_basket and latest_basket.publisher_payload:
        basket_tags = {
            o.get("tag") for o in (latest_basket.publisher_payload.get("basket") or [])
            if o.get("tag")
        }

    orders = []
    filled = 0
    rejected = 0
    pending = 0
    cancelled = 0

    # Buy orders
    buy_rows = db.query(LiveBuyStock).filter(
        LiveBuyStock.automate_equity_ra_id == live_id,
    ).all()
    for r in buy_rows:
        if basket_tags and r.publisher_tag not in basket_tags:
            continue  # Skip old basket rows
        status = (r.broker_status or "").upper() if r.broker_status else None
        if status == "COMPLETE":
            filled += 1
        elif status == "REJECTED":
            rejected += 1
        elif status in ("CANCELLED", "CANCEL"):
            cancelled += 1
        else:
            pending += 1
        orders.append(OrderDetail(
            tradingsymbol=r.tradingsymbol,
            isin=r.isin,
            side="BUY",
            qty=r.qty,
            price=float(r.price or 0),
            broker_status=status,
            reason=r.broker_status_message,
            filled_qty=int(r.actual_qty or 0),
            filled_price=float(r.actual_price or 0),
        ))

    # Sell orders
    sell_rows = db.query(LiveSellStock).filter(
        LiveSellStock.automate_equity_ra_id == live_id,
    ).all()
    for r in sell_rows:
        if basket_tags and r.publisher_tag not in basket_tags:
            continue
        status = (r.broker_status or "").upper() if r.broker_status else None
        if status == "COMPLETE":
            filled += 1
        elif status == "REJECTED":
            rejected += 1
        elif status in ("CANCELLED", "CANCEL"):
            cancelled += 1
        else:
            pending += 1
        orders.append(OrderDetail(
            tradingsymbol=r.tradingsymbol,
            isin=r.isin,
            side="SELL",
            qty=r.qty,
            price=float(r.price or 0),
            broker_status=status,
            reason=r.broker_status_message,
            filled_qty=int(r.actual_qty or 0),
            filled_price=float(r.actual_price or 0),
        ))

    return OrderStatusResponse(
        live_id=live_id,
        strategy_name=strategy.strategy_name,
        strategy_status=strategy.status.value if hasattr(strategy.status, "value") else str(strategy.status),
        total_orders=len(orders),
        filled=filled,
        rejected=rejected,
        pending=pending,
        cancelled=cancelled,
        orders=orders,
    )


@router.get("/{live_id}/dashboard", response_model=LiveDashboardResponse)
def get_strategy_dashboard(live_id: UUID, db: Session = Depends(get_db)):
    """Strategy overview: strategy info, latest equity curve snapshot, pending basket."""
    strategy = db.query(LiveStrategy).filter(
        LiveStrategy.id == live_id,
    ).first()
    if not strategy:
        raise HTTPException(status_code=404, detail="Live strategy not found")

    # Latest equity curve point
    latest_eq = db.query(LiveEquityCurve).filter(
        LiveEquityCurve.automate_equity_ra_id == live_id,
    ).order_by(LiveEquityCurve.date.desc(), LiveEquityCurve.total_days.desc()).first()
    latest_equity_curve = None
    if latest_eq:
        latest_equity_curve = EquityCurvePointResponse(
            date=latest_eq.date,
            cash=latest_eq.cash,
            stocks_value=latest_eq.stocks_value,
            aum=latest_eq.aum,
            strategy_roc=latest_eq.strategy_roc,
            strategy_daily_return=latest_eq.strategy_daily_return,
            equitycurve_percent=latest_eq.equitycurve_percent,
            max_dd_percent=latest_eq.max_dd_percent,
            total_pnl=latest_eq.total_pnl,
            sharpe=latest_eq.sharpe,
            cagr_percent=latest_eq.cagr_percent,
        )

    # Latest pending/partial basket
    pending_basket_row = db.query(LivePublisherBasket).filter(
        LivePublisherBasket.automate_equity_ra_id == live_id,
        LivePublisherBasket.status.in_(["CREATED", "PENDING_USER_APPROVAL", "PARTIAL"]),
    ).order_by(LivePublisherBasket.created_at.desc()).first()
    pending_basket = None
    if pending_basket_row:
        pending_basket = PendingBasketResponse(
            basket_key=pending_basket_row.basket_key,
            basket_type=pending_basket_row.basket_type,
            status=pending_basket_row.status,
            broker=pending_basket_row.broker,
            publisher_payload=pending_basket_row.publisher_payload,
        )

    return LiveDashboardResponse(
        strategy=serialize_strategy(strategy),
        latest_equity_curve=latest_equity_curve,
        pending_basket=pending_basket,
    )


@router.get("/{live_id}/holdings", response_model=List[TradelogHoldingResponse])
def get_strategy_holdings(live_id: UUID, db: Session = Depends(get_db)):
    """Active holdings for a strategy (from tradelog)."""
    strategy = db.query(LiveStrategy).filter(LiveStrategy.id == live_id).first()
    if not strategy:
        raise HTTPException(status_code=404, detail="Live strategy not found")

    rows = db.query(LiveTradelog).filter(
        LiveTradelog.automate_equity_ra_id == live_id,
        LiveTradelog.active == True,
    ).all()
    return [
        TradelogHoldingResponse(
            tradingsymbol=h.tradingsymbol,
            isin=h.isin,
            buy_date=h.buy_date,
            buy_qty=h.buy_qty,
            buy_price=float(h.buy_price or 0),
            buy_amount=float(h.buy_amount or 0),
            sell_qty=h.sell_qty or 0,
            sell_price=float(h.sell_price or 0),
            ltp=float(h.ltp) if h.ltp else None,
            current_value=float(h.current_value) if h.current_value else None,
            unrealised_pnl=float(h.unrealised_pnl) if h.unrealised_pnl is not None else None,
            realised_pnl=float(h.realised_pnl) if h.realised_pnl is not None else None,
            profit_percent=float(h.profit_percent) if h.profit_percent is not None else None,
            weightage=float(h.weightage) if h.weightage is not None else None,
            active=h.active,
        )
        for h in rows
    ]


@router.get("/{live_id}/equity-curve-graph", response_model=List[EquityCurveGraphPoint])
def get_equity_curve_graph(live_id: UUID, db: Session = Depends(get_db)):
    """Full equity curve data for chart (date, strategy_roc, index_roc)."""
    strategy = db.query(LiveStrategy).filter(LiveStrategy.id == live_id).first()
    if not strategy:
        raise HTTPException(status_code=404, detail="Live strategy not found")

    rows = db.query(LiveEquityCurve).filter(
        LiveEquityCurve.automate_equity_ra_id == live_id,
    ).order_by(LiveEquityCurve.date.asc()).all()
    return [
        EquityCurveGraphPoint(
            date=row.date,
            strategy_roc=row.strategy_roc,
            index_roc=row.index_roc,
        )
        for row in rows
    ]
