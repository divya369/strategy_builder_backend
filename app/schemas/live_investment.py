from datetime import date
from typing import Optional, List, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field, field_validator



class BrokerLinkedStrategy(BaseModel):
    id: UUID
    strategy_name: Optional[str] = None
    version_number: int

class BrokerAccountResponse(BaseModel):
    id: UUID
    broker: str
    label: str
    expected_client_id: Optional[str] = None
    is_active: bool
    strategies: List[BrokerLinkedStrategy] = []


class BrokerSelectionRequest(BaseModel):
    """Inline broker selection shared by Go Live and platform 'Invest Now'.

    These three fields are typed by hand into the Go Live popup, so the same
    Zerodha account arrives as 'xyz1234', 'XYZ1234 ' and 'XYZ1234' on different
    attempts. The broker-account upsert looks accounts up by broker_user_id, so
    unnormalized input misses the user's own account and falls through to an
    INSERT that collides on the (user_id, broker, label) unique index.
    """
    broker: str = Field(default="zerodha", description="Broker selection dropdown")
    broker_user_id: str = Field(description="Broker User ID / expected client ID")
    broker_account_label: Optional[str] = Field(default=None, description="Account Nickname / Label")

    @field_validator("broker")
    @classmethod
    def _normalize_broker(cls, v: str) -> str:
        return v.strip().lower()

    @field_validator("broker_user_id")
    @classmethod
    def _normalize_broker_user_id(cls, v: str) -> str:
        # Zerodha client IDs are uppercase alphanumeric.
        v = v.strip().upper()
        if not v:
            raise ValueError("broker_user_id must not be blank")
        return v

    @field_validator("broker_account_label")
    @classmethod
    def _normalize_label(cls, v: Optional[str]) -> Optional[str]:
        # A whitespace-only nickname is the same as not supplying one, which
        # lets it fall back to broker_user_id.
        v = (v or "").strip()
        return v or None


class GoLiveRequest(BrokerSelectionRequest):
    user_id: str
    screener_version_id: UUID
    strategy_name: str

    # Fresh user input from Go Live popup. Do not copy these from backtest.
    portfolio_size: int = Field(gt=0)
    wrh: int = Field(gt=0, description="Worst Hold Rank")
    rebalance_frequency: str = Field(pattern="^(weekly|monthly)$")
    aum: float = Field(gt=0)


class PlatformInvestRequest(BrokerSelectionRequest):
    """User clicks 'Invest Now' on a platform (ready-to-use) strategy.

    Same fields as GoLiveRequest, except the source is a platform SCREENER
    (one version per platform screener, so no version id is needed) and the
    user supplies a name + description for their own cloned copy.
    """
    user_id: str
    platform_screener_id: UUID
    strategy_name: str
    description: Optional[str] = None

    # Fresh user input from Go Live popup. Do not copy these from backtest.
    portfolio_size: int = Field(gt=0)
    wrh: int = Field(gt=0, description="Worst Hold Rank")
    rebalance_frequency: str = Field(pattern="^(weekly|monthly)$")
    aum: float = Field(gt=0)


class LiveStrategyResponse(BaseModel):
    id: UUID
    screener_id: Optional[UUID] = None
    version_number: int
    status: str
    strategy_name: str
    portfolio_size: int
    wrh: int
    rebalance_frequency: str
    initial_aum: float
    final_aum: float
    broker: Optional[str] = None
    broker_user_id: Optional[str] = None
    broker_account_label: Optional[str] = None
    next_rebalance_date: Optional[date] = None


class PreviewStockItem(BaseModel):
    tradingsymbol: str
    isin: Optional[str] = None
    qty: int
    price: float
    amount: float


class PreviewResponse(BaseModel):
    strategy: LiveStrategyResponse
    buy_stocks: List[PreviewStockItem]
    sell_stocks: List[PreviewStockItem]
    total_buy_amount: float
    total_sell_amount: float
    remaining_cash: float
    broker: Optional[str] = None
    broker_account_label: Optional[str] = None
    broker_user_id: Optional[str] = None

class TradeNowResponse(BaseModel):
    live_id: UUID
    basket_key: str
    basket_type: str
    status: str
    broker: str
    broker_account_label: str
    publisher_payload: Dict[str, Any]



class DuplicateRequest(BaseModel):
    broker_account_id: UUID
    strategy_name: Optional[str] = None
    aum: Optional[float] = None


# ── GET endpoint response schemas ─────────────────────────────────────────────

class TradelogHoldingResponse(BaseModel):
    tradingsymbol: str
    isin: Optional[str] = None
    buy_date: Optional[date] = None
    buy_qty: int
    buy_price: float
    buy_amount: float
    sell_date: Optional[date] = None
    hold: Optional[int] = 0
    sell_qty: Optional[int] = 0
    sell_price: Optional[float] = 0.0
    sell_amount: Optional[float] = 0.0
    ltp: Optional[float] = None
    current_value: Optional[float] = None
    unrealised_pnl: Optional[float] = None
    realised_pnl: Optional[float] = None
    profit_percent: Optional[float] = None
    weightage: Optional[float] = None
    active: bool


class EquityCurvePointResponse(BaseModel):
    date: date
    cash: Optional[float] = None
    stocks_value: Optional[float] = None
    aum: Optional[float] = None
    strategy_roc: Optional[float] = None
    strategy_daily_return: Optional[float] = None
    equitycurve_percent: Optional[float] = None
    max_dd_percent: Optional[float] = None
    total_pnl: Optional[float] = None
    sharpe: Optional[float] = None
    cagr_percent: Optional[float] = None
    total_trades: Optional[int] = None
    winning_trades: Optional[int] = None
    losing_trades: Optional[int] = None
    winning_percent: Optional[float] = None
    losing_percent: Optional[float] = None
    avg_win: Optional[float] = None
    avg_loss: Optional[float] = None
    total_charges: Optional[float] = None
    monthly_return: Optional[float] = None


class PendingBasketResponse(BaseModel):
    basket_key: str
    basket_type: str
    status: str
    broker: Optional[str] = None
    publisher_payload: Optional[Dict[str, Any]] = None


class EquityCurveGraphPoint(BaseModel):
    """Lightweight point for equity curve chart (date vs strategy_roc vs index_roc vs benchmark_roc)."""
    date: date
    strategy_roc: Optional[float] = None
    index_roc: Optional[float] = None
    benchmark_roc: Optional[float] = None
    current_dd_percent: Optional[float] = None


class LiveMonthlyReturn(BaseModel):
    year: int
    month: int
    monthly_return: float


class EquityCurveGraphResponse(BaseModel):
    """Equity curve graph data with index/benchmark labels for frontend legend."""
    index_label: str = "NIFTY 50"
    benchmark_label: str = "NIFTY 50"
    data: List[EquityCurveGraphPoint] = []
    monthly_returns: List[LiveMonthlyReturn] = []


class LiveDashboardResponse(BaseModel):
    strategy: LiveStrategyResponse
    latest_equity_curve: Optional[EquityCurvePointResponse] = None
    pending_basket: Optional[PendingBasketResponse] = None
    exit_orders_sent: bool = False


class LiveStrategyListItem(BaseModel):
    id: UUID
    version_number: int
    status: str
    strategy_name: Optional[str] = None
    broker_user_id: Optional[str] = None
    broker_account_label: Optional[str] = None
    initial_aum: float
    final_aum: float
    pnl: float
    subscription_active: bool


class PortfolioSummaryResponse(BaseModel):
    total_investment: float
    current_investment: float
    total_pnl: float
    todays_pnl: float
    strategy_count: int


# ── Order status endpoint schemas ─────────────────────────────────────────────

class OrderDetail(BaseModel):
    tradingsymbol: str
    isin: Optional[str] = None
    side: str  # BUY / SELL
    qty: int
    price: float
    broker_status: Optional[str] = None  # COMPLETE / REJECTED / CANCELLED / None (pending)
    reason: Optional[str] = None
    filled_qty: Optional[int] = 0
    filled_price: Optional[float] = 0.0


class OrderStatusResponse(BaseModel):
    live_id: UUID
    strategy_name: Optional[str] = None
    strategy_status: str
    total_orders: int
    filled: int
    rejected: int
    pending: int
    cancelled: int
    orders: List[OrderDetail]


# ── Publisher redirect-callback schema ────────────────────────────────────

class ZerodhaPublisherCallbackRequest(BaseModel):
    """Schema for frontend callback after Kite Publisher redirect.

    Frontend calls this after Kite redirects back with status=success.
    Backend exchanges request_token for access_token (read-only).
    """
    user_id: str
    live_id: UUID
    request_token: str

