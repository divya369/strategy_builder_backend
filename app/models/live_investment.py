"""
Strategy Builder live investment tables.

Purpose:
- Keep Strategy Builder live data separate from equitycase_logic DB tables.
- Keep the schema/column style very close to equitycase_logic automate equity tables
  so that future merge/migration is simple.
- Only execution/order-book handling is different: Publisher basket + postback.
"""
import enum
import uuid
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Date, DateTime, ForeignKey,
    Enum, Text, text, func, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.models.base import Base


class LiveStatus(enum.Enum):
    DRAFT = "DRAFT"
    PREVIEW_READY = "PREVIEW_READY"
    PENDING_USER_APPROVAL = "PENDING_USER_APPROVAL"
    ACTIVE = "ACTIVE"
    REBALANCE_READY = "REBALANCE_READY"
    REBALANCE_PENDING_USER_APPROVAL = "REBALANCE_PENDING_USER_APPROVAL"
    REBALANCE_SELL_COMPLETE = "REBALANCE_SELL_COMPLETE"
    EXIT_PENDING_USER_APPROVAL = "EXIT_PENDING_USER_APPROVAL"
    EXITED = "EXITED"
    ACCOUNT_MISMATCH = "ACCOUNT_MISMATCH"
    ALL_REJECTED = "ALL_REJECTED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class LiveBrokerAccount(Base):
    """
    User-selected broker account label table for Strategy Builder live trading.

    This is not demat credential storage. It is only the user's label/selection layer:
        Zerodha - Main Account
        Zerodha - Family Account
        Angel One - Father Account

    A Live Strategy locks one row from this table at Go Live. Rebalance/Exit cannot
    change it. If broker postback gives client_id, that client_id is locked on the
    strategy after first successful postback.
    """
    __tablename__ = "broker_account"
    __table_args__ = (
        UniqueConstraint("user_id", "broker", "broker_account_label", name="uq_broker_account_user_broker_label"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    broker = Column(String(50), nullable=False, index=True)
    broker_account_label = Column(String(255), nullable=False)
    broker_user_id = Column(String(100), nullable=True, index=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class LiveStrategy(Base):
    """
    Strategy Builder equivalent of equity.automate_equity_ra.

    Different table name, but same accounting fields:
    initial_aum, cash, stock_value, final_aum, pnl, todays_pnl,
    subscription_active, status, start_date, end_date.

    Extra Strategy Builder linkage fields are kept here rather than changing
    buy/sell/tradelog/equitycurve structure.
    """
    __tablename__ = "automate_equity"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)

    # User + Strategy Builder source linkage
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    screener_id = Column(UUID(as_uuid=True), ForeignKey("screeners.id", ondelete="SET NULL"), nullable=True, index=True)
    screener_version_id = Column(UUID(as_uuid=True), ForeignKey("screener_versions.id", ondelete="SET NULL"), nullable=False, index=True)
    screener_version = relationship("ScreenerVersion", primaryjoin="LiveStrategy.screener_version_id == ScreenerVersion.id")

    # User-provided live params at Go Live time. Do NOT copy these from backtest.
    strategy_name = Column(String(255), nullable=True)
    portfolio_size = Column(Integer, nullable=False)
    worst_hold_rank = Column(Integer, nullable=False)  # same meaning as WRH in equitycase_logic
    rebalance_frequency = Column(String(20), nullable=False)  # WEEKLY / MONTHLY

    # Broker account lock for Publisher/offsite flow.
    # Rebalance/Exit must use this same broker_account_id only.
    broker_account_id = Column(UUID(as_uuid=True), ForeignKey("broker_account.id", ondelete="RESTRICT"), nullable=False, index=True)
    broker = Column(String(50), nullable=False, default="ZERODHA")
    broker_account_label = Column(String(255), nullable=False)
    locked_client_id = Column(String(100), nullable=True, index=True)

    # Equitycase-style AUM fields.
    initial_aum = Column(Float, nullable=False, default=0.0)
    cash = Column(Float, nullable=False, default=0.0)
    stock_value = Column(Float, nullable=False, default=0.0)
    final_aum = Column(Float, nullable=False, default=0.0)
    pnl = Column(Float, nullable=False, default=0.0)
    todays_pnl = Column(Float, nullable=False, default=0.0)
    subscription_active = Column(Boolean, nullable=False, default=False)
    status = Column(
        Enum(LiveStatus, name="strategy_builder_live_status_enum", create_type=True),
        nullable=False,
        default=LiveStatus.DRAFT,
        server_default="DRAFT",
        index=True,
    )
    start_date = Column(Date, nullable=False)
    next_rebalance_date = Column(Date, nullable=True)

    # Copy source screener configs for audit/future replay.
    filters_json = Column(JSONB, nullable=True)
    universe_json = Column(JSONB, nullable=True)
    ranking_json = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class LiveBuyStock(Base):
    """Same column layout as equity.buy_stock, but separate table."""
    __tablename__ = "buy_stock"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("automate_equity.id", ondelete="CASCADE"), nullable=False, index=True)
    tradingsymbol = Column(String, nullable=False)
    isin = Column(String, nullable=False, default="")
    date = Column(Date, nullable=False)
    qty = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    weightage = Column(Float, nullable=True)
    actual_qty = Column(Integer, nullable=True)
    actual_price = Column(Float, nullable=True)
    actual_amount = Column(Float, nullable=True)
    stoploss = Column(Float, nullable=True)
    volatility = Column(Float, nullable=True)
    order_id = Column(String, nullable=True, index=True)
    circuit = Column(Boolean, default=False, nullable=True)
    updated_in_tradelog = Column(Boolean, default=False, nullable=True)
    # Publisher/postback fields
    publisher_tag = Column(String(8), nullable=True, unique=True, index=True)
    broker_status = Column(String(30), nullable=True)
    broker_status_message = Column(Text, nullable=True)
    broker_raw_postback = Column(JSONB, nullable=True)


class LiveSellStock(Base):
    """Same column layout as equity.sell_stock, but separate table."""
    __tablename__ = "sell_stock"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("automate_equity.id", ondelete="CASCADE"), nullable=False, index=True)
    tradingsymbol = Column(String, nullable=False)
    isin = Column(String, nullable=False, default="")
    date = Column(Date, nullable=False)
    qty = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    actual_qty = Column(Integer, nullable=True)
    actual_price = Column(Float, nullable=True)
    actual_amount = Column(Float, nullable=True)
    order_id = Column(String, nullable=True, index=True)
    method = Column(String, nullable=True)
    circuit = Column(Boolean, default=False, nullable=True)
    updated_in_tradelog = Column(Boolean, default=False, nullable=True)
    # Publisher/postback fields
    publisher_tag = Column(String(8), nullable=True, unique=True, index=True)
    broker_status = Column(String(30), nullable=True)
    broker_status_message = Column(Text, nullable=True)
    broker_raw_postback = Column(JSONB, nullable=True)


class LiveCircuitStock(Base):
    """Same column layout as equity.circuit_stock, but separate table."""
    __tablename__ = "circuit_stock"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("automate_equity.id", ondelete="CASCADE"), nullable=False, index=True)
    tradingsymbol = Column(String, nullable=False)
    isin = Column(String, nullable=False, default="")
    date = Column(Date, nullable=False)
    qty = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    weightage = Column(Float, nullable=True)
    actual_qty = Column(Integer, nullable=True)
    actual_price = Column(Float, nullable=True)
    actual_amount = Column(Float, nullable=True)
    stoploss = Column(Float, nullable=True)
    volatility = Column(Float, nullable=True)
    order_id = Column(String, nullable=True, index=True)
    updated_in_tradelog = Column(Boolean, default=False, nullable=True)
    lower_upper = Column(String, nullable=False)
    action = Column(String, nullable=False)  # buy / sell
    active = Column(Boolean, nullable=False, default=True)
    # Publisher/postback fields
    publisher_tag = Column(String(8), nullable=True, unique=True, index=True)
    broker_status = Column(String(30), nullable=True)
    broker_status_message = Column(Text, nullable=True)
    broker_raw_postback = Column(JSONB, nullable=True)


class LiveTradelog(Base):
    """Same column layout as equity.tradelog_automate_equity, but separate table."""
    __tablename__ = "tradelog_automate_equity"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("automate_equity.id", ondelete="CASCADE"), nullable=False, index=True)
    tradingsymbol = Column(String, nullable=False)
    isin = Column(String, nullable=False, default="")
    buy_date = Column(Date, nullable=False)
    sell_date = Column(Date, nullable=True)
    hold = Column(Integer, nullable=False)
    weightage = Column(Float, nullable=True)
    buy_qty = Column(Integer, nullable=False)
    buy_price = Column(Float, nullable=False)
    buy_amount = Column(Float, nullable=False)
    sell_qty = Column(Integer, nullable=True)
    sell_price = Column(Float, nullable=True)
    sell_amount = Column(Float, nullable=True)
    pyramiding = Column(Integer, nullable=True)
    volatility = Column(Float, nullable=True)
    ltp = Column(Float, nullable=True)
    stoploss = Column(Float, nullable=True)
    risk = Column(Float, nullable=True)
    risk_percent = Column(Float, nullable=True)
    current_value = Column(Float, nullable=True)
    unrealised_pnl = Column(Float, nullable=True)
    realised_pnl = Column(Float, nullable=True)
    profit_percent = Column(Float, nullable=True)
    buy_charges = Column(Float, nullable=True)
    sell_charges = Column(Float, nullable=True)
    active = Column(Boolean, nullable=False, default=False)
    buy_order_id = Column(String, nullable=True, index=True)
    sell_order_id = Column(String, nullable=True, index=True)
    pyramiding_data = Column(String, nullable=True)
    profit_booking_data = Column(String, nullable=True)


class LiveEquityCurve(Base):
    """Same column layout as equity.equitycurve_automate_equity, but separate table."""
    __tablename__ = "equitycurve_automate_equity"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("automate_equity.id", ondelete="CASCADE"), nullable=False, index=True)
    date = Column(Date, nullable=False)
    total_days = Column(Integer, nullable=False)
    portfolio_size = Column(Integer, nullable=True)
    stocks_value = Column(Float, nullable=True)
    cash = Column(Float, nullable=True)
    aum = Column(Float, nullable=True)
    index_price = Column(Float, nullable=True)
    strategy_roc = Column(Float, nullable=True)
    index_roc = Column(Float, nullable=True)
    strategy_daily_return = Column(Float, nullable=True)
    index_daily_return = Column(Float, nullable=True)
    strategy_daily_performance = Column(Float, nullable=True)
    index_daily_performance = Column(Float, nullable=True)
    unrealised_pnl = Column(Float, nullable=True)
    realised_pnl = Column(Float, nullable=True)
    total_pnl = Column(Float, nullable=True)
    winning_trades = Column(Integer, nullable=True)
    losing_trades = Column(Integer, nullable=True)
    total_trades = Column(Integer, nullable=True)
    winning_percent = Column(Float, nullable=True)
    losing_percent = Column(Float, nullable=True)
    avg_win = Column(Float, nullable=True)
    avg_loss = Column(Float, nullable=True)
    rr = Column(Float, nullable=True)
    profit_factor = Column(Float, nullable=True)
    biggest_winning_trade = Column(Float, nullable=True)
    biggest_losing_trade = Column(Float, nullable=True)
    expectancy = Column(Float, nullable=True)
    avg_profit_per_day = Column(Float, nullable=True)
    max_dd_percent = Column(Float, nullable=True)
    max_dd_absolute = Column(Float, nullable=True)
    current_dd_percent = Column(Float, nullable=True)
    sqn = Column(Float, nullable=True)
    k_multiple = Column(Float, nullable=True)
    sharpe = Column(Float, nullable=True)
    calmar = Column(Float, nullable=True)
    sortino_ratio = Column(Float, nullable=True)
    equitycurve_percent = Column(Float, nullable=True)
    cagr_percent = Column(Float, nullable=True)
    neg_2sd = Column(Float, nullable=True)
    equitycurve_avg = Column(Float, nullable=True)
    pos_2sd = Column(Float, nullable=True)
    total_charges = Column(Float, nullable=True)
    rebalance = Column(Boolean, default=False, nullable=True)
    weekly_return = Column(Float, nullable=True)
    monthly_return = Column(Float, nullable=True)
    quarterly_return = Column(Float, nullable=True)
    yearly_return = Column(Float, nullable=True)


class LivePublisherBasket(Base):
    """
    Extra table only for Publisher/offsite execution metadata.
    This does not replace buy/sell/circuit/tradelog/equitycurve.
    It only groups the Publisher basket and stores raw postback data.
    """
    __tablename__ = "publisher_basket"
    __table_args__ = (
        UniqueConstraint("basket_key", name="uq_publisher_basket_key"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    automate_equity_ra_id = Column(UUID(as_uuid=True), ForeignKey("automate_equity.id", ondelete="CASCADE"), nullable=False, index=True)
    broker_account_id = Column(UUID(as_uuid=True), ForeignKey("broker_account.id", ondelete="RESTRICT"), nullable=True, index=True)
    broker = Column(String(50), nullable=True, index=True)
    basket_key = Column(String(100), nullable=False, index=True)
    basket_type = Column(String(30), nullable=False)  # INITIAL / REBALANCE / EXIT / CIRCUIT
    status = Column(String(50), nullable=False, default="CREATED")
    publisher_payload = Column(JSONB, nullable=True)
    raw_postback = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
