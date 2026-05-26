from sqlalchemy import Column, String, Integer, Float, DateTime, JSON, func
from app.models.base import Base


class BrokerOrder(Base):
    __tablename__ = "broker_orders"

    id = Column(Integer, primary_key=True, index=True)

    internal_order_id = Column(String, unique=True, index=True, nullable=False)
    basket_id = Column(String, index=True, nullable=True)

    broker = Column(String, default="ZERODHA")
    user_id = Column(String, index=True, nullable=True)

    zerodha_order_id = Column(String, index=True, nullable=True)
    status = Column(String, default="INITIATED")

    exchange = Column(String, nullable=False)
    tradingsymbol = Column(String, nullable=False)
    transaction_type = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    product = Column(String, nullable=False)
    order_type = Column(String, nullable=False)
    price = Column(Float, nullable=True)

    filled_quantity = Column(Integer, default=0)
    average_price = Column(Float, nullable=True)

    raw_postback = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
