from typing import Optional
from pydantic import BaseModel


class PublisherOrderCreate(BaseModel):
    exchange: str = "NSE"
    tradingsymbol: str
    transaction_type: str
    quantity: int
    product: str = "CNC"
    order_type: str = "MARKET"
    price: Optional[float] = None


class PublisherOrderResponse(BaseModel):
    internal_order_id: str
    status: str
    publisher_params: dict


class ZerodhaPostbackPayload(BaseModel):
    user_id: Optional[str] = None
    order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None
    status: Optional[str] = None
    status_message: Optional[str] = None
    tradingsymbol: Optional[str] = None
    exchange: Optional[str] = None
    transaction_type: Optional[str] = None
    quantity: Optional[int] = None
    filled_quantity: Optional[int] = None
    average_price: Optional[float] = None
