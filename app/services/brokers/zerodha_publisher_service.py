from uuid import uuid4
from sqlalchemy.orm import Session

from app.models.broker_order import BrokerOrder
from app.schemas.zerodha_publisher import PublisherOrderCreate


class ZerodhaPublisherService:
    def __init__(self, db: Session):
        self.db = db

    def create_pending_order(self, payload: PublisherOrderCreate):
        internal_order_id = f"ZPUB-{uuid4().hex[:16]}"

        order = BrokerOrder(
            internal_order_id=internal_order_id,
            broker="ZERODHA",
            status="INITIATED",
            exchange=payload.exchange,
            tradingsymbol=payload.tradingsymbol,
            transaction_type=payload.transaction_type,
            quantity=payload.quantity,
            product=payload.product,
            order_type=payload.order_type,
            price=payload.price,
        )

        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)

        publisher_params = {
            "exchange": payload.exchange,
            "tradingsymbol": payload.tradingsymbol,
            "transaction_type": payload.transaction_type,
            "quantity": payload.quantity,
            "product": payload.product,
            "order_type": payload.order_type,
        }

        if payload.price is not None:
            publisher_params["price"] = payload.price

        return {
            "internal_order_id": internal_order_id,
            "status": order.status,
            "publisher_params": publisher_params,
        }

    def update_from_postback(self, data: dict):
        zerodha_order_id = data.get("order_id")

        order = None
        if zerodha_order_id:
            order = (
                self.db.query(BrokerOrder)
                .filter(BrokerOrder.zerodha_order_id == zerodha_order_id)
                .first()
            )

        if not order:
            order = BrokerOrder(
                internal_order_id=f"ZPB-{uuid4().hex[:16]}",
                broker="ZERODHA",
                status=data.get("status", "UNKNOWN"),
                exchange=data.get("exchange") or "UNKNOWN",
                tradingsymbol=data.get("tradingsymbol") or "UNKNOWN",
                transaction_type=data.get("transaction_type") or "UNKNOWN",
                quantity=data.get("quantity") or 0,
                product=data.get("product") or "UNKNOWN",
                order_type=data.get("order_type") or "UNKNOWN",
            )
            self.db.add(order)

        order.user_id = data.get("user_id")
        order.zerodha_order_id = zerodha_order_id
        order.status = data.get("status", order.status)
        order.filled_quantity = data.get("filled_quantity") or order.filled_quantity
        order.average_price = data.get("average_price") or order.average_price
        order.raw_postback = data

        self.db.commit()
        self.db.refresh(order)

        return order
