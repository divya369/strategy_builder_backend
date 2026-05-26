from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.zerodha_publisher import PublisherOrderCreate
from app.services.brokers.zerodha_publisher_service import ZerodhaPublisherService
from app.models.broker_order import BrokerOrder

router = APIRouter()


@router.post("/orders/initiate")
def initiate_publisher_order(
    payload: PublisherOrderCreate,
    db: Session = Depends(get_db),
):
    service = ZerodhaPublisherService(db)
    return service.create_pending_order(payload)


@router.post("/postback")
async def zerodha_postback(
    request: Request,
    db: Session = Depends(get_db),
):
    data = await request.json()
    service = ZerodhaPublisherService(db)
    order = service.update_from_postback(data)

    print("========== POSTBACK RECEIVED ==========")
    print(data)
    print("======================================")

    return {
        "success": True,
        "internal_order_id": order.internal_order_id,
        "zerodha_order_id": order.zerodha_order_id,
        "status": order.status,
    }


@router.get("/orders/{internal_order_id}")
def get_order_status(
    internal_order_id: str,
    db: Session = Depends(get_db),
):
    order = (
        db.query(BrokerOrder)
        .filter(BrokerOrder.internal_order_id == internal_order_id)
        .first()
    )

    if not order:
        return {"found": False}

    return {
        "found": True,
        "internal_order_id": order.internal_order_id,
        "zerodha_order_id": order.zerodha_order_id,
        "status": order.status,
        "filled_quantity": order.filled_quantity,
        "average_price": order.average_price,
        "raw_postback": order.raw_postback,
    }
