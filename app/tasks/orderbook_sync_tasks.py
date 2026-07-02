"""
Orderbook sync tasks — debounced verification of order fills via kite.orders().

The verify_basket_from_orderbook_task is triggered by the postback handler
(via Redis SETNX debounce) when a terminal postback arrives. It calls
kite.orders() to verify all basket orders, and bulk-updates fills + transitions
strategy status when all orders are terminal.

This file does NOT add any celery beat schedules — the task is purely
event-driven (triggered by postback → Redis SETNX → apply_async with countdown).
"""
import logging
from celery import shared_task
from app.core.database import SessionLocal
from app.core.logging_config import setup_logging

logger = logging.getLogger("orderbook_sync")


@shared_task(
    name="strategy_builder.verify_basket_from_orderbook",
    bind=True,
    max_retries=0,  # We handle retries ourselves via retry_count parameter
    acks_late=True,
)
def verify_basket_from_orderbook_task(self, basket_id: str, retry_count: int = 0) -> dict:
    """Debounced orderbook verification task.

    Called ~5 seconds after the first terminal postback for a basket.
    Calls kite.orders() to verify all orders, processes fills if all terminal.

    Args:
        basket_id: UUID of the LivePublisherBasket to verify.
        retry_count: Current retry number (0-based). Max 20 retries × 5s = ~100s.
    """
    setup_logging()
    logger.info("[OrderbookTask] Starting verification | basket=%s retry=%d", basket_id, retry_count)

    db = SessionLocal()
    try:
        from app.services.live_investment_service import LiveInvestmentService
        result = LiveInvestmentService.verify_and_process_from_orderbook(
            db, basket_id=basket_id, retry_count=retry_count,
        )
        logger.info("[OrderbookTask] Result | basket=%s detail=%s", basket_id, result.get("detail"))
        return result
    except Exception:
        logger.exception("[OrderbookTask] Fatal error verifying basket %s", basket_id)
        # Clear Redis key on fatal error so postbacks can re-trigger
        try:
            from app.services.live_investment_service import LiveInvestmentService
            LiveInvestmentService._redis_client.delete(f"orderbook_verify:{basket_id}")
        except Exception:
            pass
        raise
    finally:
        db.close()
