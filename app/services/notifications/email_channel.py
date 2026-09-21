"""
Resend-based email notification channel.

Uses Jinja2 for HTML template rendering and Resend SDK for delivery.
"""
from __future__ import annotations
import logging
from pathlib import Path
from typing import Any, Dict, List
import resend
from jinja2 import Environment, FileSystemLoader
from app.core.config import settings
from .base import BaseNotificationChannel

logger = logging.getLogger("notifications")

# Template directory: app/services/notifications/templates/
_TEMPLATE_DIR = Path(__file__).parent / "templates"
_jinja_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATE_DIR)),
    autoescape=True,
)


class EmailNotificationChannel(BaseNotificationChannel):
    """Concrete email channel using Resend API."""

    channel = "EMAIL"

    def send_rebalance_ready(
        self,
        *,
        user_email: str | None,
        user_name: str | None,
        strategy_name: str,
        strategy_id: str,
        changes: List[Dict[str, Any]],
        dashboard_url: str,
        timestamp: str,
        rebalance_date: str = "",
        user_phone: str | None = None,
    ) -> bool:
        if not settings.RESEND_API_KEY:
            logger.warning("[Email] RESEND_API_KEY not configured — skipping rebalance email")
            return False
        if not user_email:
            logger.info("[Email] No email for strategy=%s — skipping rebalance email", strategy_id)
            return False

        resend.api_key = settings.RESEND_API_KEY

        # Separate buys and sells for template ordering
        sells = [c for c in changes if c.get("action", "").upper() == "SELL"]
        buys = [c for c in changes if c.get("action", "").upper() == "BUY"]
        ordered_changes = sells + buys  # REMOVED first, then ADDED

        template = _jinja_env.get_template("rebalance_ready.html")
        html_body = template.render(
            strategy_name=strategy_name,
            changes=ordered_changes,
            dashboard_url=dashboard_url,
            timestamp=timestamp,
            user_name=user_name or "Investor",
            rebalance_date=rebalance_date,
        )

        to_addr = f"{user_name} <{user_email}>" if user_name else user_email

        # An empty rebalance is still worth telling the user about, but it must not
        # arrive titled "Rebalance Ready" — the subject is all most people read, and
        # promising an execution they don't have to make is the confusing part.
        subject = (
            f"Rebalance Ready — {strategy_name}" if ordered_changes
            else f"No Action Needed — {strategy_name}"
        )

        try:
            params: resend.Emails.SendParams = {
                "from": settings.EMAIL_FROM,
                "to": [to_addr],
                "subject": subject,
                "html": html_body,
            }
            result = resend.Emails.send(params)
            logger.info(
                "[Email] Rebalance email sent | strategy=%s to=%s resend_id=%s",
                strategy_id, user_email, result.get("id", "?"),
            )
            return True
        except Exception:
            logger.exception("[Email] Failed to send rebalance email | strategy=%s to=%s", strategy_id, user_email)
            return False
