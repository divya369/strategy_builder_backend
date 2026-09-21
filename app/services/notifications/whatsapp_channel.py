"""
WhatsApp notification channel using Meta's WhatsApp Cloud API.

Sends pre-approved templates (see whatsapp_templates.py) with NAMED body parameters.
"""
from __future__ import annotations
import logging
import re
from typing import Any, Dict, List
import requests
from app.core.config import settings
from .base import BaseNotificationChannel
from .whatsapp_templates import TEMPLATES

logger = logging.getLogger("notifications")

_TIMEOUT_SECONDS = 15


class WhatsAppError(Exception):
    """Raised when the Graph API rejects a send request."""


def normalize_phone(value: str) -> str:
    """Strip non-digits; 10-digit numbers are treated as Indian and get 91 prefixed."""
    digits = re.sub(r"\D", "", value or "")
    return f"91{digits}" if len(digits) == 10 else digits


def send_template(to: str, template_name: str, values: Dict[str, str]) -> Dict[str, Any]:
    """
    Send an approved template to one number. Returns the Graph API response
    (message id at ["messages"][0]["id"]). Raises WhatsAppError on failure.
    """
    if not settings.WHATSAPP_TOKEN:
        raise WhatsAppError("WHATSAPP_TOKEN is not configured")

    template = TEMPLATES.get(template_name)
    if template is None:
        raise WhatsAppError(f"Unknown WhatsApp template {template_name!r}")

    phone = normalize_phone(to)
    if not re.fullmatch(r"\d{11,15}", phone):
        raise WhatsAppError(f"Invalid phone number {to!r}")

    missing = [p for p in template["params"] if not values.get(p)]
    if missing:
        raise WhatsAppError(f"Missing values for {template_name}: {', '.join(missing)}")

    components = []
    if template["params"]:
        components.append({
            "type": "body",
            "parameters": [
                {"type": "text", "parameter_name": p, "text": str(values[p])}
                for p in template["params"]
            ],
        })

    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": template["language"]},
            "components": components,
        },
    }

    url = (
        f"https://graph.facebook.com/{settings.WHATSAPP_GRAPH_API_VERSION}"
        f"/{settings.WHATSAPP_PHONE_NUMBER_ID}/messages"
    )
    res = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {settings.WHATSAPP_TOKEN}"},
        timeout=_TIMEOUT_SECONDS,
    )
    try:
        data = res.json()
    except ValueError:
        data = {"raw": res.text}
    if not res.ok:
        raise WhatsAppError(f"Graph API error {res.status_code}: {data.get('error', data)}")
    return data


class WhatsAppNotificationChannel(BaseNotificationChannel):
    """Concrete WhatsApp channel using Meta's WhatsApp Cloud API."""

    channel = "WHATSAPP"

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
        if not settings.WHATSAPP_TOKEN:
            logger.warning("[WhatsApp] WHATSAPP_TOKEN not configured — skipping rebalance message")
            return False
        if not user_phone:
            logger.warning("[WhatsApp] No phone number for strategy=%s — skipping rebalance message", strategy_id)
            return False

        values = {"user_name": user_name or "Investor", "strategy_name": strategy_name}
        if changes:
            template_name = "ec_rebalance_review_require"
        else:
            # Portfolio already matches the screener — tell the user nothing to trade
            # this cycle. The approved template takes user_name + strategy_name only;
            # rebalance_date is deliberately not sent (see whatsapp_templates.py).
            template_name = "rebalance_no_action"

        try:
            result = send_template(user_phone, template_name, values)
            logger.info(
                "[WhatsApp] Rebalance message sent | template=%s strategy=%s to=%s wamid=%s",
                template_name, strategy_id, user_phone, (result.get("messages") or [{}])[0].get("id", "?"),
            )
            return True
        except Exception:
            logger.exception("[WhatsApp] Failed to send rebalance message | strategy=%s to=%s", strategy_id, user_phone)
            return False
