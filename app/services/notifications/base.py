"""
Abstract base class for notification channels.

Same plug-and-play pattern as app.services.broker_publishers.base.BrokerPublisherAdapter.
To add a new channel (WhatsApp, Push, Slack), subclass this ABC, implement
the abstract methods, and register in registry.py.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseNotificationChannel(ABC):
    """
    Abstract base for notification delivery channels.
    Each concrete channel must set `channel` and implement the abstract methods.
    """
    channel: str  # "EMAIL", "WHATSAPP", "PUSH", etc.

    @abstractmethod
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
        user_phone: str | None = None,
    ) -> bool:
        """
        Send rebalance-ready notification to the user.
        Each channel skips (returns False) when its own contact detail is missing.

        Args:
            user_email: Recipient email address (None if the user has none).
            user_name: Recipient display name (optional).
            strategy_name: User-given strategy name.
            strategy_id: Live strategy UUID for dashboard link.
            changes: List of dicts with keys: tradingsymbol, action, qty.
            dashboard_url: Full URL to the strategy dashboard.
            timestamp: Human-readable timestamp string.
            user_phone: Verified recipient phone number (None if unavailable).

        Returns:
            True on success, False on failure.
        """
        raise NotImplementedError
