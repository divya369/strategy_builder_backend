"""
Notification channel registry.

Same pattern as app.services.broker_publishers.registry:
    get_notifier("EMAIL") → EmailNotificationChannel instance
    notify_all("send_rebalance_ready", **kwargs) → fire on ALL channels
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from .base import BaseNotificationChannel
from .email_channel import EmailNotificationChannel

logger = logging.getLogger("notifications")

_CHANNELS: Dict[str, BaseNotificationChannel] = {
    EmailNotificationChannel.channel: EmailNotificationChannel(),
}


def get_notifier(channel: str = "EMAIL") -> BaseNotificationChannel:
    """Get a specific notification channel by name."""
    key = (channel or "").upper()
    if key not in _CHANNELS:
        supported = ", ".join(sorted(_CHANNELS))
        raise ValueError(f"Notification channel {channel!r} not supported. Supported: {supported}")
    return _CHANNELS[key]


def notify_all(method: str, **kwargs: Any) -> Dict[str, bool]:
    """
    Fire a notification method on ALL registered channels.

    Returns dict of {channel_name: success_bool}.
    Failures are logged but never raised — notifications must not block business logic.
    """
    results: Dict[str, bool] = {}
    for name, ch in _CHANNELS.items():
        fn = getattr(ch, method, None)
        if fn and callable(fn):
            try:
                results[name] = fn(**kwargs)
            except Exception:
                logger.exception("[Notify] Channel %s.%s() raised — skipping", name, method)
                results[name] = False
        else:
            logger.warning("[Notify] Channel %s has no method %s — skipping", name, method)
            results[name] = False
    return results
