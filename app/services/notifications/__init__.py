"""
Plug-and-play notification system.

Usage:
    from app.services.notifications import notify_all
    notify_all("send_rebalance_ready", **details)
"""
from .registry import get_notifier, notify_all  # noqa: F401
