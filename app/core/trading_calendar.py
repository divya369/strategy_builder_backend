"""Equitycase-style trading calendar helpers for live rebalance preparation.

Replace the simple Mon-Fri fallback with your existing holiday/special-trading-day
files if available.
"""
from __future__ import annotations
from datetime import date, time, timedelta, datetime
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo
from fastapi import HTTPException

CORE_DIR = Path(__file__).resolve().parent
HOLIDAYS_FILE = CORE_DIR / "holidays.json"
SPECIAL_TRADING_DAYS_FILE = CORE_DIR / "special_trading_days.json"

IST = ZoneInfo("Asia/Kolkata")

# NSE market hours: 09:15:00 to 15:29:59
MARKET_OPEN = time(9, 15, 0)
MARKET_CLOSE = time(18, 29, 59)


# ── Mtime-based JSON caching (auto-reloads when file is edited) ────────────
_holidays_mtime: float = 0.0
_holidays_cache: set[date] = set()
_special_mtime: float = 0.0
_special_cache: set[date] = set()


def _load_dates(path: Path) -> set[date]:
    """Parse holiday/special-trading-day JSON files.

    Format: {"Holiday Name": {"Date": "DD-MM-YYYY", "Day": "..."}, ...}
    """
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        dates = set()
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict) and "Date" in value:
                    # Format: "DD-MM-YYYY"
                    parts = value["Date"].split("-")
                    if len(parts) == 3:
                        d = date(int(parts[2]), int(parts[1]), int(parts[0]))
                        dates.add(d)
                elif isinstance(value, str):
                    # Fallback: try ISO format
                    dates.add(date.fromisoformat(value[:10]))
        return dates
    except Exception:
        return set()


def _get_holidays() -> set[date]:
    """Return holidays set, reloading from disk only if file was modified."""
    global _holidays_mtime, _holidays_cache
    try:
        mtime = os.path.getmtime(HOLIDAYS_FILE)
    except OSError:
        return _holidays_cache
    if mtime != _holidays_mtime:
        _holidays_cache = _load_dates(HOLIDAYS_FILE)
        _holidays_mtime = mtime
    return _holidays_cache


def _get_special_days() -> set[date]:
    """Return special trading days set, reloading from disk only if file was modified."""
    global _special_mtime, _special_cache
    try:
        mtime = os.path.getmtime(SPECIAL_TRADING_DAYS_FILE)
    except OSError:
        return _special_cache
    if mtime != _special_mtime:
        _special_cache = _load_dates(SPECIAL_TRADING_DAYS_FILE)
        _special_mtime = mtime
    return _special_cache


def is_trading_day(d: date) -> bool:
    if d in _get_special_days():
        return True
    if d in _get_holidays():
        return False
    return d.weekday() < 5


def is_within_trading_hours() -> bool:
    """Check if the current IST time is within NSE trading hours (09:15 - 15:29:59)."""
    now_ist = datetime.now(IST)
    return MARKET_OPEN <= now_ist.time() <= MARKET_CLOSE


def require_market_open() -> None:
    """FastAPI dependency: raises HTTP 403 if market is closed.

    Usage: Depends(require_market_open)
    Checks both trading day AND trading hours.
    """
    now_ist = datetime.now(IST)
    today = now_ist.date()

    if not is_trading_day(today):
        message = (f"Market is closed today ({today.strftime('%A, %d %b %Y')}). "
                   "This action is only available on NSE trading days.")
        raise HTTPException(
            status_code=403,
            detail={
                "message": message,
            }
        )

    if not (MARKET_OPEN <= now_ist.time() <= MARKET_CLOSE):
        message = (
        f"Market is closed. "
        f"This action is only available between {MARKET_OPEN.strftime('%I:%M %p')} "
        f"and {MARKET_CLOSE.strftime('%I:%M %p')} IST."
        )
        raise HTTPException(
            status_code=403,
            detail={
            "message": message,
        },
        )


def next_trading_day(d: date) -> date:
    x = d + timedelta(days=1)
    while not is_trading_day(x):
        x += timedelta(days=1)
    return x


def is_week_last_trading_day(d: date) -> bool:
    if not is_trading_day(d):
        return False
    nd = next_trading_day(d)
    return nd.isocalendar().week != d.isocalendar().week or nd.year != d.year


def is_month_last_trading_day(d: date) -> bool:
    if not is_trading_day(d):
        return False
    nd = next_trading_day(d)
    return nd.month != d.month or nd.year != d.year


def should_prepare_rebalance(d: date, frequency: str) -> bool:
    frequency = (frequency or "WEEKLY").upper()
    if frequency == "WEEKLY":
        return is_week_last_trading_day(d)
    if frequency == "MONTHLY":
        return is_month_last_trading_day(d)
    return False


def _last_trading_day_between(start: date, end: date) -> date:
    """Inclusive last trading day in [start, end]."""
    x = end
    while x >= start:
        if is_trading_day(x):
            return x
        x -= timedelta(days=1)
    return start


def next_week_last_trading_day(d: date) -> date:
    """Next weekly rebalance preparation date on or after d.

    Includes d itself: if d is already the last trading day of the week,
    it is returned immediately. This ensures that a strategy starting on
    Friday gets next_rebalance_date = next Monday (not the Monday after).
    """
    x = d
    while not is_week_last_trading_day(x):
        x += timedelta(days=1)
    return x


def next_month_last_trading_day(d: date) -> date:
    """Next monthly rebalance preparation date on or after d.

    Includes d itself: if d is already the last trading day of the month,
    it is returned immediately.
    """
    year = d.year
    month = d.month
    while True:
        next_month = month + 1
        next_year = year
        if next_month == 13:
            next_year += 1
            next_month = 1
        first_of_next_month = date(next_year, next_month, 1)
        month_end = first_of_next_month - timedelta(days=1)
        month_start = date(year, month, 1)
        candidate = _last_trading_day_between(month_start, month_end)
        if candidate >= d:
            return candidate
        month += 1
        if month == 13:
            year += 1
            month = 1


def next_rebalance_prepare_date(d: date, frequency: str) -> date | None:
    frequency = (frequency or "WEEKLY").upper()
    if frequency == "WEEKLY":
        return next_week_last_trading_day(d)
    if frequency == "MONTHLY":
        return next_month_last_trading_day(d)
    return None
