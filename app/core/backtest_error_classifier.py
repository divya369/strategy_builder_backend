"""
Backtest Error Classifier — maps raw engine error messages to user-friendly
messages and error types for the frontend.

Error types:
  - user_input:  User can fix this by changing their screener/date params
  - data_gap:    Missing data in backend — user should adjust params or wait
  - infra:       Server/worker issue — user should retry later
  - internal:    Unexpected bug — never show raw tracebacks to users

Usage:
    from app.core.backtest_error_classifier import classify_error

    error_type, user_message = classify_error(run.error_message)
"""

from typing import Optional, Tuple

# (substring_to_match, error_type, user_friendly_message)
# Order matters: first match wins.
_ERROR_RULES = [
    # ── User input errors ─────────────────────────────────────────────────
    (
        "No rebalance dates found",
        "user_input",
        "No screener data available for the selected date range. "
        "Try a different date range.",
    ),
    (
        "No eligible symbols",
        "user_input",
        "Your screener filters returned zero stocks. "
        "Try relaxing your filters or expanding the universe.",
    ),
    # ── Data gap errors ───────────────────────────────────────────────────
    (
        "No OHLC data found",
        "data_gap",
        "Price data not available for the selected stocks/dates.",
    ),
    (
        "No basket dates matched trading dates",
        "data_gap",
        "No trading dates found in the selected range.",
    ),
    (
        "Simulation produced no NAV rows",
        "data_gap",
        "Simulation could not produce results for this configuration.",
    ),
    # ── Infrastructure errors ─────────────────────────────────────────────
    (
        "heartbeat became stale",
        "infra",
        "Server was restarted during your backtest. Please run again.",
    ),
    (
        "old job heartbeat is stale",
        "infra",
        "Server was restarted during your backtest. Please run again.",
    ),
    (
        "worker/server stopped",
        "infra",
        "Server was restarted during your backtest. Please run again.",
    ),
    (
        "soft time limit",
        "infra",
        "Backtest took too long (over 2 hours). "
        "Try a shorter date range or fewer stocks.",
    ),
    (
        "2-hour",
        "infra",
        "Backtest took too long (over 2 hours). "
        "Try a shorter date range or fewer stocks.",
    ),
]

# Fallback for unrecognized errors (raw tracebacks, DB errors, etc.)
_DEFAULT_TYPE = "internal"
_DEFAULT_MESSAGE = "Something went wrong. Please try again or contact support."


def classify_error(
    raw_error: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Classify a raw error_message into (error_type, user_friendly_message).

    Returns (None, None) if raw_error is None or empty (no error).
    """
    if not raw_error:
        return None, None

    raw_lower = raw_error.lower()

    for substring, error_type, user_message in _ERROR_RULES:
        if substring.lower() in raw_lower:
            return error_type, user_message

    # Unrecognized error — never expose raw tracebacks to the user
    return _DEFAULT_TYPE, _DEFAULT_MESSAGE
