"""
Approved WhatsApp templates (from WhatsApp Manager).

Mirrors templates.js of the WhatsApp sender tool. `params` are the NAMED body
variables of the approved template. `language`: "English (IND)" = en_IN, plain "English" = en.
"""
from __future__ import annotations
from typing import Any, Dict

TEMPLATES: Dict[str, Dict[str, Any]] = {
    # ---- Account ----
    "ec_welcome_user": {"language": "en_IN", "params": ["user_name"]},
    "ec_welcome_back": {"language": "en_IN", "params": ["user_name"]},

    # ---- Subscription & payments ----
    "ec_subscription_confirmed": {"language": "en_IN", "params": ["user_name", "plan_name", "start_date", "end_date"]},
    "ec_subscription_renewal_due": {"language": "en_IN", "params": ["user_name", "plan_name", "renewal_date"]},
    "ec_subscription_expired": {"language": "en_IN", "params": ["user_name", "plan_name", "expiry_date"]},
    "ec_payment_failed": {"language": "en_IN", "params": ["user_name", "plan_name"]},
    "ec_plan_upgrade_successful": {"language": "en", "params": ["user_name", "old_plan_name", "new_plan_name"]},
    "ec_aum_plan_limit_exceeded": {"language": "en_IN", "params": ["user_name", "current_plan_name"]},
    "ec_backtest_limit_reached": {"language": "en_IN", "params": ["user_name", "backtest_limit", "plan_name"]},

    # ---- Portfolio & orders ----
    "ec_portfolio_created": {"language": "en_IN", "params": ["user_name", "portfolio_name"]},
    "ec_broker_order_review": {"language": "en_IN", "params": ["user_name", "strategy_name"]},
    "ec_orders_placed_broker": {"language": "en_IN", "params": ["user_name", "strategy_name"]},
    "ec_rebalance_review_require": {"language": "en_IN", "params": ["user_name", "strategy_name"]},
    "ec_rebalance_completed": {"language": "en_IN", "params": ["user_name", "strategy_name"]},
    # Note the name does NOT carry the ec_ prefix — it is "rebalance_no_action" in
    # WhatsApp Manager (template id 1067566969374367) and must match exactly.
    # Approved body: "Dear {{user_name}}, no rebalance changes are required for
    # {{strategy_name}} today. Your portfolio already matches the strategy, so no
    # action is needed from you." — two variables only. Sending a third (the old
    # next_rebalance_date) makes Meta reject the send on parameter count. Its URL
    # button is static, so it needs no parameters of its own.
    "rebalance_no_action": {"language": "en_IN", "params": ["user_name", "strategy_name"]},

    # ---- Broker ----
    "ec_broker_connected": {"language": "en_IN", "params": ["user_name", "broker_name"]},
    "ec_broker_connection_failed": {"language": "en_IN", "params": ["user_name", "broker_name"]},
}
