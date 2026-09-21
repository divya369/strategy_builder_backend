"""
Manual test for the WhatsApp rebalance notification.

Usage (from project root):
    python app/core/test_whatsapp_flow.py <phone> [user_name] [strategy_name]
    python app/core/test_whatsapp_flow.py 9106566100 "Dhruv Bhai" "Momentum Top 10"

Sends ec_rebalance_review_require directly — does NOT touch the DB or the email channel.
"""
import sys
import os

# Add the project root to sys.path so we can import 'app'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.core.config import settings
from app.services.notifications.whatsapp_channel import send_template, normalize_phone, WhatsAppError


def test_whatsapp_flow(phone: str, user_name: str, strategy_name: str):
    print("=== Testing WhatsApp Rebalance Notification ===")
    print(f"Phone number ID : {settings.WHATSAPP_PHONE_NUMBER_ID}")
    print(f"Graph API       : {settings.WHATSAPP_GRAPH_API_VERSION}")
    print(f"Token loaded    : {'yes' if settings.WHATSAPP_TOKEN else 'NO — set WHATSAPP_TOKEN in .env'}")
    print(f"To              : {normalize_phone(phone)}")
    print(f"user_name       : {user_name}")
    print(f"strategy_name   : {strategy_name}\n")

    try:
        result = send_template(
            phone,
            "ec_rebalance_review_require",
            {"user_name": user_name, "strategy_name": strategy_name},
        )
        print("✅ Accepted by WhatsApp")
        print(f"   wamid: {(result.get('messages') or [{}])[0].get('id')}")
        print("   Check the phone to confirm delivery.")
    except WhatsAppError as e:
        print(f"❌ {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    test_whatsapp_flow(
        phone=sys.argv[1],
        user_name=sys.argv[2] if len(sys.argv) > 2 else "Test User",
        strategy_name=sys.argv[3] if len(sys.argv) > 3 else "Dummy Alpha Strategy",
    )
