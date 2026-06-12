import sys
import os
import uuid
from datetime import date, datetime

# Add the project root to sys.path so we can import 'app'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import text
from app.core.database import EquitycaseSessionLocal
from app.services.notifications import notify_all
from app.core.config import settings

def test_email_flow(user_id=None):
    print("=== Testing Email Notification Flow ===")
    
    ec_db = EquitycaseSessionLocal()
    try:
        if user_id:
            # Query specific user
            print(f"Querying equitycase DB for user_id: {user_id}...")
            row = ec_db.execute(
                text("SELECT id, email FROM users WHERE id = :uid LIMIT 1"),
                {"uid": user_id},
            ).fetchone()
        else:
            # Pick the first user we can find to test
            print("No user_id provided. Finding the first available user in equitycase DB...")
            row = ec_db.execute(
                text("SELECT id, email FROM users WHERE email IS NOT NULL LIMIT 1")
            ).fetchone()
            
        if not row:
            print("❌ No user found in the equitycase database. Please check your DB connection.")
            return

        target_user_id = row.id
        user_email = row.email
        user_name = getattr(row, "name", None) or "Test User"
        
        print(f"✅ Found User: {user_name} ({user_email}) | ID: {target_user_id}")
        
    except Exception as e:
        print(f"❌ Error querying equitycase DB: {e}")
        return
    finally:
        ec_db.close()

    print("\nPreparing dummy rebalance data...")
    strategy_name = "Dummy Alpha Strategy"
    strategy_id = str(uuid.uuid4())
    dashboard_url = f"{settings.FRONTEND_BASE_URL}/live-investment/{strategy_id}"
    
    # Dummy Buy / Sell rows
    changes = [
        {"tradingsymbol": "RELIANCE", "action": "SELL", "qty": 15},
        {"tradingsymbol": "TCS", "action": "SELL", "qty": 10},
        {"tradingsymbol": "INFY", "action": "BUY", "qty": 20},
        {"tradingsymbol": "HDFCBANK", "action": "BUY", "qty": 35},
    ]
    
    import pytz
    ist = pytz.timezone("Asia/Kolkata")
    timestamp = datetime.now(ist).strftime("%d %b %Y, %I:%M %p IST")
    rebalance_date = "Today"

    print("Sending email via notify_all...")
    try:
        # Trigger the same notification function the real app uses
        notify_all(
            "send_rebalance_ready",
            user_email=user_email,
            user_name=user_name,
            strategy_name=strategy_name,
            strategy_id=strategy_id,
            changes=changes,
            dashboard_url=dashboard_url,
            timestamp=timestamp,
            rebalance_date=rebalance_date,
        )
        print("\n✅ Email send request completed! Check the logs/console for Resend success or failure.")
        print(f"Check the inbox of {user_email} to verify.")
    except Exception as e:
        print(f"\n❌ Failed to send email: {e}")

if __name__ == "__main__":
    target_id = "d135d480-8fdf-4179-b535-81477d379fec"
    test_email_flow(target_id)
