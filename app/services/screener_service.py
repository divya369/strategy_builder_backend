import uuid
from datetime import datetime
from sqlalchemy.orm import Session
from app.models.screener import Screener
from app.schemas.screener import ScreenerCreate

class ScreenerService:
    def create_screener(self, db: Session, screener_in: ScreenerCreate, user_id: uuid.UUID) -> Screener:
        # Platform strategies start inactive: admin reviews the data first, then activates
        # via the toggle endpoint to make it visible to users.
        is_active = False if screener_in.role == "platform" else screener_in.is_active
        db_screener = Screener(user_id=user_id, name=screener_in.name, description=screener_in.description, role=screener_in.role, is_active=is_active, created_at=datetime.utcnow(), updated_at=datetime.utcnow())
        db.add(db_screener); db.commit(); db.refresh(db_screener)
        return db_screener

    def get_screener(self, db: Session, screener_id: uuid.UUID) -> Screener:
        return db.query(Screener).filter(Screener.id == screener_id).first()

    def soft_delete_screener(self, db: Session, screener_id: uuid.UUID) -> Screener:
        screener = db.query(Screener).filter(Screener.id == screener_id).first()
        if not screener: return None
        if not screener.is_active: return screener
        screener.is_active = False; screener.deleted_at = datetime.utcnow()
        db.add(screener); db.commit(); db.refresh(screener)
        return screener

    def toggle_screener_active(self, db: Session, screener_id: uuid.UUID) -> Screener:
        """Flip is_active: active → inactive (sets deleted_at), inactive → active (clears deleted_at)."""
        screener = db.query(Screener).filter(Screener.id == screener_id).first()
        if not screener: return None
        if screener.is_active:
            screener.is_active = False; screener.deleted_at = datetime.utcnow()
        else:
            screener.is_active = True; screener.deleted_at = None
        db.add(screener); db.commit(); db.refresh(screener)
        return screener

screener_service = ScreenerService()
