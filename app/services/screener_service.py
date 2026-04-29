import uuid
from datetime import datetime
from sqlalchemy.orm import Session
from app.models.screener import Screener
from app.schemas.screener import ScreenerCreate

class ScreenerService:
    def create_screener(self, db: Session, screener_in: ScreenerCreate, user_id: uuid.UUID) -> Screener:
        db_screener = Screener(user_id=user_id, name=screener_in.name, description=screener_in.description, is_active=screener_in.is_active, created_at=datetime.utcnow(), updated_at=datetime.utcnow())
        db.add(db_screener); db.commit(); db.refresh(db_screener)
        return db_screener

    def get_screener(self, db: Session, screener_id: uuid.UUID) -> Screener:
        return db.query(Screener).filter(Screener.id == screener_id).first()

    def soft_delete_screener(self, db: Session, screener_id: uuid.UUID, user_id: uuid.UUID = None) -> Screener:
        screener = db.query(Screener).filter(Screener.id == screener_id).first()
        if not screener: return None
        if not screener.is_active: return screener
        screener.is_active = False; screener.deleted_at = datetime.utcnow()
        db.add(screener); db.commit(); db.refresh(screener)
        return screener

screener_service = ScreenerService()
