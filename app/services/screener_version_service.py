"""
Screener Version Service — manages creation and retrieval of screener versions.
Uses UUID for screener_id (passed through from the caller).
"""
import uuid
from datetime import datetime
from typing import Union

from sqlalchemy.orm import Session

from app.models.screener import ScreenerVersion
from app.schemas.screener import ScreenerVersionCreate


class ScreenerVersionService:

    def create_version(
        self,
        db: Session,
        screener_id: uuid.UUID,
        version_in: ScreenerVersionCreate,
        version_number: int,
    ) -> ScreenerVersion:
        # Mark all existing versions of this screener as not-current
        db.query(ScreenerVersion).filter(
            ScreenerVersion.screener_id == screener_id
        ).update({"is_current": False})

        db_version = ScreenerVersion(
            screener_id=screener_id,
            version_number=version_number,
            filters_json=[f.model_dump() for f in version_in.filters] if version_in.filters else [],
            universe_json=version_in.universe.model_dump() if version_in.universe else {},
            ranking_json=version_in.ranking.model_dump() if version_in.ranking else None,
            is_current=True,
            created_at=datetime.utcnow(),
        )
        db.add(db_version)
        db.commit()
        db.refresh(db_version)
        return db_version

    def get_latest_version(
        self, db: Session, screener_id: uuid.UUID
    ) -> ScreenerVersion:
        """
        Returns the current version of a screener.
        Strategy:
          1. Look for is_current=True (canonical latest)
          2. Fall back to highest version_number if no current flag set
        """
        current = (
            db.query(ScreenerVersion)
            .filter(
                ScreenerVersion.screener_id == screener_id,
                ScreenerVersion.is_current == True,
            )
            .order_by(ScreenerVersion.version_number.desc())
            .first()
        )
        if current:
            return current

        return (
            db.query(ScreenerVersion)
            .filter(ScreenerVersion.screener_id == screener_id)
            .order_by(ScreenerVersion.version_number.desc())
            .first()
        )


screener_version_service = ScreenerVersionService()
