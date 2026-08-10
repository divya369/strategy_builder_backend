import uuid
from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from app.models.screener import Screener, ScreenerVersion
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

    def _current_version(self, db: Session, screener_id: uuid.UUID) -> ScreenerVersion:
        return db.query(ScreenerVersion).filter(
            ScreenerVersion.screener_id == screener_id,
        ).order_by(ScreenerVersion.is_current.desc(), ScreenerVersion.version_number.desc()).first()

    def resolve_platform_invest_version(
        self,
        db: Session,
        platform_screener_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> tuple:
        """Decide which screener version an 'Invest Now' should run against, WITHOUT
        creating anything yet.

        - Validates the platform strategy is investable (exists, role='platform',
          active).
        - If the user has ALREADY adopted this platform strategy (a prior invest
          activated), returns that clone's current version — a normal user screener,
          so the standard Go Live flow/guards apply and no new screener is made.
        - Otherwise returns the PLATFORM version and the platform_screener_id as the
          'defer' marker: the live strategy runs on the platform version for now and
          the user's screener is created only when it first goes ACTIVE.

        Returns (screener_version_id, defer_source_platform_id | None).
        """
        source = db.query(Screener).filter(Screener.id == platform_screener_id).first()
        if not source:
            raise HTTPException(status_code=404, detail="Platform screener not found")
        if source.role != "platform":
            raise HTTPException(status_code=400, detail="Not a platform strategy")
        if not source.is_active:
            raise HTTPException(status_code=400, detail="This platform strategy is not active")

        existing = db.query(Screener).filter(
            Screener.user_id == user_id,
            Screener.source_platform_screener_id == platform_screener_id,
            Screener.deleted_at.is_(None),
        ).first()
        if existing:
            v = self._current_version(db, existing.id)
            if v:
                return (v.id, None)  # already adopted — run on the user's own screener

        sv = self._current_version(db, source.id)
        if not sv:
            raise HTTPException(status_code=400, detail="Platform strategy has no saved version")
        return (sv.id, platform_screener_id)  # defer: run on platform version, clone at activation

    def adopt_platform_screener_for_user(
        self,
        db: Session,
        platform_screener_id: uuid.UUID,
        user_id: uuid.UUID,
        name: str,
        description: str = None,
    ) -> ScreenerVersion:
        """Get-or-create the user's own copy of a platform strategy — called when a
        deferred platform-invested strategy first goes ACTIVE.

        On the first activation the platform screener is cloned ONCE into a user-owned
        screener (role='user') so it appears in the user's strategy builder. Later
        activations of the same platform strategy REUSE that clone. No is_active
        re-check here: the user already committed at invest time, so activation must
        not fail if an admin has since paused the platform strategy.

        Returns the current version of the user's adopted screener.
        """
        source = db.query(Screener).filter(Screener.id == platform_screener_id).first()
        if not source:
            raise HTTPException(status_code=404, detail="Platform screener not found")
        if source.role != "platform":
            raise HTTPException(status_code=400, detail="Not a platform strategy")

        existing = db.query(Screener).filter(
            Screener.user_id == user_id,
            Screener.source_platform_screener_id == platform_screener_id,
            Screener.deleted_at.is_(None),
        ).first()
        if existing:
            v = self._current_version(db, existing.id)
            if v:
                return v

        source_version = self._current_version(db, source.id)
        if not source_version:
            raise HTTPException(status_code=400, detail="Platform strategy has no saved version")

        now = datetime.utcnow()
        new_screener = Screener(
            user_id=user_id, name=name,
            description=description if description is not None else source.description,
            role="user", source_platform_screener_id=platform_screener_id,
            is_active=True, created_at=now, updated_at=now,
        )
        db.add(new_screener)
        db.flush()  # need new_screener.id for the version FK

        new_version = ScreenerVersion(
            screener_id=new_screener.id,
            version_number=1,  # user's first version of their own adopted screener
            filters_json=source_version.filters_json,
            universe_json=source_version.universe_json,
            ranking_json=source_version.ranking_json,
            is_current=True,
            created_at=now,
        )
        db.add(new_version)
        db.commit()
        db.refresh(new_version)
        return new_version

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
