"""
Screener and ScreenerVersion models.
UUID primary keys throughout; user_id references users.id (UUID).
"""
import uuid
from sqlalchemy import (
    Column, String, Text, Boolean, DateTime, JSON, ForeignKey, Integer
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.models.base import Base


class Screener(Base):
    __tablename__ = "screeners"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    deleted_at = Column(DateTime, nullable=True)

    versions = relationship("ScreenerVersion", back_populates="screener", cascade="all, delete-orphan")


class ScreenerVersion(Base):
    __tablename__ = "screener_versions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    screener_id = Column(UUID(as_uuid=True), ForeignKey("screeners.id", ondelete="CASCADE"), nullable=False, index=True)
    version_number = Column(Integer, nullable=False)
    # JSON blobs store the full config state at the time of save.
    # Keyed by version so backtest results can always reference the original config.
    filters_json = Column(JSON, nullable=False)
    universe_json = Column(JSON, nullable=False)
    ranking_json = Column(JSON, nullable=True)
    is_current = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, nullable=False)

    screener = relationship("Screener", back_populates="versions")
