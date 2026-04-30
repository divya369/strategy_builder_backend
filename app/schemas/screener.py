import uuid
from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import List, Any, Optional

class UniverseConfig(BaseModel):
    type: str
    value: str

class FilterConfig(BaseModel):
    field: Optional[str] = None
    operator: Optional[str] = None
    value: Any = None
    type: Optional[str] = None
    period: Optional[str] = None
    relation: Optional[str] = None
    left_field: Optional[str] = None
    left_period: Optional[str] = None
    right_field: Optional[str] = None
    right_period: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def clean_null_strings(cls, values):
        """Convert "null" strings to actual None so exclude_none works."""
        if isinstance(values, dict):
            return {k: (None if v == "null" else v) for k, v in values.items()}
        return values

class RankingConfig(BaseModel):
    field: str
    order: str
    period: Optional[str] = None

class RebalanceConfig(BaseModel):
    frequency: str
    max_positions: int

class ScreenerCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    is_active: bool = True
    user_id: str
    universe: UniverseConfig
    filters: List[FilterConfig]
    ranking: Optional[RankingConfig] = None
    rebalance: Optional[RebalanceConfig] = None

class ScreenerVersionCreate(BaseModel):
    description: Optional[str] = None
    universe: UniverseConfig
    filters: List[FilterConfig]
    ranking: Optional[RankingConfig] = None
    rebalance: Optional[RebalanceConfig] = None

class ScreenerVersionResponse(BaseModel):
    screener_id: uuid.UUID
    name: str
    version_id: uuid.UUID
    version_number: int
    message: str

class ScreenerResponse(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    description: Optional[str] = None
    is_active: bool
    model_config = ConfigDict(from_attributes=True)
