import uuid
from pydantic import BaseModel, field_validator, ConfigDict
from typing import List, Optional
from datetime import datetime

class CustomBacktestRequest(BaseModel):
    user_id: Optional[uuid.UUID] = None
    screener_id: Optional[uuid.UUID] = None
    screener_version_id: Optional[uuid.UUID] = None
    run_name: Optional[str] = None
    from_date: str
    to_date: str
    frequency: str = "weekly"
    universe: dict = {}
    filters: List[dict] = []
    ranking: dict = {}
    wrh: int = 40
    portfolio_size: int = 30
    initial_capital: float = 1_000_000.0
    transaction_cost_bps: float = 20.0
    slippage_bps: float = 10.0

    @field_validator("to_date", mode="after")
    @classmethod
    def to_date_after_from(cls, v, info):
        from_date_str = info.data.get("from_date")
        if from_date_str:
            from datetime import datetime as _dt
            try:
                if _dt.strptime(v, "%Y-%m-%d").date() <= _dt.strptime(from_date_str, "%Y-%m-%d").date():
                    raise ValueError("to_date must be after from_date")
            except ValueError as e:
                raise e
        return v
