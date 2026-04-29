import uuid
from pydantic import BaseModel, ConfigDict
from typing import List, Optional, Union
from datetime import date


class MetricItem(BaseModel):
    """Single metric entry in the overview response."""
    label: str
    value: Optional[Union[float, int]] = None
    unit: str


class OverviewSection(BaseModel):
    """One section of grouped metrics."""
    section: str
    metrics: List[MetricItem]


class BacktestDailyNavPoint(BaseModel):
    trade_date: date
    portfolio_nav_gross: float
    portfolio_nav_net: float
    benchmark_nav: Optional[float] = None
    drawdown: Optional[float] = None
    model_config = ConfigDict(from_attributes=True)

class BacktestDrawdownEpisodeResponse(BaseModel):
    peak_date: date
    trough_date: date
    recovery_date: Optional[date] = None
    drawdown_pct: float
    peak_to_trough_days: int
    trough_to_recovery_days: Optional[int] = None
    total_recovery_days: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)

class BacktestMonthlyReturnResponse(BaseModel):
    year: int
    month: int
    monthly_return: float
    benchmark_monthly_return: Optional[float] = None
    excess_monthly_return: Optional[float] = None
    model_config = ConfigDict(from_attributes=True)

class BacktestRebalanceEventResponse(BaseModel):
    rebalance_date: date
    portfolio_value_before: float
    portfolio_value_after: float
    turnover: float
    transaction_cost: float
    positions_before: int
    positions_after: int
    added_count: int
    dropped_count: int
    retained_count: int
    model_config = ConfigDict(from_attributes=True)
