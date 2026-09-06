"""
Pydantic schemas for network endpoints.
"""

from datetime import date, datetime

from typing import List, Optional

from pydantic import BaseModel, Field


class NetworkSummaryResponse(BaseModel):
    total_activity: float = Field(
        description="Total network activity through the effective as_of."
    )
    active_grids: int = Field(
        description="Number of grids with positive activity."
    )
    peak_hour: int = Field(
        description="Hour of day with the highest aggregate activity.",
        ge=0,
        le=23,
    )
    top_grid: str = Field(
        description="Grid with the highest aggregate activity."
    )
    as_of: datetime = Field(
        description="Effective reporting timestamp used for this response."
    )


class GridActivityPoint(BaseModel):
    timestamp: datetime
    date: date
    hour: int = Field(ge=0, le=23)
    call_activity: float
    sms_activity: float
    internet_activity: float
    total_activity: float


class GridActivityResponse(BaseModel):
    grid_id: int
    activity: list[GridActivityPoint]
    as_of: datetime

# ---------------------------------------------------------------------------
# Activity
# ---------------------------------------------------------------------------

class ActivityMeasures(BaseModel):
    """
    Activity measures associated with a grid/hour.
    """

    total_activity: float = 0.0
    total_sms: float = 0.0
    total_calls: float = 0.0
    internet: float = 0.0

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class RiskInfo(BaseModel):
    """
    Future ML risk information.
    """

    score: Optional[float] = None
    label: Optional[str] = None
    model_version: Optional[str] = None


class HotspotItem(BaseModel):
    """
    Single network hotspot.
    """

    grid_id: int
    timestamp: datetime

    call_activity: float
    sms_activity: float
    internet_activity: float
    total_activity: float

    status: str
    reason: str

    source: str = "analytics"

    # Reserved for future ML scoring.
    risk: Optional[RiskInfo] = None


class HotspotListResponse(BaseModel):
    """
    Hotspot API response.
    """

    as_of: datetime
    count: int
    items: List[HotspotItem]


class AlertItem(BaseModel):
    """
    Single network alert.
    """

    grid_id: int
    timestamp: datetime

    call_activity: float
    sms_activity: float
    internet_activity: float
    total_activity: float

    status: str
    severity: str
    reason: str

    source: str = "rule"

    # Reserved for future ML scoring.
    risk: Optional[RiskInfo] = None


class AlertListResponse(BaseModel):
    """
    Alert API response.
    """

    as_of: datetime
    count: int
    items: List[AlertItem]