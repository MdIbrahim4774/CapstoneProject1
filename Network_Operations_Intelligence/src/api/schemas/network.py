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


class GridFeature(BaseModel):
    """
    Stable API contract for the ML2 feature vector.

    Do not add/remove ML features here without coordinating
    with ML5, RE5 and Claude consumers.
    """

    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float
    feature_timestamp: str

    data_quality: str = Field(
        description="Quality status of the stored feature row"
    )

    freshness: str = Field(
        description="Freshness status of the stored feature row"
    )


class GridFeatureResponse(BaseModel):
    grid_id: str
    features: list[GridFeature]


class RiskPredictionRequest(BaseModel):
    """
    Stable request contract for risk prediction.

    ML5 derives hour_sin and hour_cos from feature_timestamp
    because these features were generated during ML3 training.
    """

    avg_activity: float = Field(
        ...,
        ge=0,
        description="Average network activity for the grid.",
    )

    activity_growth: float = Field(
        ...,
        description="Activity growth relative to the relevant baseline.",
    )

    active_hours: float = Field(
        ...,
        ge=0,
        le=24,
        description="Number of active hours in the observation window.",
    )

    peak_ratio: float = Field(
        ...,
        ge=0,
        description="Ratio representing peak activity relative to normal activity.",
    )

    variability: float = Field(
        ...,
        ge=0,
        description="Activity variability measure.",
    )

    internet_share: float = Field(
        ...,
        ge=0,
        le=1,
        description="Share of total activity attributable to internet usage.",
    )

    feature_timestamp: datetime = Field(
        ...,
        description="Timestamp associated with the ML2 feature row.",
    )

class RiskPredictionResponse(BaseModel):
    """
    Stable prediction response contract.

    The implementation behind this contract can change from
    stub -> trained ML model without requiring frontend changes.
    """

    risk_score: float = Field(
        ...,
        ge=0,
        le=1,
        description="Predicted network risk score between 0 and 1.",
    )

    risk_level: str = Field(
        ...,
        description="Human-readable risk classification.",
    )

    model_version: str = Field(
        ...,
        description="Version identifier of the prediction implementation/model.",
    )

    explanation_note: str = Field(
        ...,
        description="Explanation of the prediction.",
    )