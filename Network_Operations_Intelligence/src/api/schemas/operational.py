from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class TaskStatus(BaseModel):
    status: str = Field(..., description="Task execution status")
    rows_in: int = 0
    rows_rejected: int = 0
    nulls_handled: int = 0
    rows_published: int = 0


class PipelineStatusResponse(BaseModel):
    healthy: bool
    reasons: List[str] = Field(default_factory=list)

    run_id: str
    timestamp: datetime

    tasks: Dict[str, TaskStatus]

    rows_in: int = 0
    rows_rejected: int = 0
    nulls_handled: int = 0
    rows_published: int = 0

    as_of: Optional[datetime] = None

    freshness_minutes: Optional[float] = None
    freshness_status: str


class GridLocationResponse(BaseModel):
    grid_id: str

    centroid_latitude: float
    centroid_longitude: float

    polygon_reference: str


class GridNeighbour(BaseModel):
    grid_id: str
    distance_km: float


class GridNeighboursResponse(BaseModel):
    grid_id: str
    neighbours: List[GridNeighbour]