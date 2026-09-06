"""
Pydantic schemas for network endpoints.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class NetworkSummaryResponse(BaseModel):
    """
    Top-level network KPI response.
    """

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