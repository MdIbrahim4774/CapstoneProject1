"""
Network routes.

API1 - Network Summary
API2 - Grid Activity
"""

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas.network import (
    NetworkSummaryResponse,
    GridActivityResponse,
)
from src.api.services.network_service import (
    get_network_summary,
    get_grid_activity,
)
from src.db.connection import get_connection


router = APIRouter(
    prefix="/network",
    tags=["Network"],
)


@router.get("/summary", response_model=NetworkSummaryResponse)
def network_summary(
    as_of: datetime | None = Query(default=None),
) -> NetworkSummaryResponse:
    connection = None
    try:
        connection = get_connection()
        result = get_network_summary(
            connection=connection,
            requested_as_of=as_of,
        )
        return NetworkSummaryResponse(**result)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Analytics data source unavailable",
                "message": str(exc),
            },
        ) from exc
    finally:
        if connection is not None:
            connection.close()


@router.get("/grid/{grid_id}", response_model=GridActivityResponse)
def grid_activity(
    grid_id: int,
    date: str | None = Query(default=None),
    hour: int | None = Query(default=None, ge=0, le=23),
    as_of: datetime | None = Query(default=None),
) -> GridActivityResponse:
    if not 1 <= grid_id <= 10000:
        raise HTTPException(status_code=404, detail="Grid not found")

    connection = None
    try:
        connection = get_connection()
        result = get_grid_activity(
            connection=connection,
            grid_id=grid_id,
            requested_date=date,
            requested_hour=hour,
            requested_as_of=as_of,
        )
        return GridActivityResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Analytics data source unavailable",
                "message": str(exc),
            },
        ) from exc
    finally:
        if connection is not None:
            connection.close()