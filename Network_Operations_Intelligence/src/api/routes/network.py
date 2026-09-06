"""
Network routes.

API1 - Network Summary
"""

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas.network import NetworkSummaryResponse
from src.api.services.network_service import get_network_summary
from src.db.connection import get_connection


router = APIRouter(
    prefix="/network",
    tags=["Network"],
)


@router.get(
    "/summary",
    response_model=NetworkSummaryResponse,
    summary="Get network summary",
    description=(
        "Returns total activity, active grids, peak hour, "
        "top grid, and the effective reporting timestamp. "
        "If as_of is omitted, the maximum timestamp in "
        "the analytics layer is used."
    ),
)
def network_summary(
    as_of: datetime | None = Query(
        default=None,
        description=(
            "Optional reporting timestamp. "
            "Defaults to the maximum timestamp available "
            "in the analytics layer."
        ),
    ),
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
            try:
                connection.close()
            except Exception:
                pass