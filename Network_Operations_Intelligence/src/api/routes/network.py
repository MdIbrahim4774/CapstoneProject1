"""
Network routes.

API1 - Network Summary
API2 - Grid Activity
"""

from datetime import datetime

from typing import List, Optional

from pydantic import BaseModel, Field

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas.network import (
    NetworkSummaryResponse,
    GridActivityResponse,
    AlertListResponse,
    HotspotListResponse,
    GridFeatureResponse,
    RiskPredictionRequest,
    RiskPredictionResponse,
)

from src.api.services.prediction_service import predict_risk

from src.api.services.network_service import (
    get_network_summary,
    get_grid_activity,
    get_alerts,
    get_hotspots,
    get_grid_features,
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

# ---------------------------------------------------------------------------
# HOTSPOTS
# ---------------------------------------------------------------------------

@router.get(
    "/hotspots",
    response_model=HotspotListResponse,
    summary="Get network activity hotspots",
)
def network_hotspots(
    limit: int = Query(
        default=20,
        ge=1,
        le=500,
        description="Maximum number of hotspot records to return.",
    ),
    as_of: datetime | None = Query(
        default=None,
        description=(
            "Reporting timestamp. If omitted, the latest available "
            "timestamp is used."
        ),
    ),
) -> HotspotListResponse:
    """
    Return the highest-activity grids.
    """

    connection = None

    try:
        connection = get_connection()

        result = get_hotspots(
            connection=connection,
            limit=limit,
            requested_as_of=as_of,
        )

        return HotspotListResponse(**result)

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


# ---------------------------------------------------------------------------
# ALERTS
# ---------------------------------------------------------------------------

@router.get(
    "/alerts",
    response_model=AlertListResponse,
    summary="Get network alerts",
)
def network_alerts(
    limit: int = Query(
        default=20,
        ge=1,
        le=500,
        description="Maximum number of alerts to return.",
    ),
    severity: str | None = Query(
        default=None,
        description=(
            "Filter by severity. Examples: LOW, MEDIUM, HIGH, CRITICAL."
        ),
    ),
    as_of: datetime | None = Query(
        default=None,
        description=(
            "Reporting timestamp. If omitted, the latest available "
            "timestamp is used."
        ),
    ),
) -> AlertListResponse:
    """
    Return rule-based NP3 network alerts.
    """

    connection = None

    try:
        connection = get_connection()

        result = get_alerts(
            connection=connection,
            limit=limit,
            severity=severity,
            requested_as_of=as_of,
        )

        return AlertListResponse(**result)

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



@router.get(
    "/network/grid/{grid_id}/features",
    response_model=GridFeatureResponse,
)
def grid_features(
    grid_id: str,
    start_time: datetime = Query(...),
    end_time: datetime = Query(...),
):
    connection = None

    try:
        connection = get_connection()

        result = get_grid_features(
            connection=connection,
            grid_id=grid_id,
            start_time=start_time,
            end_time=end_time,
        )

        return result

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "Analytics data source unavailable",
                "message": str(exc),
            },
        )

    finally:
        if connection is not None:
            connection.close()

@router.post(
    "/network/predict-risk",
    response_model=RiskPredictionResponse,
)
def predict_network_risk(
    request: RiskPredictionRequest,
) -> RiskPredictionResponse:
    """
    Predict network risk for the supplied feature set.

    API5 currently returns a deterministic stub prediction.
    ML5 will replace the service implementation without changing
    this endpoint's request or response contract.
    """

    try:
        return predict_risk(request)

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Prediction service unavailable",
                "message": str(exc),
            },
        ) from exc