from fastapi import APIRouter, HTTPException, Query

from src.api.schemas.operational import (
    GridLocationResponse,
    GridNeighboursResponse,
    PipelineStatusResponse,
)
from src.api.services.operational_service import (
    get_grid_location,
    get_grid_neighbours,
    read_pipeline_status,
)


router = APIRouter(
    tags=["Operational Support"],
)


@router.get(
    "/pipeline/status",
    response_model=PipelineStatusResponse,
    summary="Get pipeline operational status",
)
def pipeline_status():
    """
    Return machine-readable evidence about the latest
    DE7 pipeline execution and analytics freshness.
    """

    return read_pipeline_status()


@router.get(
    "/network/grid/{grid_id}/location",
    response_model=GridLocationResponse,
    summary="Get grid geographic location",
)
def grid_location(grid_id: str):

    result = get_grid_location(grid_id)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Grid not found",
                "grid_id": grid_id,
            },
        )

    return result


@router.get(
    "/network/grid/{grid_id}/neighbours",
    response_model=GridNeighboursResponse,
    summary="Get nearby grid cells",
)
def grid_neighbours(
    grid_id: str,
    limit: int = Query(
        default=5,
        ge=1,
        le=20,
    ),
):

    result = get_grid_neighbours(
        grid_id=grid_id,
        limit=limit,
    )

    if result is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Grid not found",
                "grid_id": grid_id,
            },
        )

    return result