"""
FastAPI application.

Network Operations Intelligence
"""

from fastapi import FastAPI

from src.api.routes.network import router as network_router


app = FastAPI(
    title="Network Operations Intelligence API",
    description=(
        "API layer for the Network Operations Intelligence "
        "analytics platform."
    ),
    version="1.0.0",
)


app.include_router(network_router)


@app.get(
    "/",
    tags=["Health"],
    summary="API health check",
)
def root():
    return {
        "service": "Network Operations Intelligence API",
        "status": "running",
    }