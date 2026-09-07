"""
FastAPI application.

Network Operations Intelligence
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes.network import router as network_router
from src.api.routes.operational import router as operational_router


app = FastAPI(
    title="Network Operations Intelligence API",
    description=(
        "API layer for the Network Operations Intelligence "
        "analytics platform."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(network_router)
app.include_router(operational_router)


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