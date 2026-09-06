"""
Prediction service.

API5 currently uses a stub implementation.

ML5 should replace predict_risk() with the trained-model
implementation while keeping the function signature and
response contract unchanged.
"""

from src.api.schemas.network import (
    RiskPredictionRequest,
    RiskPredictionResponse,
)


STUB_MODEL_VERSION = "stub-v1"


def predict_risk(
    request: RiskPredictionRequest,
) -> RiskPredictionResponse:
    """
    Generate a stub risk prediction.

    This function intentionally does not perform ML inference yet.

    ML5 can replace the implementation while preserving:
        - the request model
        - the response model
        - the route contract
    """

    # Contract-first placeholder.
    #
    # Keep the value deterministic so API tests remain stable.
    risk_score = 0.5

    if risk_score >= 0.7:
        risk_level = "HIGH"
    elif risk_score >= 0.4:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    return RiskPredictionResponse(
        risk_score=risk_score,
        risk_level=risk_level,
        model_version=STUB_MODEL_VERSION,
        explanation_note=(
            "Risk prediction implementation is currently a stub. "
            "A trained ML model will replace this implementation in ML5."
        ),
    )