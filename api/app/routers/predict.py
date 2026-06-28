"""
Router pour le endpoint de prediction ML.
"""

from datetime import datetime, timezone
from time import perf_counter

from fastapi import APIRouter

from app.metrics import (
    model_prediction_errors_total,
    model_prediction_latency_seconds,
    model_predictions_total,
)
from app.models.prediction import PredictionRequest, PredictionResponse
from app.services.prediction_service import predictor

router = APIRouter()


@router.post("/", response_model=PredictionResponse)
def predict(payload: PredictionRequest):
    """
    Retourne une prediction day/night.

    Cette version utilise un modele fictif pour stabiliser le contrat API.
    """
    start = perf_counter()
    try:
        result = predictor.predict(payload)
        latency = perf_counter() - start

        model_predictions_total.labels(
            model_name=result["model_name"],
            predicted_class=result["prediction"],
        ).inc()
        model_prediction_latency_seconds.labels(
            model_name=result["model_name"]
        ).observe(latency)

        return PredictionResponse(
            **result,
            inference_ms=round(latency * 1000, 3),
            predicted_at=datetime.now(timezone.utc),
        )
    except Exception:
        latency = perf_counter() - start
        model_prediction_errors_total.labels(
            model_name=getattr(predictor, "model_name", "unknown_model"),
            error_type="inference_error",
        ).inc()
        model_prediction_latency_seconds.labels(
            model_name=getattr(predictor, "model_name", "unknown_model")
        ).observe(latency)
        raise
