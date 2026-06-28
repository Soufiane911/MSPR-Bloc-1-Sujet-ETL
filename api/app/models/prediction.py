"""Schemas Pydantic pour la prediction ML substitution."""

from datetime import datetime
from typing import Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class PredictionRequest(BaseModel):
    """Entree du endpoint /predict selon le contrat ML."""

    model_config = ConfigDict(
        protected_namespaces=(),
        json_schema_extra={
            "example": {
                "distance_km": 820.0,
                "duration_min": 510,
                "is_international": True,
                "train_type": "night",
                "weekly_frequency": 7,
                "estimated_co2_saving_kg": 140.5,
                "origin_country": "FR",
                "destination_country": "IT",
                "avg_speed_kmh": 96.47,
            }
        }
    )

    distance_km: float = Field(..., gt=0, description="Distance estimee du trajet")
    duration_min: int = Field(..., gt=0, description="Duree du trajet en minutes")
    is_international: bool = Field(..., description="Liaison internationale ou domestique")
    train_type: Literal["day", "night"] = Field(..., description="Type de train")
    weekly_frequency: float = Field(..., ge=0, description="Frequence hebdomadaire de la liaison")
    estimated_co2_saving_kg: float = Field(
        ..., ge=0, description="Gain CO2 estime si report modal"
    )
    origin_country: str = Field(..., min_length=2, max_length=8, description="Code pays origine")
    destination_country: str = Field(..., min_length=2, max_length=8, description="Code pays destination")
    avg_speed_kmh: Optional[float] = Field(
        None,
        gt=0,
        description="Vitesse moyenne optionnelle; calculee automatiquement si absente",
    )


class PredictionResponse(BaseModel):
    """Sortie du endpoint /predict selon le contrat ML."""

    model_config = ConfigDict(
        protected_namespaces=(),
        json_schema_extra={
            "example": {
                "prediction": "fort_potentiel",
                "confidence": 0.78,
                "probabilities": {
                    "faible_potentiel": 0.03,
                    "potentiel_moyen": 0.19,
                    "fort_potentiel": 0.78
                },
                "explanation": "Prediction fort_potentiel basee sur : distance elevee, ...",
                "model_name": "gradient_boosting",
                "model_version": "2026-06-27T16:31:41.311962+00:00",
                "inference_ms": 2.41,
                "predicted_at": "2026-06-28T10:30:00",
            }
        }
    )

    prediction: str
    confidence: float = Field(..., ge=0, le=1)
    probabilities: Dict[str, float]
    explanation: str
    model_name: str
    model_version: str
    inference_ms: float = Field(..., ge=0)
    predicted_at: datetime
