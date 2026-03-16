"""
Modèles Pydantic pour les trains.
"""

from pydantic import BaseModel, ConfigDict, Field
from typing import Optional
from datetime import datetime


class ClassificationMetadata(BaseModel):
    """Metadonnees de classification jour/nuit."""

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "classification_method": "rule",
                "classification_reason": "Back-on-Track night train list",
                "classification_confidence": 1.0,
                "ml_night_probability": None,
                "night_percentage": 85.0,
                "needs_manual_review": False,
            }
        },
    )

    classification_method: Optional[str] = Field(
        None, description="Methode utilisee: rule, heuristic, ml, ou combinaison"
    )
    classification_reason: Optional[str] = Field(
        None, description="Raison detaillee de la classification"
    )
    classification_confidence: Optional[float] = Field(
        None, ge=0, le=1, description="Niveau de confiance (0-1)"
    )
    ml_night_probability: Optional[float] = Field(
        None, ge=0, le=1, description="Probabilite ML que ce soit un train de nuit"
    )
    night_percentage: Optional[float] = Field(
        None, description="Pourcentage du trajet effectue de nuit"
    )
    needs_manual_review: Optional[bool] = Field(
        False, description="Indique si une verification manuelle est necessaire"
    )


class Train(BaseModel):
    """Modèle de base pour un train."""

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "train_id": 1,
                "train_number": "TGV 1234",
                "operator_name": "SNCF",
                "train_type": "day",
                "category": "TGV",
                "route_name": "Paris - Lyon",
            }
        },
    )

    train_id: int = Field(..., description="Identifiant unique du train")
    train_number: str = Field(..., description="Numéro du train")
    operator_name: str = Field(..., description="Nom de l'opérateur")
    train_type: str = Field(..., description="Type: 'day' ou 'night'")
    category: Optional[str] = Field(None, description="Catégorie (TGV, ICE, etc.)")
    route_name: Optional[str] = Field(None, description="Nom de la ligne")


class TrainDetail(Train):
    """Modèle détaillé pour un train."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "train_id": 1,
                "train_number": "TGV 1234",
                "operator_name": "SNCF",
                "operator_id": 1,
                "train_type": "day",
                "category": "TGV",
                "route_name": "Paris - Lyon",
                "country": "FR",
                "classification": {
                    "classification_method": "rule",
                    "classification_reason": "TGV identified as day train",
                    "classification_confidence": 1.0,
                    "ml_night_probability": None,
                    "night_percentage": 0.0,
                    "needs_manual_review": False,
                },
                "created_at": "2025-01-15T10:30:00",
                "updated_at": "2025-01-15T10:30:00",
            }
        }
    )

    operator_id: int = Field(..., description="Identifiant de l'opérateur")
    country: str = Field(..., description="Code pays de l'opérateur")
    classification: Optional[ClassificationMetadata] = Field(
        None, description="Metadonnees de classification jour/nuit"
    )
    created_at: datetime = Field(..., description="Date de création")
    updated_at: datetime = Field(..., description="Date de mise à jour")
