"""
Modèles Pydantic pour les trains.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class Train(BaseModel):
    """Modèle de base pour un train."""
    
    train_id: int = Field(..., description="Identifiant unique du train")
    train_number: str = Field(..., description="Numéro du train")
    operator_name: str = Field(..., description="Nom de l'opérateur")
    train_type: str = Field(..., description="Type: 'day' ou 'night'")
    category: Optional[str] = Field(None, description="Catégorie (TGV, ICE, etc.)")
    route_name: Optional[str] = Field(None, description="Nom de la ligne")
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "train_id": 1,
                "train_number": "TGV 1234",
                "operator_name": "SNCF",
                "train_type": "day",
                "category": "TGV",
                "route_name": "Paris - Lyon"
            }
        }


class TrainDetail(Train):
    """Modèle détaillé pour un train."""
    
    operator_id: int = Field(..., description="Identifiant de l'opérateur")
    country: str = Field(..., description="Code pays de l'opérateur")
    created_at: datetime = Field(..., description="Date de création")
    updated_at: datetime = Field(..., description="Date de mise à jour")
    
    class Config:
        json_schema_extra = {
            "example": {
                "train_id": 1,
                "train_number": "TGV 1234",
                "operator_name": "SNCF",
                "operator_id": 1,
                "train_type": "day",
                "category": "TGV",
                "route_name": "Paris - Lyon",
                "country": "FR",
                "created_at": "2025-01-15T10:30:00",
                "updated_at": "2025-01-15T10:30:00"
            }
        }
