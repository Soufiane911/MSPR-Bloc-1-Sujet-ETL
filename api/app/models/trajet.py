"""
Modeles Pydantic pour les trajets ferroviaires.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class Trajet(BaseModel):
    """Modele metier expose par l'API /trajets."""

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "trajet_id": 1,
                "train_number": "TGV 1234",
                "operator_name": "SNCF",
                "train_type": "day",
                "origin": "Paris Gare de Lyon",
                "destination": "Lyon Part-Dieu",
                "origin_country": "FR",
                "destination_country": "FR",
                "origin_latitude": 48.8443,
                "origin_longitude": 2.3744,
                "destination_latitude": 45.7600,
                "destination_longitude": 4.8590,
                "departure_time": "2026-02-23T08:00:00",
                "arrival_time": "2026-02-23T10:00:00",
                "duration_min": 120,
                "distance_km": 460.0,
            }
        },
    )

    trajet_id: int = Field(..., description="Identifiant unique du trajet")
    train_number: str = Field(..., description="Numero du train")
    operator_name: str = Field(..., description="Nom de l'operateur")
    train_type: str = Field(..., description="Type: 'day' ou 'night'")
    origin: str = Field(..., description="Gare de depart")
    destination: str = Field(..., description="Gare d'arrivee")
    origin_country: str = Field(..., description="Pays de depart")
    destination_country: str = Field(..., description="Pays d'arrivee")
    origin_latitude: Optional[float] = Field(None, description="Latitude de depart")
    origin_longitude: Optional[float] = Field(None, description="Longitude de depart")
    destination_latitude: Optional[float] = Field(None, description="Latitude d'arrivee")
    destination_longitude: Optional[float] = Field(None, description="Longitude d'arrivee")
    departure_time: Optional[datetime] = Field(None, description="Heure de depart")
    arrival_time: Optional[datetime] = Field(None, description="Heure d'arrivee")
    duration_min: Optional[int] = Field(None, description="Duree en minutes")
    distance_km: Optional[float] = Field(None, description="Distance en kilometres")
