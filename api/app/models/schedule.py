"""
Modèles Pydantic pour les dessertes.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class Schedule(BaseModel):
    """Modèle pour une desserte."""
    
    schedule_id: int = Field(..., description="Identifiant unique de la desserte")
    train_number: str = Field(..., description="Numéro du train")
    train_type: str = Field(..., description="Type: 'day' ou 'night'")
    origin: str = Field(..., description="Gare de départ")
    origin_city: str = Field(..., description="Ville de départ")
    origin_country: str = Field(..., description="Pays de départ")
    destination: str = Field(..., description="Gare d'arrivée")
    destination_city: str = Field(..., description="Ville d'arrivée")
    destination_country: str = Field(..., description="Pays d'arrivée")
    departure_time: datetime = Field(..., description="Heure de départ")
    arrival_time: datetime = Field(..., description="Heure d'arrivée")
    duration_min: int = Field(..., description="Durée en minutes")
    distance_km: Optional[float] = Field(None, description="Distance en km")
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "schedule_id": 1,
                "train_number": "TGV 1234",
                "train_type": "day",
                "origin": "Paris Gare de Lyon",
                "origin_city": "Paris",
                "origin_country": "FR",
                "destination": "Lyon Part-Dieu",
                "destination_city": "Lyon",
                "destination_country": "FR",
                "departure_time": "2025-02-05T08:00:00+01:00",
                "arrival_time": "2025-02-05T10:00:00+01:00",
                "duration_min": 120,
                "distance_km": 460.5
            }
        }


class ScheduleSearchResult(Schedule):
    """Résultat de recherche de dessertes."""
    
    operator_name: str = Field(..., description="Nom de l'opérateur")
    category: Optional[str] = Field(None, description="Catégorie du train")
    
    class Config:
        json_schema_extra = {
            "example": {
                "schedule_id": 1,
                "train_number": "TGV 1234",
                "operator_name": "SNCF",
                "train_type": "day",
                "category": "TGV",
                "origin": "Paris Gare de Lyon",
                "origin_city": "Paris",
                "origin_country": "FR",
                "destination": "Lyon Part-Dieu",
                "destination_city": "Lyon",
                "destination_country": "FR",
                "departure_time": "2025-02-05T08:00:00+01:00",
                "arrival_time": "2025-02-05T10:00:00+01:00",
                "duration_min": 120,
                "distance_km": 460.5
            }
        }
