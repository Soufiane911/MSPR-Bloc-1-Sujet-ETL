"""
Router pour les endpoints metier lies aux trajets ferroviaires.
"""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.models.trajet import Trajet
from app.services.trajet_service import TrajetService

router = APIRouter()
trajet_service = TrajetService()


@router.get("", response_model=List[Trajet])
def get_trajets(
    country: Optional[str] = Query(None, description="Code pays (FR, DE, etc.)"),
    train_type: Optional[str] = Query(None, description="Type: 'day' ou 'night'"),
    origin: Optional[str] = Query(None, description="Gare ou ville de depart"),
    destination: Optional[str] = Query(None, description="Gare ou ville d'arrivee"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de resultats"),
    offset: int = Query(0, ge=0, description="Decalage pour la pagination"),
):
    """
    Recupere les trajets ferroviaires exploitables par le dashboard.
    """
    return trajet_service.get_trajets(
        country=country,
        train_type=train_type,
        origin=origin,
        destination=destination,
        limit=limit,
        offset=offset,
    )


@router.get("/", response_model=List[Trajet], include_in_schema=False)
def get_trajets_with_slash(
    country: Optional[str] = Query(None, description="Code pays (FR, DE, etc.)"),
    train_type: Optional[str] = Query(None, description="Type: 'day' ou 'night'"),
    origin: Optional[str] = Query(None, description="Gare ou ville de depart"),
    destination: Optional[str] = Query(None, description="Gare ou ville d'arrivee"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de resultats"),
    offset: int = Query(0, ge=0, description="Decalage pour la pagination"),
):
    """Alias avec slash final."""
    return get_trajets(
        country=country,
        train_type=train_type,
        origin=origin,
        destination=destination,
        limit=limit,
        offset=offset,
    )


@router.get("/{trajet_id}", response_model=Trajet)
def get_trajet(trajet_id: int):
    """
    Recupere le detail d'un trajet ferroviaire.
    """
    trajet = trajet_service.get_trajet_by_id(trajet_id)
    if not trajet:
        raise HTTPException(status_code=404, detail="Trajet non trouve")
    return trajet
