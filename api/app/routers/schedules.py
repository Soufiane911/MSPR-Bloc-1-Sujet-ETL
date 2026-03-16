"""
Router pour les endpoints liés aux dessertes (schedules).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import date
from app.models.schedule import Schedule, ScheduleSearchResult
from app.services.schedule_service import ScheduleService

router = APIRouter()
schedule_service = ScheduleService()


@router.get("/", response_model=List[Schedule])
def get_schedules(
    origin: Optional[str] = Query(None, description="Ville de départ (recherche partielle)"),
    destination: Optional[str] = Query(None, description="Ville d'arrivée (recherche partielle)"),
    train_type: Optional[str] = Query(None, description="Type: 'day' ou 'night'"),
    country: Optional[str] = Query(None, description="Code pays (FR, DE, etc.)"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats"),
    offset: int = Query(0, ge=0, description="Décalage pour la pagination")
):
    """
    Récupère la liste des dessertes avec filtres.
    
    - **origin**: Filtre par ville de départ (recherche partielle)
    - **destination**: Filtre par ville d'arrivée (recherche partielle)
    - **train_type**: Filtre par type (day/night)
    - **country**: Filtre par code pays
    - **limit**: Nombre maximum de résultats
    - **offset**: Décalage pour la pagination
    
    Returns:
        List[Schedule]: Liste des dessertes
    """
    schedules = schedule_service.get_schedules(
        origin=origin,
        destination=destination,
        train_type=train_type,
        country=country,
        limit=limit,
        offset=offset
    )
    return schedules


@router.get("/search", response_model=List[ScheduleSearchResult])
def search_schedules(
    from_city: str = Query(..., description="Ville de départ (obligatoire)"),
    to_city: str = Query(..., description="Ville d'arrivée (obligatoire)"),
    travel_date: Optional[date] = Query(None, description="Date de voyage (optionnel)"),
    train_type: Optional[str] = Query(None, description="Type: 'day' ou 'night'")
):
    """
    Recherche de dessertes entre deux villes.
    
    Cet endpoint permet de rechercher toutes les dessertes disponibles
    entre une ville de départ et une ville d'arrivée.
    
    - **from_city**: Ville de départ (obligatoire)
    - **to_city**: Ville d'arrivée (obligatoire)
    - **travel_date**: Date de voyage (optionnel)
    - **train_type**: Filtre par type (day/night)
    
    Returns:
        List[ScheduleSearchResult]: Résultats de la recherche
        
    Raises:
        HTTPException: 404 si aucune desserte n'est trouvée
    """
    results = schedule_service.search_schedules(
        from_city=from_city,
        to_city=to_city,
        travel_date=travel_date,
        train_type=train_type
    )
    
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune desserte trouvée entre {from_city} et {to_city}"
        )
    
    return results


@router.get("/{schedule_id}")
def get_schedule(schedule_id: int):
    """
    Récupère les détails d'une desserte spécifique.
    
    - **schedule_id**: Identifiant unique de la desserte
    
    Returns:
        dict: Détails de la desserte
        
    Raises:
        HTTPException: 404 si la desserte n'est pas trouvée
    """
    schedule = schedule_service.get_schedule_by_id(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Desserte non trouvée")
    return schedule
