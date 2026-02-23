"""
Router pour les endpoints liés aux gares (stations).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.services.station_service import StationService

router = APIRouter()
station_service = StationService()


@router.get("/")
def get_stations(
    country: Optional[str] = Query(None, description="Code pays (FR, DE, etc.)"),
    city: Optional[str] = Query(None, description="Ville (recherche partielle)"),
    name: Optional[str] = Query(None, description="Nom de la gare (recherche partielle)"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats"),
    offset: int = Query(0, ge=0, description="Décalage pour la pagination")
):
    """
    Récupère la liste des gares avec filtres.
    
    - **country**: Filtre par code pays
    - **city**: Filtre par ville (recherche partielle)
    - **name**: Filtre par nom de gare (recherche partielle)
    - **limit**: Nombre maximum de résultats
    - **offset**: Décalage pour la pagination
    
    Returns:
        list: Liste des gares
    """
    stations = station_service.get_stations(
        country=country,
        city=city,
        name=name,
        limit=limit,
        offset=offset
    )
    return stations


@router.get("/{station_id}")
def get_station(station_id: int):
    """
    Récupère les détails d'une gare spécifique.
    
    - **station_id**: Identifiant unique de la gare
    
    Returns:
        dict: Détails de la gare
        
    Raises:
        HTTPException: 404 si la gare n'est pas trouvée
    """
    station = station_service.get_station_by_id(station_id)
    if not station:
        raise HTTPException(status_code=404, detail="Gare non trouvée")
    return station


@router.get("/{station_id}/departures")
def get_station_departures(
    station_id: int,
    train_type: Optional[str] = Query(None, description="Type: 'day' ou 'night'"),
    limit: int = Query(50, ge=1, le=500, description="Nombre maximum de résultats")
):
    """
    Récupère les départs d'une gare.
    
    - **station_id**: Identifiant unique de la gare
    - **train_type**: Filtre par type (day/night)
    - **limit**: Nombre maximum de résultats
    
    Returns:
        list: Liste des départs
        
    Raises:
        HTTPException: 404 si la gare n'est pas trouvée
    """
    # Vérifier que la gare existe
    station = station_service.get_station_by_id(station_id)
    if not station:
        raise HTTPException(status_code=404, detail="Gare non trouvée")
    
    departures = station_service.get_departures(
        station_id=station_id,
        train_type=train_type,
        limit=limit
    )
    return departures


@router.get("/{station_id}/arrivals")
def get_station_arrivals(
    station_id: int,
    train_type: Optional[str] = Query(None, description="Type: 'day' ou 'night'"),
    limit: int = Query(50, ge=1, le=500, description="Nombre maximum de résultats")
):
    """
    Récupère les arrivées à une gare.
    
    - **station_id**: Identifiant unique de la gare
    - **train_type**: Filtre par type (day/night)
    - **limit**: Nombre maximum de résultats
    
    Returns:
        list: Liste des arrivées
        
    Raises:
        HTTPException: 404 si la gare n'est pas trouvée
    """
    # Vérifier que la gare existe
    station = station_service.get_station_by_id(station_id)
    if not station:
        raise HTTPException(status_code=404, detail="Gare non trouvée")
    
    arrivals = station_service.get_arrivals(
        station_id=station_id,
        train_type=train_type,
        limit=limit
    )
    return arrivals


@router.get("/count/by-country")
def count_stations_by_country():
    """
    Compte les gares par pays.
    
    Returns:
        dict: Comptage par pays
    """
    return station_service.count_by_country()
