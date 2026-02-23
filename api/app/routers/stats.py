"""
Router pour les endpoints de statistiques.
"""

from fastapi import APIRouter, Query
from typing import Optional
from app.services.stats_service import StatsService

router = APIRouter()
stats_service = StatsService()


@router.get("/summary")
def get_summary_stats():
    """
    Récupère les statistiques globales de la base de données.
    
    Returns:
        dict: Statistiques globales
    """
    return stats_service.get_summary()


@router.get("/by-country")
def get_stats_by_country():
    """
    Récupère les statistiques par pays.
    
    Returns:
        list: Statistiques par pays
    """
    return stats_service.get_by_country()


@router.get("/day-night")
def get_day_night_comparison(
    country: Optional[str] = Query(None, description="Code pays (FR, DE, etc.)")
):
    """
    Récupère la comparaison trains de jour vs trains de nuit.
    
    - **country**: Filtre par pays (optionnel)
    
    Returns:
        dict: Comparaison jour/nuit
    """
    return stats_service.get_day_night_comparison(country=country)


@router.get("/data-quality")
def get_data_quality():
    """
    Récupère les statistiques de qualité des données.
    
    Returns:
        list: Qualité des données par table
    """
    return stats_service.get_data_quality()


@router.get("/top-routes")
def get_top_routes(
    limit: int = Query(10, ge=1, le=50, description="Nombre de routes à retourner")
):
    """
    Récupère les routes les plus fréquentées.
    
    - **limit**: Nombre de routes à retourner
    
    Returns:
        list: Top routes
    """
    return stats_service.get_top_routes(limit=limit)
