"""
Router pour les statistiques aériennes.
"""

from fastapi import APIRouter, Query

from app.services.aviation_stats_service import AviationStatsService

router = APIRouter()
service = AviationStatsService()


@router.get("/summary")
def get_summary_stats():
    """Récupère les statistiques globales aviation."""
    return service.get_summary()


@router.get("/byCountry")
def get_stats_by_country():
    """Récupère les statistiques aviation par pays de compagnie."""
    return service.get_by_country()


@router.get("/topRoutes")
def get_top_routes(
    limit: int = Query(10, ge=1, le=50, description="Nombre de routes à retourner")
):
    """Récupère les routes aériennes les plus utilisées."""
    return service.get_top_routes(limit=limit)


@router.get("/topAirports")
def get_top_airports(
    limit: int = Query(10, ge=1, le=50, description="Nombre d'aéroports à retourner")
):
    """Récupère les aéroports les plus actifs."""
    return service.get_airport_activity(limit=limit)


@router.get("/airlineSummary")
def get_airline_summary():
    """Récupère le résumé par compagnie aérienne."""
    return service.get_airline_summary()


@router.get("/dataQuality")
def get_data_quality():
    """Récupère un aperçu de la qualité des données aviation."""
    return service.get_data_quality()