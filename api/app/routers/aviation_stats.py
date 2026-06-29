"""
Router for aviation statistics endpoints.
"""

from fastapi import APIRouter, Query

from app.services.aviation_stats_service import AviationStatsService

router = APIRouter()
aviation_stats_service = AviationStatsService()


@router.get("/summary")
def get_summary_stats():
    """Return global aviation dataset statistics."""
    return aviation_stats_service.get_summary()


@router.get("/byCountry")
def get_stats_by_country():
    """Return aviation route statistics by origin country."""
    return aviation_stats_service.get_by_country()


@router.get("/topRoutes")
def get_top_routes(limit: int = Query(20, ge=1, le=100)):
    """Return the most representative aviation routes for the demo."""
    return aviation_stats_service.get_top_routes(limit=limit)


@router.get("/topAirports")
def get_top_airports(limit: int = Query(20, ge=1, le=100)):
    """Return airports with the highest observed route activity."""
    return aviation_stats_service.get_top_airports(limit=limit)


@router.get("/airlineSummary")
def get_airline_summary(limit: int = Query(50, ge=1, le=200)):
    """Return route coverage statistics by airline code."""
    return aviation_stats_service.get_airline_summary(limit=limit)


@router.get("/dataQuality")
def get_data_quality():
    """Return data quality metrics for the aviation CSV snapshots."""
    return aviation_stats_service.get_data_quality()
