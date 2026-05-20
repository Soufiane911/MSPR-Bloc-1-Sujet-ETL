"""
API router pour les aéroports.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.services.airport_service import AirportService

router = APIRouter()
service = AirportService()


@router.get("/")
def get_airports(
    country: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    return service.get_airports(country=country, city=city, name=name, limit=limit, offset=offset)


@router.get("/{airport_id}")
def get_airport(airport_id: int):
    a = service.get_airport_by_id(airport_id)
    if not a:
        raise HTTPException(status_code=404, detail="Airport not found")
    return a


@router.get("/count/by-country")
def count_airports_by_country():
    return service.count_by_country()
