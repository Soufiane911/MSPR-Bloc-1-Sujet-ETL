"""
API router pour les compagnies aériennes.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.services.airline_service import AirlineService

router = APIRouter()
service = AirlineService()


@router.get("/")
def get_airlines(
    country: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    return service.get_airlines(country=country, name=name, limit=limit, offset=offset)


@router.get("/{airline_id}")
def get_airline(airline_id: int):
    a = service.get_airline_by_id(airline_id)
    if not a:
        raise HTTPException(status_code=404, detail="Airline not found")
    return a


@router.get("/count/by-country")
def count_airlines_by_country():
    return service.count_by_country()
