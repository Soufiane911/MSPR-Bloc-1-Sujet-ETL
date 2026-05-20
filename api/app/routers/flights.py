"""
API router pour les vols.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.services.flight_service import FlightService

router = APIRouter()
service = FlightService()


@router.get("/")
def get_flights(
    airline: Optional[str] = Query(None),
    origin: Optional[str] = Query(None),
    destination: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    return service.get_flights(airline=airline, origin=origin, destination=destination, limit=limit, offset=offset)


@router.get("/{flight_id}")
def get_flight(flight_id: int):
    f = service.get_flight_by_id(flight_id)
    if not f:
        raise HTTPException(status_code=404, detail="Flight not found")
    return f
