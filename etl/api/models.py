"""
Modèles Pydantic pour les réponses de l'API
"""

from pydantic import BaseModel
from typing import Optional, List
from datetime import time


# ============================================================================
# Modèles GTFS
# ============================================================================

class Agency(BaseModel):
    """Modèle pour un opérateur"""
    agency_id: str
    agency_name: str
    agency_url: Optional[str] = None
    agency_timezone: str
    agency_lang: Optional[str] = None

    class Config:
        from_attributes = True


class Route(BaseModel):
    """Modèle pour une ligne de train"""
    route_id: str
    agency_id: str
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    route_type: int
    route_color: Optional[str] = None
    route_text_color: Optional[str] = None

    class Config:
        from_attributes = True


class RouteStats(BaseModel):
    """Modèle pour les statistiques d'une ligne (vue Gold)"""
    route_id: str
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    route_type: Optional[int] = None
    agency_name: str
    num_trips: int
    num_stops: int
    num_trips_with_stops: int
    avg_stops_per_trip: Optional[float] = None
    min_stops_per_trip: Optional[int] = None
    max_stops_per_trip: Optional[int] = None

    class Config:
        from_attributes = True


class Stop(BaseModel):
    """Modèle pour une gare"""
    stop_id: str
    stop_name: str
    stop_lat: float
    stop_lon: float
    location_type: int
    parent_station: Optional[str] = None

    class Config:
        from_attributes = True


class StopPopularity(BaseModel):
    """Modèle pour la popularité d'une gare (vue Gold)"""
    stop_id: str
    stop_name: str
    stop_lat: float
    stop_lon: float
    location_type: int
    num_trips: int
    num_routes: int
    num_agencies: int
    total_stop_times: int

    class Config:
        from_attributes = True


class Trip(BaseModel):
    """Modèle pour un trajet"""
    trip_id: str
    route_id: str
    service_id: int
    trip_headsign: Optional[str] = None
    direction_id: Optional[int] = None
    block_id: Optional[int] = None

    class Config:
        from_attributes = True


class TripEnriched(BaseModel):
    """Modèle pour un trajet enrichi (vue Gold)"""
    trip_id: str
    route_id: str
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    trip_headsign: Optional[str] = None
    direction_id: Optional[int] = None
    agency_name: str
    num_stops: int
    first_stop_sequence: Optional[int] = None
    last_stop_sequence: Optional[int] = None
    first_arrival_time: Optional[str] = None
    last_departure_time: Optional[str] = None
    num_service_dates: int

    class Config:
        from_attributes = True


class StopTime(BaseModel):
    """Modèle pour un horaire"""
    trip_id: str
    arrival_time: str
    departure_time: str
    stop_id: str
    stop_sequence: int
    pickup_type: int
    drop_off_type: int

    class Config:
        from_attributes = True


class StopTimeWithDetails(BaseModel):
    """Modèle pour un horaire avec détails de la gare"""
    trip_id: str
    arrival_time: str
    departure_time: str
    stop_id: str
    stop_name: str
    stop_sequence: int
    pickup_type: int
    drop_off_type: int

    class Config:
        from_attributes = True


# ============================================================================
# Modèles Back-on-Track
# ============================================================================

class NightTrain(BaseModel):
    """Modèle pour un train de nuit"""
    routeid: str
    nighttrain: Optional[str] = None
    itinerary: Optional[str] = None
    routelongname: Optional[str] = None
    itinerarylong: Optional[str] = None
    countries: Optional[str] = None
    operators: Optional[str] = None
    source: Optional[str] = None
    num_stations_direction_1: Optional[int] = None
    num_stations_direction_2: Optional[int] = None
    num_countries: Optional[int] = None
    num_operators: Optional[int] = None

    class Config:
        from_attributes = True


class NightTrainEnriched(BaseModel):
    """Modèle pour un train de nuit enrichi (vue Gold)"""
    routeid: str
    nighttrain: Optional[str] = None
    itinerary: Optional[str] = None
    routelongname: Optional[str] = None
    num_countries: int
    num_operators: int
    num_stations: int
    num_stations_direction_1: Optional[int] = None
    num_stations_direction_2: Optional[int] = None
    countries_list: Optional[str] = None
    operators_list: Optional[str] = None

    class Config:
        from_attributes = True


class NightTrainStation(BaseModel):
    """Modèle pour une gare d'un train de nuit"""
    routeid: str
    direction: int
    sequence: int
    station: str

    class Config:
        from_attributes = True


# ============================================================================
# Modèles de réponse génériques
# ============================================================================

class Message(BaseModel):
    """Modèle pour un message de réponse"""
    message: str


class PaginatedResponse(BaseModel):
    """Modèle pour une réponse paginée"""
    total: int
    page: int
    page_size: int
    total_pages: int
    items: List[dict]

    class Config:
        from_attributes = True

