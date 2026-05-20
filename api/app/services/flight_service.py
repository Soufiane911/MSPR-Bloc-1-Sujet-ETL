"""
Service pour les vols (flights).
"""
from sqlalchemy import text
from typing import List, Optional, Dict, Any
from app.database import engine


class FlightService:
    def get_flights(
        self,
        airline: Optional[str] = None,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT f.flight_id, f.flight_number, f.distance_km, f.aircraft_type,
                a.airline_id, a.name AS airline_name,
                so.airport_id AS origin_id, so.name AS origin_name, so.iata AS origin_iata,
                sd.airport_id AS destination_id, sd.name AS destination_name, sd.iata AS destination_iata
            FROM flights f
            LEFT JOIN airlines a ON f.airline_id = a.airline_id
            LEFT JOIN airports so ON f.origin_airport_id = so.airport_id
            LEFT JOIN airports sd ON f.destination_airport_id = sd.airport_id
            WHERE 1=1
        """
        params = {}
        if airline:
            query += " AND (a.name ILIKE :airline OR a.iata = :airline_exact)"
            params["airline"] = f"%{airline}%"
            params["airline_exact"] = airline
        if origin:
            query += " AND (so.iata = :origin OR so.icao = :origin)"
            params["origin"] = origin
        if destination:
            query += " AND (sd.iata = :destination OR sd.icao = :destination)"
            params["destination"] = destination

        query += " ORDER BY f.flight_id LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = [dict(r._mapping) for r in result]

        return rows

    def get_flight_by_id(self, flight_id: int) -> Optional[Dict[str, Any]]:
        query = """
            SELECT f.flight_id, f.flight_number, f.distance_km, f.aircraft_type, f.source_name, f.created_at, f.updated_at,
                a.airline_id, a.name AS airline_name,
                so.airport_id AS origin_id, so.name AS origin_name, so.iata AS origin_iata,
                sd.airport_id AS destination_id, sd.name AS destination_name, sd.iata AS destination_iata
            FROM flights f
            LEFT JOIN airlines a ON f.airline_id = a.airline_id
            LEFT JOIN airports so ON f.origin_airport_id = so.airport_id
            LEFT JOIN airports sd ON f.destination_airport_id = sd.airport_id
            WHERE f.flight_id = :flight_id
        """
        with engine.connect() as conn:
            row = conn.execute(text(query), {"flight_id": flight_id}).fetchone()
            if row:
                return dict(row._mapping)
            return None
