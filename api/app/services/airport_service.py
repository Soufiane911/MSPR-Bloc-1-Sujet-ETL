"""
Service pour les aéroports.
"""
from sqlalchemy import text
from typing import List, Optional, Dict, Any
from app.database import engine


class AirportService:
    def get_airports(
        self,
        country: Optional[str] = None,
        city: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT airport_id, name, city, country, iata, icao, latitude, longitude, timezone
            FROM airports
            WHERE 1=1
        """
        params = {}
        if country:
            query += " AND country = :country"
            params["country"] = country
        if city:
            query += " AND city ILIKE :city"
            params["city"] = f"%{city}%"
        if name:
            query += " AND name ILIKE :name"
            params["name"] = f"%{name}%"

        query += " ORDER BY name LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = [dict(r._mapping) for r in result]

        return rows

    def get_airport_by_id(self, airport_id: int) -> Optional[Dict[str, Any]]:
        query = """
            SELECT airport_id, name, city, country, iata, icao, latitude, longitude, timezone, created_at, updated_at
            FROM airports
            WHERE airport_id = :airport_id
        """
        with engine.connect() as conn:
            row = conn.execute(text(query), {"airport_id": airport_id}).fetchone()
            if row:
                return dict(row._mapping)
            return None

    def count_by_country(self) -> List[Dict[str, Any]]:
        query = """
            SELECT country, COUNT(*) AS count
            FROM airports
            GROUP BY country
            ORDER BY count DESC
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            counts = [dict(r._mapping) for r in result]
        return counts
