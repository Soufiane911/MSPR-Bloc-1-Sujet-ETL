"""
Service pour les compagnies aériennes.
"""
from sqlalchemy import text
from typing import List, Optional, Dict, Any
from app.database import engine


class AirlineService:
    def get_airlines(
        self,
        country: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT airline_id, name, alias, iata, icao, country, created_at, updated_at
            FROM airlines
            WHERE 1=1
        """
        params = {}
        if country:
            query += " AND country = :country"
            params["country"] = country
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

    def get_airline_by_id(self, airline_id: int) -> Optional[Dict[str, Any]]:
        query = """
            SELECT airline_id, name, alias, iata, icao, country, created_at, updated_at
            FROM airlines
            WHERE airline_id = :airline_id
        """
        with engine.connect() as conn:
            row = conn.execute(text(query), {"airline_id": airline_id}).fetchone()
            if row:
                return dict(row._mapping)
            return None

    def count_by_country(self) -> List[Dict[str, Any]]:
        query = """
            SELECT country, COUNT(*) AS count
            FROM airlines
            GROUP BY country
            ORDER BY count DESC
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            counts = [dict(r._mapping) for r in result]
        return counts
