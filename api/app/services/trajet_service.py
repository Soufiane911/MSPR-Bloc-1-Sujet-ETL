"""
Service pour les trajets ferroviaires exposes au front-end.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.database import engine


class TrajetService:
    """Service de consultation des trajets issus des dessertes."""

    def get_trajets(
        self,
        country: Optional[str] = None,
        train_type: Optional[str] = None,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Recupere les trajets filtrables pour l'interface utilisateur."""
        query = """
            SELECT
                s.schedule_id AS trajet_id,
                t.train_number,
                o.name AS operator_name,
                t.train_type,
                so.name AS origin,
                sd.name AS destination,
                COALESCE(so.country, 'EU') AS origin_country,
                COALESCE(sd.country, 'EU') AS destination_country,
                so.latitude AS origin_latitude,
                so.longitude AS origin_longitude,
                sd.latitude AS destination_latitude,
                sd.longitude AS destination_longitude,
                s.departure_time,
                s.arrival_time,
                s.duration_min,
                s.distance_km
            FROM schedules s
            JOIN trains t ON s.train_id = t.train_id
            JOIN operators o ON t.operator_id = o.operator_id
            JOIN stations so ON s.origin_id = so.station_id
            JOIN stations sd ON s.destination_id = sd.station_id
            WHERE 1=1
        """
        params: Dict[str, Any] = {}

        if country:
            query += """
                AND (
                    so.country = :country
                    OR sd.country = :country
                    OR o.country = :country
                )
            """
            params["country"] = country

        if train_type:
            query += " AND t.train_type = :train_type"
            params["train_type"] = train_type

        if origin:
            query += " AND (so.name ILIKE :origin OR so.city ILIKE :origin)"
            params["origin"] = f"%{origin}%"

        if destination:
            query += " AND (sd.name ILIKE :destination OR sd.city ILIKE :destination)"
            params["destination"] = f"%{destination}%"

        query += " ORDER BY s.departure_time, s.schedule_id LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            return [dict(row._mapping) for row in result]

    def get_trajet_by_id(self, trajet_id: int) -> Optional[Dict[str, Any]]:
        """Recupere un trajet par son identifiant."""
        query = """
            SELECT
                s.schedule_id AS trajet_id,
                t.train_number,
                o.name AS operator_name,
                t.train_type,
                so.name AS origin,
                sd.name AS destination,
                COALESCE(so.country, 'EU') AS origin_country,
                COALESCE(sd.country, 'EU') AS destination_country,
                so.latitude AS origin_latitude,
                so.longitude AS origin_longitude,
                sd.latitude AS destination_latitude,
                sd.longitude AS destination_longitude,
                s.departure_time,
                s.arrival_time,
                s.duration_min,
                s.distance_km
            FROM schedules s
            JOIN trains t ON s.train_id = t.train_id
            JOIN operators o ON t.operator_id = o.operator_id
            JOIN stations so ON s.origin_id = so.station_id
            JOIN stations sd ON s.destination_id = sd.station_id
            WHERE s.schedule_id = :trajet_id
        """

        with engine.connect() as conn:
            result = conn.execute(text(query), {"trajet_id": trajet_id})
            row = result.fetchone()
            return dict(row._mapping) if row else None
