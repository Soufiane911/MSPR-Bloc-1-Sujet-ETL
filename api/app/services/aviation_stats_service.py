"""
Service pour les statistiques aériennes.
"""

from typing import Any, Dict, List

from sqlalchemy import text

from app.database import engine


class AviationStatsService:
    """Service pour les statistiques aériennes."""

    def get_summary(self) -> Dict[str, Any]:
        """Retourne les indicateurs globaux du jeu de données aviation."""
        queries = {
            "airlines": "SELECT COUNT(*) FROM airlines",
            "airports": "SELECT COUNT(*) FROM airports",
            "flights": "SELECT COUNT(*) FROM flights",
            "countries": "SELECT COUNT(DISTINCT country) FROM airlines WHERE country IS NOT NULL AND country <> ''",
            "routes": """
                SELECT COUNT(DISTINCT (origin_airport_id, destination_airport_id))
                FROM flights
            """,
        }

        stats: Dict[str, Any] = {}
        with engine.connect() as conn:
            for key, query in queries.items():
                stats[key] = conn.execute(text(query)).scalar()

        return stats

    def get_by_country(self) -> List[Dict[str, Any]]:
        """Retourne des statistiques aériennes par pays de compagnie."""
        query = """
            SELECT
                COALESCE(a.country, 'UNKNOWN') AS country,
                COUNT(DISTINCT a.airline_id) AS nb_airlines,
                COUNT(DISTINCT f.flight_id) AS nb_flights,
                COUNT(DISTINCT (f.origin_airport_id, f.destination_airport_id)) AS nb_routes,
                COUNT(DISTINCT so.airport_id) AS nb_origin_airports,
                COUNT(DISTINCT sd.airport_id) AS nb_destination_airports,
                ROUND(CAST(AVG(f.distance_km) AS NUMERIC), 0) AS avg_distance_km,
                ROUND(CAST(SUM(f.distance_km) AS NUMERIC), 0) AS total_distance_km
            FROM airlines a
            LEFT JOIN flights f ON a.airline_id = f.airline_id
            LEFT JOIN airports so ON f.origin_airport_id = so.airport_id
            LEFT JOIN airports sd ON f.destination_airport_id = sd.airport_id
            GROUP BY COALESCE(a.country, 'UNKNOWN')
            ORDER BY nb_flights DESC, country
        """

        with engine.connect() as conn:
            result = conn.execute(text(query))
            return [dict(row._mapping) for row in result]

    def get_top_routes(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Retourne les routes aériennes les plus utilisées."""
        query = """
            SELECT
                so.airport_id AS origin_id,
                so.name AS origin_name,
                so.city AS origin_city,
                so.country AS origin_country,
                so.iata AS origin_iata,
                sd.airport_id AS destination_id,
                sd.name AS destination_name,
                sd.city AS destination_city,
                sd.country AS destination_country,
                sd.iata AS destination_iata,
                COUNT(*) AS frequency,
                COUNT(DISTINCT f.airline_id) AS nb_airlines,
                ROUND(CAST(AVG(f.distance_km) AS NUMERIC), 0) AS avg_distance_km,
                ROUND(CAST(MIN(f.distance_km) AS NUMERIC), 0) AS min_distance_km,
                ROUND(CAST(MAX(f.distance_km) AS NUMERIC), 0) AS max_distance_km
            FROM flights f
            LEFT JOIN airports so ON f.origin_airport_id = so.airport_id
            LEFT JOIN airports sd ON f.destination_airport_id = sd.airport_id
            GROUP BY
                so.airport_id, so.name, so.city, so.country, so.iata,
                sd.airport_id, sd.name, sd.city, sd.country, sd.iata
            ORDER BY frequency DESC, avg_distance_km DESC
            LIMIT :limit
        """

        with engine.connect() as conn:
            result = conn.execute(text(query), {"limit": limit})
            return [dict(row._mapping) for row in result]

    def get_airport_activity(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Retourne les aéroports les plus actifs en départs et arrivées."""
        query = """
            WITH departures AS (
                SELECT origin_airport_id AS airport_id, COUNT(*) AS departures, AVG(distance_km) AS avg_departure_distance
                FROM flights
                GROUP BY origin_airport_id
            ), arrivals AS (
                SELECT destination_airport_id AS airport_id, COUNT(*) AS arrivals
                FROM flights
                GROUP BY destination_airport_id
            )
            SELECT
                a.airport_id,
                a.name,
                a.city,
                a.country,
                a.iata,
                a.icao,
                COALESCE(d.departures, 0) AS departures,
                COALESCE(ar.arrivals, 0) AS arrivals,
                COALESCE(d.departures, 0) + COALESCE(ar.arrivals, 0) AS movements,
                ROUND(CAST(COALESCE(d.avg_departure_distance, 0) AS NUMERIC), 0) AS avg_departure_distance_km
            FROM airports a
            LEFT JOIN departures d ON a.airport_id = d.airport_id
            LEFT JOIN arrivals ar ON a.airport_id = ar.airport_id
            ORDER BY movements DESC, departures DESC, a.name
            LIMIT :limit
        """

        with engine.connect() as conn:
            result = conn.execute(text(query), {"limit": limit})
            return [dict(row._mapping) for row in result]

    def get_airline_summary(self) -> List[Dict[str, Any]]:
        """Retourne le résumé par compagnie aérienne."""
        query = """
            SELECT
                a.airline_id,
                a.name AS airline_name,
                a.country,
                COUNT(DISTINCT f.flight_id) AS nb_routes,
                COUNT(fi.instance_id) AS nb_instances
            FROM airlines a
            LEFT JOIN flights f ON f.airline_id = a.airline_id
            LEFT JOIN flight_instances fi ON fi.flight_id = f.flight_id
            GROUP BY a.airline_id, a.name, a.country
            ORDER BY nb_routes DESC, airline_name
        """

        with engine.connect() as conn:
            result = conn.execute(text(query))
            return [dict(row._mapping) for row in result]

    def get_data_quality(self) -> List[Dict[str, Any]]:
        """Retourne un aperçu simple de la qualité des données aviation."""
        query = """
            SELECT
                'airlines' AS dataset,
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE country IS NULL OR country = '') AS missing_country,
                COUNT(*) FILTER (WHERE iata IS NULL OR iata = '') AS missing_iata,
                COUNT(*) FILTER (WHERE icao IS NULL OR icao = '') AS missing_icao,
                0 AS missing_links
            FROM airlines
            UNION ALL
            SELECT
                'airports' AS dataset,
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE country IS NULL OR country = '') AS missing_country,
                COUNT(*) FILTER (WHERE iata IS NULL OR iata = '') AS missing_iata,
                COUNT(*) FILTER (WHERE icao IS NULL OR icao = '') AS missing_icao,
                0 AS missing_links
            FROM airports
            UNION ALL
            SELECT
                'flights' AS dataset,
                COUNT(*) AS total_rows,
                0 AS missing_country,
                COUNT(*) FILTER (WHERE airline_id IS NULL) AS missing_iata,
                COUNT(*) FILTER (WHERE origin_airport_id IS NULL OR destination_airport_id IS NULL) AS missing_icao,
                COUNT(*) FILTER (WHERE airline_id IS NULL OR origin_airport_id IS NULL OR destination_airport_id IS NULL) AS missing_links
            FROM flights
        """

        with engine.connect() as conn:
            result = conn.execute(text(query))
            return [dict(row._mapping) for row in result]