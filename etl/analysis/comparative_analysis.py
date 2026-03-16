"""Analyses comparatives jour/nuit pour le dashboard."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text


class ComparativeAnalysis:
    """Expose les requetes analytiques utilisees par le dashboard."""

    def __init__(self, connection):
        self.connection = connection

    def _read_sql(self, query: str, params: dict | None = None) -> pd.DataFrame:
        try:
            return pd.read_sql(text(query), self.connection, params=params)
        except Exception:
            return pd.DataFrame()

    def get_basic_counts(self) -> pd.DataFrame:
        return self._read_sql(
            """
            SELECT
                t.train_type,
                COUNT(DISTINCT t.train_id) AS nb_trains,
                COUNT(s.schedule_id) AS nb_schedules,
                ROUND(AVG(s.duration_min), 2) AS avg_duration_min,
                ROUND(AVG(s.distance_km), 2) AS avg_distance_km
            FROM trains t
            LEFT JOIN schedules s ON s.train_id = t.train_id
            GROUP BY t.train_type
            ORDER BY t.train_type
            """
        )

    def compare_by_frequency(self) -> pd.DataFrame:
        return self._read_sql(
            """
            WITH route_services AS (
                SELECT
                    o.country,
                    t.train_type,
                    s.origin_id,
                    s.destination_id,
                    COUNT(*) AS total_services
                FROM schedules s
                JOIN trains t ON t.train_id = s.train_id
                JOIN operators o ON o.operator_id = t.operator_id
                GROUP BY o.country, t.train_type, s.origin_id, s.destination_id
            )
            SELECT
                country,
                train_type,
                COUNT(*) AS nb_unique_routes,
                SUM(total_services) AS total_services,
                ROUND(AVG(total_services), 2) AS avg_services_per_route,
                ROUND(AVG(total_services) / 7.0, 2) AS services_per_day
            FROM route_services
            GROUP BY country, train_type
            ORDER BY country, train_type
            """
        )

    def compare_by_distance_segment(self) -> pd.DataFrame:
        return self._read_sql(
            """
            WITH segmented AS (
                SELECT
                    t.train_type,
                    CASE
                        WHEN s.distance_km IS NULL THEN 'Inconnue'
                        WHEN s.distance_km < 200 THEN '< 200 km'
                        WHEN s.distance_km < 500 THEN '200-499 km'
                        WHEN s.distance_km < 800 THEN '500-799 km'
                        ELSE '800+ km'
                    END AS distance_segment,
                    CASE
                        WHEN s.duration_min > 0 AND s.distance_km IS NOT NULL
                        THEN s.distance_km / (s.duration_min / 60.0)
                    END AS speed_kmh,
                    s.distance_km
                FROM schedules s
                JOIN trains t ON t.train_id = s.train_id
            )
            SELECT
                train_type,
                distance_segment,
                COUNT(*) AS nb_services,
                ROUND(AVG(distance_km), 2) AS avg_distance_km,
                ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh
            FROM segmented
            GROUP BY train_type, distance_segment
            ORDER BY
                CASE distance_segment
                    WHEN '< 200 km' THEN 1
                    WHEN '200-499 km' THEN 2
                    WHEN '500-799 km' THEN 3
                    WHEN '800+ km' THEN 4
                    ELSE 5
                END,
                train_type
            """
        )

    def compare_international_coverage(self) -> pd.DataFrame:
        return self._read_sql(
            """
            SELECT
                t.train_type,
                COUNT(DISTINCT CAST(s.origin_id AS TEXT) || '->' || CAST(s.destination_id AS TEXT)) AS international_routes,
                COUNT(*) AS total_services,
                ROUND(AVG(s.distance_km), 2) AS avg_distance_km
            FROM schedules s
            JOIN trains t ON t.train_id = s.train_id
            JOIN stations so ON so.station_id = s.origin_id
            JOIN stations sd ON sd.station_id = s.destination_id
            WHERE so.country IS NOT NULL
              AND sd.country IS NOT NULL
              AND so.country <> sd.country
            GROUP BY t.train_type
            ORDER BY t.train_type
            """
        )

    def compare_connectivity(self) -> pd.DataFrame:
        return self._read_sql(
            """
            SELECT
                t.train_type,
                COUNT(DISTINCT s.origin_id) AS origin_stations,
                COUNT(DISTINCT s.destination_id) AS destination_stations,
                COUNT(DISTINCT CAST(s.origin_id AS TEXT) || '->' || CAST(s.destination_id AS TEXT)) AS unique_connections
            FROM schedules s
            JOIN trains t ON t.train_id = s.train_id
            GROUP BY t.train_type
            ORDER BY t.train_type
            """
        )

    def get_comprehensive_comparison(self) -> dict[str, pd.DataFrame]:
        return {
            "basic_counts": self.get_basic_counts(),
            "frequency": self.compare_by_frequency(),
            "distance": self.compare_by_distance_segment(),
            "international": self.compare_international_coverage(),
            "connectivity": self.compare_connectivity(),
        }
