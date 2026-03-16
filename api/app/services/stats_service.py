"""
Service pour les statistiques.
"""

from sqlalchemy import text
from typing import List, Optional, Dict, Any
from app.database import engine


class StatsService:
    """Service pour les statistiques."""
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Récupère les statistiques globales.
        
        Returns:
            Statistiques globales
        """
        queries = {
            'operators': "SELECT COUNT(*) FROM operators",
            'stations': "SELECT COUNT(*) FROM stations",
            'trains': "SELECT COUNT(*) FROM trains",
            'schedules': "SELECT COUNT(*) FROM schedules",
            'day_trains': "SELECT COUNT(*) FROM trains WHERE train_type = 'day'",
            'night_trains': "SELECT COUNT(*) FROM trains WHERE train_type = 'night'"
        }
        
        stats = {}
        with engine.connect() as conn:
            for key, query in queries.items():
                result = conn.execute(text(query))
                stats[key] = result.scalar()
        
        return stats
    
    def get_by_country(self) -> List[Dict[str, Any]]:
        """
        Récupère les statistiques par pays.
        
        Returns:
            Statistiques par pays
        """
        query = """
            SELECT 
                o.country,
                COUNT(DISTINCT o.operator_id) as nb_operators,
                COUNT(DISTINCT t.train_id) as nb_trains,
                COUNT(DISTINCT CASE WHEN t.train_type = 'day' THEN t.train_id END) as day_trains,
                COUNT(DISTINCT CASE WHEN t.train_type = 'night' THEN t.train_id END) as night_trains,
                COUNT(DISTINCT s.origin_id) as nb_stations,
                COUNT(s.schedule_id) as nb_schedules,
                ROUND(AVG(s.duration_min), 0) as avg_duration_min,
                ROUND(AVG(s.distance_km), 0) as avg_distance_km
            FROM operators o
            LEFT JOIN trains t ON o.operator_id = t.operator_id
            LEFT JOIN schedules s ON t.train_id = s.train_id
            GROUP BY o.country
            ORDER BY nb_trains DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            stats = [dict(row._mapping) for row in result]
        
        return stats
    
    def get_day_night_comparison(self, country: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Récupère la comparaison jour/nuit.
        
        Args:
            country: Filtre par pays
            
        Returns:
            Comparaison jour/nuit
        """
        query = """
            SELECT 
                o.country,
                t.train_type,
                COUNT(DISTINCT t.train_id) as nb_trains,
                COUNT(s.schedule_id) as nb_schedules,
                ROUND(AVG(s.duration_min), 0) as avg_duration_min,
                ROUND(AVG(s.distance_km), 0) as avg_distance_km,
                MIN(s.duration_min) as min_duration,
                MAX(s.duration_min) as max_duration
            FROM trains t
            JOIN operators o ON t.operator_id = o.operator_id
            LEFT JOIN schedules s ON t.train_id = s.train_id
            WHERE 1=1
        """
        params = {}
        
        if country:
            query += " AND o.country = :country"
            params['country'] = country
        
        query += " GROUP BY o.country, t.train_type ORDER BY o.country, t.train_type"
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            comparison = [dict(row._mapping) for row in result]
        
        return comparison
    
    def get_data_quality(self) -> List[Dict[str, Any]]:
        """
        Récupère les statistiques de qualité des données.
        
        Returns:
            Qualité des données
        """
        query = """
            SELECT * FROM v_data_quality
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            quality = [dict(row._mapping) for row in result]
        
        return quality
    
    def get_top_routes(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Récupère les routes les plus fréquentées.
        
        Args:
            limit: Nombre de routes à retourner
            
        Returns:
            Top routes
        """
        query = """
            SELECT 
                so.name as origin,
                so.city as origin_city,
                sd.name as destination,
                sd.city as destination_city,
                COUNT(*) as frequency,
                ROUND(AVG(s.duration_min), 0) as avg_duration,
                ROUND(AVG(s.distance_km), 0) as avg_distance
            FROM schedules s
            JOIN stations so ON s.origin_id = so.station_id
            JOIN stations sd ON s.destination_id = sd.station_id
            GROUP BY so.name, so.city, sd.name, sd.city
            ORDER BY frequency DESC
            LIMIT :limit
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {'limit': limit})
            routes = [dict(row._mapping) for row in result]
        
        return routes
