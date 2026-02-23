"""
Service pour la gestion des dessertes.
"""

from sqlalchemy import text
from typing import List, Optional, Dict, Any
from datetime import date
from app.database import engine


class ScheduleService:
    """Service pour les opérations sur les dessertes."""
    
    def get_schedules(
        self,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
        train_type: Optional[str] = None,
        country: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Récupère les dessertes avec filtres.
        
        Args:
            origin: Ville de départ
            destination: Ville d'arrivée
            train_type: Type de train
            country: Code pays
            limit: Limite de résultats
            offset: Décalage
            
        Returns:
            Liste des dessertes
        """
        query = """
            SELECT 
                s.schedule_id,
                t.train_number,
                t.train_type,
                so.name as origin,
                so.city as origin_city,
                so.country as origin_country,
                sd.name as destination,
                sd.city as destination_city,
                sd.country as destination_country,
                s.departure_time,
                s.arrival_time,
                s.duration_min,
                s.distance_km
            FROM schedules s
            JOIN trains t ON s.train_id = t.train_id
            JOIN stations so ON s.origin_id = so.station_id
            JOIN stations sd ON s.destination_id = sd.station_id
            WHERE 1=1
        """
        params = {}
        
        if origin:
            query += " AND so.city ILIKE :origin"
            params['origin'] = f"%{origin}%"
        
        if destination:
            query += " AND sd.city ILIKE :destination"
            params['destination'] = f"%{destination}%"
        
        if train_type:
            query += " AND t.train_type = :train_type"
            params['train_type'] = train_type
        
        if country:
            query += " AND (so.country = :country OR sd.country = :country)"
            params['country'] = country
        
        query += " ORDER BY s.departure_time LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            schedules = [dict(row._mapping) for row in result]
        
        return schedules
    
    def search_schedules(
        self,
        from_city: str,
        to_city: str,
        travel_date: Optional[date] = None,
        train_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Recherche de dessertes entre deux villes.
        
        Args:
            from_city: Ville de départ
            to_city: Ville d'arrivée
            travel_date: Date de voyage
            train_type: Type de train
            
        Returns:
            Liste des résultats
        """
        query = """
            SELECT 
                s.schedule_id,
                t.train_number,
                o.name as operator_name,
                t.train_type,
                t.category,
                so.name as origin,
                so.city as origin_city,
                so.country as origin_country,
                sd.name as destination,
                sd.city as destination_city,
                sd.country as destination_country,
                s.departure_time,
                s.arrival_time,
                s.duration_min,
                s.distance_km
            FROM schedules s
            JOIN trains t ON s.train_id = t.train_id
            JOIN operators o ON t.operator_id = o.operator_id
            JOIN stations so ON s.origin_id = so.station_id
            JOIN stations sd ON s.destination_id = sd.station_id
            WHERE so.city ILIKE :from_city
            AND sd.city ILIKE :to_city
        """
        params = {
            'from_city': f"%{from_city}%",
            'to_city': f"%{to_city}%"
        }
        
        if train_type:
            query += " AND t.train_type = :train_type"
            params['train_type'] = train_type
        
        query += " ORDER BY s.duration_min"
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            schedules = [dict(row._mapping) for row in result]
        
        return schedules
    
    def get_schedule_by_id(self, schedule_id: int) -> Optional[Dict[str, Any]]:
        """
        Récupère une desserte par son ID.
        
        Args:
            schedule_id: ID de la desserte
            
        Returns:
            Détails de la desserte ou None
        """
        query = """
            SELECT 
                s.schedule_id,
                t.train_number,
                t.train_type,
                so.name as origin,
                so.city as origin_city,
                sd.name as destination,
                sd.city as destination_city,
                s.departure_time,
                s.arrival_time,
                s.duration_min,
                s.distance_km,
                s.frequency
            FROM schedules s
            JOIN trains t ON s.train_id = t.train_id
            JOIN stations so ON s.origin_id = so.station_id
            JOIN stations sd ON s.destination_id = sd.station_id
            WHERE s.schedule_id = :schedule_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {'schedule_id': schedule_id})
            row = result.fetchone()
            
            if row:
                return dict(row._mapping)
            return None
