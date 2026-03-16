"""
Service pour la gestion des gares.
"""

from sqlalchemy import text
from typing import List, Optional, Dict, Any
from app.database import engine


class StationService:
    """Service pour les opérations sur les gares."""
    
    def get_stations(
        self,
        country: Optional[str] = None,
        city: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Récupère les gares avec filtres.
        
        Args:
            country: Code pays
            city: Ville
            name: Nom de la gare
            limit: Limite de résultats
            offset: Décalage
            
        Returns:
            Liste des gares
        """
        query = """
            SELECT 
                station_id,
                name,
                city,
                country,
                latitude,
                longitude,
                uic_code,
                timezone
            FROM stations
            WHERE 1=1
        """
        params = {}
        
        if country:
            query += " AND country = :country"
            params['country'] = country
        
        if city:
            query += " AND city ILIKE :city"
            params['city'] = f"%{city}%"
        
        if name:
            query += " AND name ILIKE :name"
            params['name'] = f"%{name}%"
        
        query += " ORDER BY name LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            stations = [dict(row._mapping) for row in result]
        
        return stations
    
    def get_station_by_id(self, station_id: int) -> Optional[Dict[str, Any]]:
        """
        Récupère une gare par son ID.
        
        Args:
            station_id: ID de la gare
            
        Returns:
            Détails de la gare ou None
        """
        query = """
            SELECT 
                station_id,
                name,
                city,
                country,
                latitude,
                longitude,
                uic_code,
                timezone,
                created_at,
                updated_at
            FROM stations
            WHERE station_id = :station_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {'station_id': station_id})
            row = result.fetchone()
            
            if row:
                return dict(row._mapping)
            return None
    
    def get_departures(
        self,
        station_id: int,
        train_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Récupère les départs d'une gare.
        
        Args:
            station_id: ID de la gare
            train_type: Type de train
            limit: Limite de résultats
            
        Returns:
            Liste des départs
        """
        query = """
            SELECT 
                s.schedule_id,
                t.train_number,
                t.train_type,
                sd.name as destination,
                sd.city as destination_city,
                s.departure_time,
                s.arrival_time,
                s.duration_min
            FROM schedules s
            JOIN trains t ON s.train_id = t.train_id
            JOIN stations sd ON s.destination_id = sd.station_id
            WHERE s.origin_id = :station_id
        """
        params = {'station_id': station_id}
        
        if train_type:
            query += " AND t.train_type = :train_type"
            params['train_type'] = train_type
        
        query += " ORDER BY s.departure_time LIMIT :limit"
        params['limit'] = limit
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            departures = [dict(row._mapping) for row in result]
        
        return departures
    
    def get_arrivals(
        self,
        station_id: int,
        train_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Récupère les arrivées à une gare.
        
        Args:
            station_id: ID de la gare
            train_type: Type de train
            limit: Limite de résultats
            
        Returns:
            Liste des arrivées
        """
        query = """
            SELECT 
                s.schedule_id,
                t.train_number,
                t.train_type,
                so.name as origin,
                so.city as origin_city,
                s.departure_time,
                s.arrival_time,
                s.duration_min
            FROM schedules s
            JOIN trains t ON s.train_id = t.train_id
            JOIN stations so ON s.origin_id = so.station_id
            WHERE s.destination_id = :station_id
        """
        params = {'station_id': station_id}
        
        if train_type:
            query += " AND t.train_type = :train_type"
            params['train_type'] = train_type
        
        query += " ORDER BY s.arrival_time LIMIT :limit"
        params['limit'] = limit
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            arrivals = [dict(row._mapping) for row in result]
        
        return arrivals
    
    def count_by_country(self) -> List[Dict[str, Any]]:
        """
        Compte les gares par pays.
        
        Returns:
            Comptage par pays
        """
        query = """
            SELECT 
                country,
                COUNT(*) as count
            FROM stations
            GROUP BY country
            ORDER BY count DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            counts = [dict(row._mapping) for row in result]
        
        return counts
