"""
Service pour la gestion des trains.
"""

import pandas as pd
from sqlalchemy import text
from typing import List, Optional, Dict, Any
from app.database import engine


class TrainService:
    """Service pour les opérations sur les trains."""
    
    def get_trains(
        self,
        train_type: Optional[str] = None,
        operator: Optional[str] = None,
        category: Optional[str] = None,
        country: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Récupère les trains avec filtres.
        
        Args:
            train_type: Type de train (day/night)
            operator: Nom de l'opérateur
            category: Catégorie de train
            country: Code pays
            limit: Limite de résultats
            offset: Décalage
            
        Returns:
            Liste des trains
        """
        query = """
            SELECT 
                t.train_id,
                t.train_number,
                o.name as operator_name,
                t.train_type,
                t.category,
                t.route_name
            FROM trains t
            JOIN operators o ON t.operator_id = o.operator_id
            WHERE 1=1
        """
        params = {}
        
        if train_type:
            query += " AND t.train_type = :train_type"
            params['train_type'] = train_type
        
        if operator:
            query += " AND o.name ILIKE :operator"
            params['operator'] = f"%{operator}%"
        
        if category:
            query += " AND t.category ILIKE :category"
            params['category'] = f"%{category}%"
        
        if country:
            query += " AND o.country = :country"
            params['country'] = country
        
        query += " ORDER BY t.train_id LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            trains = [dict(row._mapping) for row in result]
        
        return trains
    
    def get_train_by_id(self, train_id: int) -> Optional[Dict[str, Any]]:
        """
        Récupère un train par son ID.
        
        Args:
            train_id: ID du train
            
        Returns:
            Détails du train ou None
        """
        query = """
            SELECT 
                t.train_id,
                t.train_number,
                t.operator_id,
                o.name as operator_name,
                o.country,
                t.train_type,
                t.category,
                t.route_name,
                t.created_at,
                t.updated_at
            FROM trains t
            JOIN operators o ON t.operator_id = o.operator_id
            WHERE t.train_id = :train_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {'train_id': train_id})
            row = result.fetchone()
            
            if row:
                return dict(row._mapping)
            return None
    
    def get_train_schedules(self, train_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Récupère les dessertes d'un train.
        
        Args:
            train_id: ID du train
            limit: Limite de résultats
            
        Returns:
            Liste des dessertes
        """
        query = """
            SELECT 
                s.schedule_id,
                so.name as origin,
                so.city as origin_city,
                sd.name as destination,
                sd.city as destination_city,
                s.departure_time,
                s.arrival_time,
                s.duration_min,
                s.distance_km
            FROM schedules s
            JOIN stations so ON s.origin_id = so.station_id
            JOIN stations sd ON s.destination_id = sd.station_id
            WHERE s.train_id = :train_id
            ORDER BY s.departure_time
            LIMIT :limit
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {'train_id': train_id, 'limit': limit})
            schedules = [dict(row._mapping) for row in result]
        
        return schedules
    
    def count_by_type(self) -> Dict[str, int]:
        """
        Compte les trains par type.
        
        Returns:
            Comptage par type
        """
        query = """
            SELECT train_type, COUNT(*) as count
            FROM trains
            GROUP BY train_type
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            counts = {row.train_type: row.count for row in result}
        
        return counts
    
    def count_by_country(self) -> List[Dict[str, Any]]:
        """
        Compte les trains par pays.
        
        Returns:
            Comptage par pays
        """
        query = """
            SELECT 
                o.country,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE t.train_type = 'day') as day_count,
                COUNT(*) FILTER (WHERE t.train_type = 'night') as night_count
            FROM trains t
            JOIN operators o ON t.operator_id = o.operator_id
            GROUP BY o.country
            ORDER BY total DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            counts = [dict(row._mapping) for row in result]
        
        return counts
