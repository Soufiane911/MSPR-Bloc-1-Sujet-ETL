"""
Service pour la gestion des opérateurs.
"""

from sqlalchemy import text
from typing import List, Optional, Dict, Any
from app.database import engine


class OperatorService:
    """Service pour les opérations sur les opérateurs."""
    
    def get_operators(
        self,
        country: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Récupère les opérateurs avec filtres.
        
        Args:
            country: Code pays
            name: Nom de l'opérateur
            limit: Limite de résultats
            offset: Décalage
            
        Returns:
            Liste des opérateurs
        """
        query = """
            SELECT 
                o.operator_id,
                o.name,
                o.country,
                o.website,
                COUNT(DISTINCT t.train_id) as train_count
            FROM operators o
            LEFT JOIN trains t ON o.operator_id = t.operator_id
            WHERE 1=1
        """
        params = {}
        
        if country:
            query += " AND o.country = :country"
            params['country'] = country
        
        if name:
            query += " AND o.name ILIKE :name"
            params['name'] = f"%{name}%"
        
        query += " GROUP BY o.operator_id ORDER BY o.name LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            operators = [dict(row._mapping) for row in result]
        
        return operators
    
    def get_operator_by_id(self, operator_id: int) -> Optional[Dict[str, Any]]:
        """
        Récupère un opérateur par son ID.
        
        Args:
            operator_id: ID de l'opérateur
            
        Returns:
            Détails de l'opérateur ou None
        """
        query = """
            SELECT 
                o.operator_id,
                o.name,
                o.country,
                o.website,
                o.source_name,
                o.created_at,
                o.updated_at,
                COUNT(DISTINCT t.train_id) as train_count
            FROM operators o
            LEFT JOIN trains t ON o.operator_id = t.operator_id
            WHERE o.operator_id = :operator_id
            GROUP BY o.operator_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {'operator_id': operator_id})
            row = result.fetchone()
            
            if row:
                return dict(row._mapping)
            return None
    
    def get_trains(
        self,
        operator_id: int,
        train_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Récupère les trains d'un opérateur.
        
        Args:
            operator_id: ID de l'opérateur
            train_type: Type de train
            limit: Limite de résultats
            
        Returns:
            Liste des trains
        """
        query = """
            SELECT 
                train_id,
                train_number,
                train_type,
                category,
                route_name
            FROM trains
            WHERE operator_id = :operator_id
        """
        params = {'operator_id': operator_id}
        
        if train_type:
            query += " AND train_type = :train_type"
            params['train_type'] = train_type
        
        query += " ORDER BY train_number LIMIT :limit"
        params['limit'] = limit
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            trains = [dict(row._mapping) for row in result]
        
        return trains
    
    def count_by_country(self) -> List[Dict[str, Any]]:
        """
        Compte les opérateurs par pays.
        
        Returns:
            Comptage par pays
        """
        query = """
            SELECT 
                country,
                COUNT(*) as count
            FROM operators
            GROUP BY country
            ORDER BY count DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            counts = [dict(row._mapping) for row in result]
        
        return counts
