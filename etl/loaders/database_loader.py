"""
Module de chargement des données pour l'ETL ObRail Europe.

Gère le chargement des données transformées dans PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Optional
from config.database import engine
from config.logging_config import setup_logging


class DatabaseLoader:
    """
    Classe de chargement des données dans PostgreSQL.
    
    Responsabilités:
    - Chargement des données dans les tables
    - Gestion des contraintes d'intégrité
    - Validation des données chargées
    - Génération des rapports de chargement
    """
    
    def __init__(self):
        """Initialise le loader de base de données."""
        self.logger = setup_logging("loader.database")
        self.engine = engine
        self.stats = {
            'operators_loaded': 0,
            'stations_loaded': 0,
            'trains_loaded': 0,
            'schedules_loaded': 0
        }
    
    def load_operators(self, df: pd.DataFrame) -> int:
        """
        Charge les opérateurs dans la base de données.
        
        Args:
            df: DataFrame des opérateurs
            
        Returns:
            int: Nombre d'opérateurs chargés
        """
        self.logger.info("=" * 60)
        self.logger.info("Chargement des opérateurs...")
        self.logger.info("=" * 60)
        
        if df.empty:
            self.logger.warning("⚠ Aucun opérateur à charger")
            return 0
        
        # Mapping des colonnes
        column_mapping = {
            'agency_name': 'name',
            'agency_url': 'website',
            'source_name': 'source_name'
        }
        
        # Renommage des colonnes
        df_load = df.rename(columns=column_mapping)
        
        # Sélection des colonnes nécessaires
        required_cols = ['name', 'country', 'website', 'source_name']
        available_cols = [c for c in required_cols if c in df_load.columns]
        df_load = df_load[available_cols]
        
        try:
            # Chargement
            df_load.to_sql('operators', self.engine, if_exists='append', index=False)
            
            count = len(df_load)
            self.stats['operators_loaded'] = count
            
            self.logger.info(f"✓ Opérateurs chargés: {count}")
            return count
            
        except Exception as e:
            self.logger.error(f"✗ Erreur chargement opérateurs: {str(e)}")
            raise
    
    def load_stations(self, df: pd.DataFrame) -> int:
        """
        Charge les gares dans la base de données.
        
        Args:
            df: DataFrame des gares
            
        Returns:
            int: Nombre de gares chargées
        """
        self.logger.info("Chargement des gares...")
        
        if df.empty:
            self.logger.warning("⚠ Aucune gare à charger")
            return 0
        
        # Mapping des colonnes
        column_mapping = {
            'stop_name': 'name',
            'stop_lat': 'latitude',
            'stop_lon': 'longitude',
            'stop_id': 'uic_code',
            'source_name': 'source_name'
        }
        
        df_load = df.rename(columns=column_mapping)
        
        # Sélection des colonnes
        required_cols = ['name', 'city', 'country', 'latitude', 'longitude', 'uic_code', 'timezone', 'source_name']
        available_cols = [c for c in required_cols if c in df_load.columns]
        df_load = df_load[available_cols]
        
        try:
            df_load.to_sql('stations', self.engine, if_exists='append', index=False)
            
            count = len(df_load)
            self.stats['stations_loaded'] = count
            
            self.logger.info(f"✓ Gares chargées: {count}")
            return count
            
        except Exception as e:
            self.logger.error(f"✗ Erreur chargement gares: {str(e)}")
            raise
    
    def load_trains(self, df: pd.DataFrame) -> int:
        """
        Charge les trains dans la base de données.
        
        Args:
            df: DataFrame des trains
            
        Returns:
            int: Nombre de trains chargés
        """
        self.logger.info("Chargement des trains...")
        
        if df.empty:
            self.logger.warning("⚠ Aucun train à charger")
            return 0
        
        # Mapping des colonnes
        column_mapping = {
            'trip_id': 'train_number',
            'route_short_name': 'category',
            'route_long_name': 'route_name',
            'train_type': 'train_type',
            'source_name': 'source_name'
        }
        
        df_load = df.rename(columns=column_mapping)
        
        # Sélection des colonnes
        required_cols = ['train_number', 'operator_id', 'train_type', 'category', 'route_name', 'source_name']
        available_cols = [c for c in required_cols if c in df_load.columns]
        df_load = df_load[available_cols]
        
        try:
            df_load.to_sql('trains', self.engine, if_exists='append', index=False)
            
            count = len(df_load)
            self.stats['trains_loaded'] = count
            
            self.logger.info(f"✓ Trains chargés: {count}")
            return count
            
        except Exception as e:
            self.logger.error(f"✗ Erreur chargement trains: {str(e)}")
            raise
    
    def load_schedules(self, df: pd.DataFrame) -> int:
        """
        Charge les dessertes dans la base de données.
        
        Args:
            df: DataFrame des dessertes
            
        Returns:
            int: Nombre de dessertes chargées
        """
        self.logger.info("Chargement des dessertes...")
        
        if df.empty:
            self.logger.warning("⚠ Aucune desserte à charger")
            return 0
        
        # Les colonnes sont déjà mappées par le DataMerger
        df_load = df.copy()
        
        # Sélection des colonnes
        required_cols = ['train_id', 'origin_id', 'destination_id', 'departure_time', 
                        'arrival_time', 'duration_min', 'distance_km', 'frequency', 'source_name']
        available_cols = [c for c in required_cols if c in df_load.columns]
        df_load = df_load[available_cols]
        
        try:
            df_load.to_sql('schedules', self.engine, if_exists='append', index=False)
            
            count = len(df_load)
            self.stats['schedules_loaded'] = count
            
            self.logger.info(f"✓ Dessertes chargées: {count}")
            return count
            
        except Exception as e:
            self.logger.error(f"✗ Erreur chargement dessertes: {str(e)}")
            raise
    
    def get_stats(self) -> Dict[str, int]:
        """
        Retourne les statistiques de chargement.
        
        Returns:
            Dict[str, int]: Statistiques
        """
        return self.stats.copy()
    
    def verify_counts(self) -> Dict[str, int]:
        """
        Vérifie les comptes dans la base de données.
        
        Returns:
            Dict[str, int]: Comptes par table
        """
        with self.engine.connect() as conn:
            counts = {}
            
            for table in ['operators', 'stations', 'trains', 'schedules']:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                counts[table] = result.scalar()
            
            return counts
