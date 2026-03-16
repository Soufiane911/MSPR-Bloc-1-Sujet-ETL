"""
Module de nettoyage des données pour l'ETL ObRail Europe.

Gère la suppression des doublons, la gestion des valeurs manquantes,
et la validation des formats.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from config.logging_config import setup_logging


class DataCleaner:
    """
    Classe de nettoyage des données ferroviaires.
    
    Responsabilités:
    - Suppression des doublons
    - Gestion des valeurs manquantes
    - Validation des formats
    - Détection des incohérences
    """
    
    def __init__(self):
        """Initialise le nettoyeur de données."""
        self.logger = setup_logging("transformer.cleaner")
        self.stats = {
            'duplicates_removed': 0,
            'missing_values': 0,
            'rows_removed': 0
        }
    
    def remove_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Supprime les doublons d'un DataFrame.
        
        Args:
            df: DataFrame à nettoyer
            subset: Colonnes à considérer pour la détection des doublons
            
        Returns:
            pd.DataFrame: DataFrame sans doublons
        """
        initial_count = len(df)
        df_clean = df.drop_duplicates(subset=subset, keep='first')
        removed = initial_count - len(df_clean)
        
        self.stats['duplicates_removed'] += removed
        
        if removed > 0:
            self.logger.info(f"[OK] Doublons supprimes: {removed} lignes")
        
        return df_clean
    
    def handle_missing_values(self, df: pd.DataFrame, 
                              required_columns: List[str],
                              strategy: str = 'drop') -> pd.DataFrame:
        """
        Gère les valeurs manquantes.
        
        Args:
            df: DataFrame à nettoyer
            required_columns: Colonnes obligatoires
            strategy: Stratégie ('drop' ou 'fill')
            
        Returns:
            pd.DataFrame: DataFrame nettoyé
        """
        initial_count = len(df)
        
        # Compter les valeurs manquantes
        missing_counts = df[required_columns].isnull().sum()
        total_missing = missing_counts.sum()
        
        if total_missing > 0:
            self.logger.warning("[WARN] Valeurs manquantes detectees:")
            for col, count in missing_counts.items():
                if count > 0:
                    self.logger.warning(f"  - {col}: {count} manquantes")
            
            self.stats['missing_values'] += total_missing
        
        # Supprimer les lignes avec valeurs manquantes dans les colonnes requises
        if strategy == 'drop':
            df_clean = df.dropna(subset=required_columns)
            removed = initial_count - len(df_clean)
            
            if removed > 0:
                self.logger.info(f"[OK] Lignes supprimees (valeurs manquantes): {removed}")
                self.stats['rows_removed'] += removed
        
        return df_clean
    
    def clean_stations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie les données de gares.
        
        Args:
            df: DataFrame des gares
            
        Returns:
            pd.DataFrame: Gares nettoyées
        """
        self.logger.info("Nettoyage des données de gares...")
        
        # Suppression des doublons sur stop_id ou uic_code
        df = self.remove_duplicates(df, subset=['stop_id'] if 'stop_id' in df.columns else None)
        
        # Nettoyage des noms
        if 'stop_name' in df.columns:
            df['stop_name'] = df['stop_name'].str.strip()
        
        # Validation des coordonnées
        if 'stop_lat' in df.columns and 'stop_lon' in df.columns:
            # Conversion en numérique
            df['stop_lat'] = pd.to_numeric(df['stop_lat'], errors='coerce')
            df['stop_lon'] = pd.to_numeric(df['stop_lon'], errors='coerce')
            
            # Filtrer les coordonnées invalides
            valid_lat = (df['stop_lat'] >= -90) & (df['stop_lat'] <= 90)
            valid_lon = (df['stop_lon'] >= -180) & (df['stop_lon'] <= 180)
            
            invalid_coords = ~(valid_lat & valid_lon)
            if invalid_coords.sum() > 0:
                self.logger.warning(f"[WARN] {invalid_coords.sum()} coordonnees invalides")
                df.loc[invalid_coords, ['stop_lat', 'stop_lon']] = np.nan
        
        self.logger.info(f"[OK] Gares nettoyees: {len(df)} lignes")
        
        return df
    
    def clean_routes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie les données de lignes.
        
        Args:
            df: DataFrame des lignes
            
        Returns:
            pd.DataFrame: Lignes nettoyées
        """
        self.logger.info("Nettoyage des données de lignes...")
        
        # Suppression des doublons
        df = self.remove_duplicates(df, subset=['route_id'] if 'route_id' in df.columns else None)
        
        # Nettoyage des noms
        if 'route_long_name' in df.columns:
            df['route_long_name'] = df['route_long_name'].str.strip()
        
        if 'route_short_name' in df.columns:
            df['route_short_name'] = df['route_short_name'].str.strip()
        
        self.logger.info(f"[OK] Lignes nettoyees: {len(df)} lignes")
        
        return df
    
    def clean_stop_times(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie les données d'horaires.
        
        Args:
            df: DataFrame des horaires
            
        Returns:
            pd.DataFrame: Horaires nettoyés
        """
        self.logger.info("Nettoyage des données d'horaires...")
        
        # Suppression des doublons
        df = self.remove_duplicates(df, subset=['trip_id', 'stop_id', 'stop_sequence'])
        
        # Validation des séquences
        if 'stop_sequence' in df.columns:
            df['stop_sequence'] = pd.to_numeric(df['stop_sequence'], errors='coerce')
        
        self.logger.info(f"[OK] Horaires nettoyes: {len(df)} lignes")
        
        return df
    
    def get_stats(self) -> Dict[str, int]:
        """
        Retourne les statistiques de nettoyage.
        
        Returns:
            Dict[str, int]: Statistiques
        """
        return self.stats.copy()
