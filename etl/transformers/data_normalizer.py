"""
Module de normalisation des données pour l'ETL ObRail Europe.

Gère la normalisation des codes gares, des fuseaux horaires,
et des formats de date/heure.
"""

import pandas as pd
import pytz
from datetime import datetime, time
from typing import Dict, Optional
from config.logging_config import setup_logging


class DataNormalizer:
    """
    Classe de normalisation des données ferroviaires.
    
    Responsabilités:
    - Normalisation des codes gares (UIC)
    - Conversion des fuseaux horaires (UTC)
    - Normalisation des formats de date/heure
    - Standardisation des unités
    """
    
    # Mapping des codes pays vers fuseaux horaires
    COUNTRY_TIMEZONES = {
        'FR': 'Europe/Paris',
        'DE': 'Europe/Berlin',
        'IT': 'Europe/Rome',
        'ES': 'Europe/Madrid',
        'CH': 'Europe/Zurich',
        'AT': 'Europe/Vienna',
        'BE': 'Europe/Brussels',
        'NL': 'Europe/Amsterdam',
        'UK': 'Europe/London',
    }
    
    def __init__(self):
        """Initialise le normaliseur de données."""
        self.logger = setup_logging("transformer.normalizer")
    
    def normalize_station_codes(self, df: pd.DataFrame, 
                                code_column: str = 'stop_id') -> pd.DataFrame:
        """
        Normalise les codes de gares.
        
        Args:
            df: DataFrame des gares
            code_column: Nom de la colonne de code
            
        Returns:
            pd.DataFrame: Gares avec codes normalisés
        """
        df = df.copy()
        
        if code_column in df.columns:
            # Nettoyage des codes
            df[code_column] = df[code_column].astype(str).str.strip().str.upper()
            
            self.logger.info(f"✓ Codes normalisés: {df[code_column].nunique()} codes uniques")
        
        return df
    
    def normalize_time(self, time_str: str) -> Optional[time]:
        """
        Normalise une chaîne de temps au format GTFS (HH:MM:SS ou H:MM:SS).
        
        Args:
            time_str: Chaîne de temps
            
        Returns:
            Optional[time]: Objet time ou None
        """
        if pd.isna(time_str):
            return None
        
        try:
            # Format GTFS: HH:MM:SS ou H:MM:SS
            parts = str(time_str).strip().split(':')
            
            if len(parts) >= 2:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2]) if len(parts) > 2 else 0
                
                # GTFS permet les heures > 24 (ex: 25:30:00 pour 01:30 du lendemain)
                hours = hours % 24
                
                return time(hours, minutes, seconds)
            
        except (ValueError, IndexError) as e:
            self.logger.warning(f"⚠ Format de temps invalide: {time_str} - {e}")
        
        return None
    
    def normalize_stop_times(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalise les colonnes d'heure d'un DataFrame stop_times.
        
        Args:
            df: DataFrame des horaires
            
        Returns:
            pd.DataFrame: Horaires normalisés
        """
        df = df.copy()
        
        # Normalisation des colonnes d'heure
        for col in ['arrival_time', 'departure_time']:
            if col in df.columns:
                self.logger.info(f"Normalisation de {col}...")
                
                # Conversion en timedelta pour gérer les heures > 24
                df[col] = pd.to_timedelta(df[col], errors='coerce')
                
                # Compter les erreurs
                errors = df[col].isna().sum()
                if errors > 0:
                    self.logger.warning(f"⚠ {errors} valeurs de temps invalides dans {col}")
        
        return df
    
    def add_country_code(self, df: pd.DataFrame, 
                         source_country: str,
                         agency_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Ajoute le code pays aux données.
        
        Args:
            df: DataFrame à enrichir
            source_country: Code pays de la source
            agency_df: DataFrame des agences (pour mapping)
            
        Returns:
            pd.DataFrame: DataFrame avec code pays
        """
        df = df.copy()
        df['country_code'] = source_country
        
        self.logger.info(f"✓ Code pays ajouté: {source_country}")
        
        return df
    
    def normalize_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalise les coordonnées géographiques.
        
        Args:
            df: DataFrame avec coordonnées
            
        Returns:
            pd.DataFrame: Coordonnées normalisées
        """
        df = df.copy()
        
        # Normalisation de la latitude
        if 'stop_lat' in df.columns:
            df['stop_lat'] = pd.to_numeric(df['stop_lat'], errors='coerce')
            df.loc[~df['stop_lat'].between(-90, 90), 'stop_lat'] = None
        
        # Normalisation de la longitude
        if 'stop_lon' in df.columns:
            df['stop_lon'] = pd.to_numeric(df['stop_lon'], errors='coerce')
            df.loc[~df['stop_lon'].between(-180, 180), 'stop_lon'] = None
        
        return df
    
    def standardize_route_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardise les types de lignes selon la spec GTFS.
        
        Args:
            df: DataFrame des lignes
            
        Returns:
            pd.DataFrame: Types standardisés
        """
        # Mapping des types GTFS
        ROUTE_TYPES = {
            0: 'Tram',
            1: 'Subway',
            2: 'Rail',
            3: 'Bus',
            4: 'Ferry',
            5: 'Cable Tram',
            6: 'Aerial Lift',
            7: 'Funicular',
            100: 'Railway Service',
            101: 'High Speed Rail',
            102: 'Long Distance Rail',
            103: 'Inter Regional Rail',
            104: 'Car Transport Rail',
            105: 'Sleeper Rail',
            106: 'Regional Rail',
            107: 'Tourist Railway',
            108: 'Rail Shuttle',
            109: 'Suburban Railway',
            110: 'Replacement Rail',
            111: 'Special Rail',
            112: 'Truck Transport Rail',
            113: 'All Rail Services',
            114: 'Cross-Country Rail',
            115: 'Vehicle Transport Rail',
            116: 'Rack and Pinion Railway',
            117: 'Additional Rail',
        }
        
        df = df.copy()
        
        if 'route_type' in df.columns:
            df['route_type'] = pd.to_numeric(df['route_type'], errors='coerce')
            df['route_type_label'] = df['route_type'].map(ROUTE_TYPES).fillna('Unknown')
        
        return df
