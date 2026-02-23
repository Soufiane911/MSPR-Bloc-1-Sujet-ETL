"""
Module de fusion des données pour l'ETL ObRail Europe.

Fusionne les données de différentes sources en un jeu de données unifié.
"""

import pandas as pd
from typing import Dict, List
from config.logging_config import setup_logging


class DataMerger:
    """
    Classe de fusion des données de multiples sources.
    
    Responsabilités:
    - Fusion des données de différents opérateurs
    - Harmonisation des schémas
    - Gestion des conflits d'identifiants
    - Création des relations entre entités
    """
    
    def __init__(self):
        """Initialise le fusionneur de données."""
        self.logger = setup_logging("transformer.merger")
        self.sources_data = {}
    
    def add_source(self, source_name: str, data: Dict[str, pd.DataFrame]):
        """
        Ajoute une source de données.
        
        Args:
            source_name: Nom de la source
            data: Dictionnaire des DataFrames de la source
        """
        self.sources_data[source_name] = data
        self.logger.info(f"Source ajoutée: {source_name}")
    
    def merge_operators(self) -> pd.DataFrame:
        """
        Fusionne les opérateurs de toutes les sources.
        
        Returns:
            pd.DataFrame: Opérateurs fusionnés
        """
        self.logger.info("Fusion des opérateurs...")
        
        all_operators = []
        
        for source_name, data in self.sources_data.items():
            if 'agency' in data or 'agencies' in data:
                # GTFS: agency.txt, Back-on-Track: agencies.json
                agency_key = 'agency' if 'agency' in data else 'agencies'
                df = data[agency_key].copy()
                
                # Normalisation des colonnes
                if 'agency_id' in df.columns:
                    df['source_agency_id'] = df['agency_id'].astype(str)
                    df['agency_id'] = f"{source_name}_" + df['agency_id'].astype(str)
                
                df['source_name'] = source_name
                all_operators.append(df)
        
        if all_operators:
            merged = pd.concat(all_operators, ignore_index=True)
            
            # Suppression des doublons par nom
            merged = merged.drop_duplicates(subset=['agency_name'], keep='first')
            
            # Réindexation
            merged['operator_id'] = range(1, len(merged) + 1)
            
            self.logger.info(f"✓ Opérateurs fusionnés: {len(merged)} opérateurs uniques")
            return merged
        
        return pd.DataFrame()
    
    def merge_stations(self) -> pd.DataFrame:
        """
        Fusionne les gares de toutes les sources.
        
        Returns:
            pd.DataFrame: Gares fusionnées
        """
        self.logger.info("Fusion des gares...")
        
        all_stations = []
        
        for source_name, data in self.sources_data.items():
            if 'stops' in data:
                df = data['stops'].copy()
                
                if 'stop_id' in df.columns:
                    df['source_stop_id'] = df['stop_id'].astype(str)
                    df['stop_id'] = f"{source_name}_" + df['stop_id'].astype(str)
                
                # Gestion du pays (obligatoire en BDD)
                if 'country' not in df.columns:
                    if 'sncf' in source_name:
                        df['country'] = 'FR'
                    else:
                        df['country'] = 'EU'  # Valeur par défaut pour l'Europe
                
                df['source_name'] = source_name
                all_stations.append(df)
        
        if all_stations:
            merged = pd.concat(all_stations, ignore_index=True)
            
            # Suppression des doublons par stop_id et nom
            if 'stop_id' in merged.columns:
                merged = merged.drop_duplicates(subset=['stop_id'], keep='first')
            if 'stop_name' in merged.columns:
                merged = merged.drop_duplicates(subset=['stop_name'], keep='first')
            
            # Réindexation
            merged['station_id'] = range(1, len(merged) + 1)
            
            self.logger.info(f"✓ Gares fusionnées: {len(merged)} gares uniques")
            return merged
        
        return pd.DataFrame()
    
    def merge_trains(self, operators: pd.DataFrame) -> pd.DataFrame:
        """
        Fusionne les trains de toutes les sources.
        
        Args:
            operators: DataFrame des opérateurs fusionnés
            
        Returns:
            pd.DataFrame: Trains fusionnés
        """
        self.logger.info("Fusion des trains...")
        
        all_trains = []
        
        for source_name, data in self.sources_data.items():
            if 'routes' in data and 'trips' in data:
                # Fusion routes + trips pour GTFS
                routes = data['routes'].copy()
                trips = data['trips'].copy()
                
                # Merge routes et trips
                df = trips.merge(routes, on='route_id', how='left')
                
                # Normalisation
                if 'trip_id' in df.columns:
                    df['source_trip_id'] = df['trip_id'].astype(str)
                    df['trip_id'] = f"{source_name}_" + df['trip_id'].astype(str)
                
                if 'route_id' in df.columns:
                    df['source_route_id'] = df['route_id'].astype(str)
                    df['route_id'] = f"{source_name}_" + df['route_id'].astype(str)
                
                if 'agency_id' in df.columns:
                    df['source_agency_id'] = df['agency_id'].astype(str)
                    df['agency_id'] = f"{source_name}_" + df['agency_id'].astype(str)
                
                df['source_name'] = source_name
                all_trains.append(df)
            
            elif 'routes' in data and 'trips' not in data:
                # Back-on-Track: routes contient déjà les informations
                df = data['routes'].copy()
                
                if 'route_id' in df.columns:
                    df['source_route_id'] = df['route_id'].astype(str)
                    df['route_id'] = f"{source_name}_" + df['route_id'].astype(str)
                    # Mapper route_id sur trip_id pour DatabaseLoader
                    df['trip_id'] = df['route_id']
                
                if 'agency_id' in df.columns:
                    df['source_agency_id'] = df['agency_id'].astype(str)
                    df['agency_id'] = f"{source_name}_" + df['agency_id'].astype(str)
                
                df['source_name'] = source_name
                df['train_type'] = 'night'
                all_trains.append(df)
        
        if all_trains:
            merged = pd.concat(all_trains, ignore_index=True)
            self.logger.info(f"Types de trains avant mapping: {merged['train_type'].value_counts().to_dict()}")
            
            # Mapping des opérateurs
            if operators is not None and not operators.empty:
                op_map = operators.set_index('agency_id')['operator_id'].to_dict()
                merged['operator_id'] = merged['agency_id'].map(op_map)
                self.logger.info(f"Types de trains après mapping: {merged.groupby('train_type')['operator_id'].count().to_dict()}")
                
                # Fallback pour SNCF Transilien si non mappé
                mask = (merged['operator_id'].isna()) & (merged['source_name'] == 'sncf_transilien')
                if mask.any():
                    # On suppose que le premier opérateur est SNCF (ID 1)
                    merged.loc[mask, 'operator_id'] = 1
                
                # Gestion du train_type (obligatoire en BDD)
                if 'train_type' not in merged.columns or merged['train_type'].isna().any():
                    if 'train_type' not in merged.columns:
                        merged['train_type'] = 'day'
                    else:
                        merged['train_type'] = merged['train_type'].fillna('day')
                
                # Suppression des lignes sans opérateur (obligatoire en BDD)
                merged = merged.dropna(subset=['operator_id'])
                merged['operator_id'] = merged['operator_id'].astype(int)
            
            # Suppression des doublons
            subset = []
            if 'trip_id' in merged.columns: subset.append('trip_id')
            elif 'route_id' in merged.columns: subset.append('route_id')
            
            if subset:
                merged = merged.drop_duplicates(subset=subset, keep='first')
            
            # Réindexation
            merged['train_id'] = range(1, len(merged) + 1)
            
            self.logger.info(f"✓ Trains fusionnés: {len(merged)} trains uniques")
            return merged
        
        return pd.DataFrame()
    
    def merge_schedules(self, trains: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
        """
        Fusionne les horaires de toutes les sources.
        """
        self.logger.info("Fusion des horaires...")
        all_schedules = []
        
        for source_name, data in self.sources_data.items():
            if 'stop_times' in data:
                df = data['stop_times'].copy()
                df['source_name'] = source_name
                all_schedules.append(df)
            elif 'trip_stop' in data:
                df = data['trip_stop'].copy()
                df['source_name'] = source_name
                all_schedules.append(df)
        
        if all_schedules:
            merged = pd.concat(all_schedules, ignore_index=True)
            
            # Mapping des IDs techniques pour les trains
            if trains is not None and not trains.empty:
                train_id_map = trains.set_index('trip_id')['train_id'].to_dict()
                merged['train_id'] = merged['trip_id'].map(train_id_map)
            
            # Pour GTFS, on doit agréger pour avoir origine/destination par trajet
            if 'stop_sequence' in merged.columns:
                # Calcul des points de départ et d'arrivée
                merged = merged.sort_values(['trip_id', 'stop_sequence'])
                
                # Groupby par trip_id pour extraire origin/destination
                agg_df = merged.groupby('trip_id').agg({
                    'stop_id': ['first', 'last'],
                    'departure_time': 'first',
                    'arrival_time': 'last',
                    'source_name': 'first',
                    'train_id': 'first'
                }).reset_index()
                
                # Renommer les colonnes multi-index
                agg_df.columns = ['trip_id', 'origin_uic', 'destination_uic', 
                                 'departure_time', 'arrival_time', 'source_name', 'train_id']
                merged = agg_df
            
            # Mapping des IDs techniques pour les stations (origine et destination)
            if stations is not None and not stations.empty:
                # Utiliser stop_id comme clé de jointure (c'est le uic_code dans la DF stations)
                station_id_map = stations.set_index('stop_id')['station_id'].to_dict()
                merged['origin_id'] = merged['origin_uic'].map(station_id_map)
                merged['destination_id'] = merged['destination_uic'].map(station_id_map)
            
            # Calcul de la durée si nécessaire
            if 'duration_min' not in merged.columns:
                try:
                    # Conversion simple (HH:MM:SS) - à améliorer si J+1
                    dep = pd.to_timedelta(merged['departure_time'].astype(str))
                    arr = pd.to_timedelta(merged['arrival_time'].astype(str))
                    merged['duration_min'] = (arr - dep).dt.total_seconds() / 60
                except:
                    merged['duration_min'] = 0
            
            # Nettoyage
            merged = merged.dropna(subset=['train_id', 'origin_id', 'destination_id'])
            merged[['train_id', 'origin_id', 'destination_id']] = merged[['train_id', 'origin_id', 'destination_id']].astype(int)
            
            self.logger.info(f"✓ Horaires fusionnés: {len(merged)} dessertes")
            return merged
        
        return pd.DataFrame()

    def get_summary(self) -> Dict[str, int]:
        """
        Retourne un résumé des données fusionnées.
        
        Returns:
            Dict[str, int]: Statistiques
        """
        return {
            'sources': len(self.sources_data),
            'operators': sum(1 for d in self.sources_data.values() if 'agency' in d or 'agencies' in d),
            'stations': sum(1 for d in self.sources_data.values() if 'stops' in d),
            'routes': sum(1 for d in self.sources_data.values() if 'routes' in d)
        }
