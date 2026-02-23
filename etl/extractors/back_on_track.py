"""
Extracteur pour la base de données Back-on-Track Night Train.

Source : https://github.com/Back-on-Track-eu/night-train-data
Format : JSON
Mise à jour : Quotidienne
Licence : GPL-3.0
"""

import requests
import pandas as pd
from typing import Dict, List
from extractors.base_extractor import BaseExtractor


class BackOnTrackExtractor(BaseExtractor):
    """
    Extracteur pour la base de données Back-on-Track Night Train.
    
    Cette source fournit des données sur les trains de nuit en Europe,
    parfaitement adaptées pour la comparaison jour/nuit du projet ObRail.
    """
    
    BASE_URL = "https://raw.githubusercontent.com/Back-on-Track-eu/night-train-data/main/data/latest"
    
    ENDPOINTS = [
        'agencies',        # Opérateurs ferroviaires
        'stops',           # Gares et arrêts
        'routes',          # Lignes ferroviaires
        'trips',           # Voyages
        'trip_stop',       # Arrêts par voyage
        'calendar',        # Calendriers de service
        'calendar_dates',  # Dates spécifiques
        'translations',    # Traductions
        'classes'          # Classes de service
    ]
    
    def __init__(self):
        """Initialise l'extracteur Back-on-Track."""
        super().__init__(source_name="Back-on-Track")
    
    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Extrait tous les endpoints de Back-on-Track.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire des DataFrames extraits
            
        Raises:
            requests.RequestException: Si une erreur de requête survient
        """
        self.logger.info("=" * 60)
        self.logger.info("Démarrage de l'extraction Back-on-Track")
        self.logger.info("=" * 60)
        
        for endpoint in self.ENDPOINTS:
            try:
                url = f"{self.BASE_URL}/{endpoint}.json"
                self.logger.info(f"Téléchargement: {endpoint}.json")
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                df = pd.DataFrame.from_dict(data, orient='index')
                
                # Réinitialiser l'index pour que les clés deviennent des colonnes si nécessaire (ex: stop_id)
                df = df.reset_index(names='id_from_key')
                
                self.data[endpoint] = df
                self.logger.info(f"✓ {endpoint}: {len(df)} enregistrements extraits")
                
            except requests.RequestException as e:
                self.logger.error(f"✗ Erreur extraction {endpoint}: {str(e)}")
                raise
        
        self.logger.info("-" * 60)
        self.logger.info(f"Extraction terminée: {len(self.data)} endpoints")
        self.logger.info("=" * 60)
        
        return self.data
    
    def validate(self) -> bool:
        """
        Valide l'intégrité des données extraites.
        
        Vérifie que les endpoints requis sont présents et non vides,
        et que les relations entre tables sont cohérentes.
        
        Returns:
            bool: True si les données sont valides, False sinon
        """
        self.logger.info("Validation des données Back-on-Track...")
        
        # Vérification des endpoints requis
        required_endpoints = ['agencies', 'routes', 'stops', 'trips']
        
        for endpoint in required_endpoints:
            if endpoint not in self.data:
                self.logger.error(f"✗ Données manquantes pour {endpoint}")
                return False
            if self.data[endpoint].empty:
                self.logger.error(f"✗ Données vides pour {endpoint}")
                return False
        
        # Validation des relations
        try:
            agency_ids = set(self.data['agencies']['agency_id'])
            route_agencies = set(self.data['routes']['agency_id'])
            
            orphaned_routes = route_agencies - agency_ids
            if orphaned_routes:
                self.logger.warning(
                    f"⚠ {len(orphaned_routes)} agency_id des routes "
                    f"ne sont pas dans agencies: {list(orphaned_routes)[:5]}"
                )
            
            # Validation des stops
            stop_ids = set(self.data['stops']['stop_id'])
            trip_stop_ids = set(self.data['trip_stop']['stop_id'])
            
            orphaned_stops = trip_stop_ids - stop_ids
            if orphaned_stops:
                self.logger.warning(
                    f"⚠ {len(orphaned_stops)} stop_id des trip_stop "
                    f"ne sont pas dans stops"
                )
            
        except KeyError as e:
            self.logger.error(f"✗ Erreur de validation: colonne manquante {e}")
            return False
        
        self.logger.info("✓ Validation Back-on-Track réussie")
        return True
    
    def get_summary(self) -> Dict[str, int]:
        """
        Retourne un résumé des données extraites.
        
        Returns:
            Dict[str, int]: Nombre d'enregistrements par endpoint
        """
        return {
            endpoint: len(df) 
            for endpoint, df in self.data.items()
        }
