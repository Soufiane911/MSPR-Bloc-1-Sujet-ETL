"""
Extracteur pour le catalogue Mobility Database.

Source : https://github.com/MobilityData/mobility-database-catalogs
Format : CSV
Mise à jour : 1-3 fois par mois
Licence : Apache-2.0
"""

import pandas as pd
from typing import Dict, List
from extractors.base_extractor import BaseExtractor


class MobilityCatalogExtractor(BaseExtractor):
    """
    Extracteur pour le catalogue Mobility Database.
    
    Ce catalogue contient plus de 2000 flux GTFS à travers le monde,
    permettant de découvrir et filtrer les sources par pays.
    """
    
    CATALOG_URL = "https://storage.googleapis.com/storage/v1/b/mdb-csv/o/sources.csv?alt=media"
    
    # Pays européens pour filtrage
    EUROPEAN_COUNTRIES = [
        'FR', 'DE', 'IT', 'ES', 'CH', 'AT', 'BE', 'NL', 'UK',
        'SE', 'NO', 'DK', 'FI', 'IE', 'PT', 'PL', 'CZ', 'HU',
        'RO', 'BG', 'HR', 'SI', 'SK', 'LT', 'LV', 'EE', 'LU'
    ]
    
    def __init__(self):
        """Initialise l'extracteur Mobility Catalog."""
        super().__init__(source_name="Mobility-Catalog")
    
    def extract(self) -> pd.DataFrame:
        """
        Télécharge le catalogue complet.
        
        Returns:
            pd.DataFrame: Catalogue complet des flux GTFS
        """
        self.logger.info("=" * 60)
        self.logger.info("Téléchargement du catalogue Mobility Database")
        self.logger.info("=" * 60)
        
        try:
            df = pd.read_csv(self.CATALOG_URL)
            self.data['catalog'] = df
            
            self.logger.info(f"✓ Catalogue téléchargé: {len(df)} flux")
            self.logger.info(f"  - Colonnes: {len(df.columns)}")
            self.logger.info("=" * 60)
            
            return df
            
        except Exception as e:
            self.logger.error(f"✗ Erreur téléchargement catalogue: {str(e)}")
            raise
    
    def get_european_feeds(self) -> pd.DataFrame:
        """
        Filtre le catalogue pour les flux européens.
        
        Returns:
            pd.DataFrame: Flux GTFS européens
        """
        if 'catalog' not in self.data:
            self.extract()
        
        df = self.data['catalog']
        
        # Filtrer pour les flux européens GTFS
        europe_feeds = df[
            (df['location.country_code'].isin(self.EUROPEAN_COUNTRIES)) &
            (df['data_type'] == 'gtfs')
        ].copy()
        
        self.logger.info(f"Flux européens trouvés: {len(europe_feeds)}")
        
        return europe_feeds
    
    def get_feeds_by_country(self, country_code: str) -> pd.DataFrame:
        """
        Récupère les flux pour un pays spécifique.
        
        Args:
            country_code: Code pays ISO 3166-1 alpha-2
            
        Returns:
            pd.DataFrame: Flux pour le pays spécifié
        """
        if 'catalog' not in self.data:
            self.extract()
        
        df = self.data['catalog']
        
        country_feeds = df[
            (df['location.country_code'] == country_code) &
            (df['data_type'] == 'gtfs')
        ].copy()
        
        self.logger.info(f"Flux pour {country_code}: {len(country_feeds)}")
        
        return country_feeds
    
    def get_feeds_by_operator(self, operator_name: str) -> pd.DataFrame:
        """
        Recherche les flux par nom d'opérateur.
        
        Args:
            operator_name: Nom (partiel) de l'opérateur
            
        Returns:
            pd.DataFrame: Flux correspondants
        """
        if 'catalog' not in self.data:
            self.extract()
        
        df = self.data['catalog']
        
        operator_feeds = df[
            df['provider'].str.contains(operator_name, case=False, na=False)
        ].copy()
        
        self.logger.info(f"Flux pour opérateur '{operator_name}': {len(operator_feeds)}")
        
        return operator_feeds
    
    def validate(self) -> bool:
        """
        Valide le catalogue.
        
        Returns:
            bool: True si le catalogue est valide
        """
        if 'catalog' not in self.data:
            self.logger.error("✗ Catalogue non chargé")
            return False
        
        df = self.data['catalog']
        
        # Vérification des colonnes requises
        required_columns = ['mdb_source_id', 'data_type', 'provider', 'urls.direct_download']
        
        for col in required_columns:
            if col not in df.columns:
                self.logger.error(f"✗ Colonne requise manquante: {col}")
                return False
        
        self.logger.info("✓ Validation catalogue réussie")
        return True
    
    def get_summary(self) -> Dict[str, int]:
        """
        Retourne un résumé du catalogue.
        
        Returns:
            Dict[str, int]: Statistiques du catalogue
        """
        if 'catalog' not in self.data:
            self.extract()
        
        df = self.data['catalog']
        europe = self.get_european_feeds()
        
        return {
            'total_feeds': len(df),
            'european_feeds': len(europe),
            'countries': df['location.country_code'].nunique(),
            'european_countries': europe['location.country_code'].nunique()
        }
