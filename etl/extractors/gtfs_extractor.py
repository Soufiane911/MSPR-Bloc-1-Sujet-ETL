"""
Extracteur générique pour les flux GTFS.

Gère le téléchargement et l'extraction des fichiers GTFS
pour les opérateurs ferroviaires nationaux.
"""

import os
import zipfile
import io
import requests
import pandas as pd
from typing import Dict, Optional
from pathlib import Path
from extractors.base_extractor import BaseExtractor


class GTFSExtractor(BaseExtractor):
    """
    Extracteur générique pour les flux GTFS.
    
    Gère le téléchargement, l'extraction et la lecture des fichiers GTFS
    au format standard (agency.txt, stops.txt, routes.txt, etc.)
    """
    
    GTFS_FILES = [
        'agency',          # Informations opérateurs
        'stops',           # Arrêts et gares
        'routes',          # Lignes
        'trips',           # Voyages
        'stop_times',      # Horaires aux arrêts
        'calendar',        # Calendriers de service
        'calendar_dates',  # Dates spécifiques
        'shapes',          # Tracés géographiques
        'transfers',       # Correspondances
        'feed_info'        # Métadonnées du flux
    ]
    
    def __init__(self, source_name: str, url: str, country_code: str):
        """
        Initialise l'extracteur GTFS.
        
        Args:
            source_name: Nom de la source (ex: "SNCF", "Deutsche Bahn")
            url: URL de téléchargement du fichier GTFS
            country_code: Code pays ISO 3166-1 alpha-2
        """
        super().__init__(source_name=source_name)
        self.url = url
        self.country_code = country_code
        self.extract_path = Path(f"data/raw/{source_name.lower().replace(' ', '_')}")
    
    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Télécharge et extrait un fichier GTFS.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire des DataFrames GTFS
            
        Raises:
            requests.RequestException: Si le téléchargement échoue
            zipfile.BadZipFile: Si le fichier ZIP est invalide
        """
        self.logger.info("=" * 60)
        self.logger.info(f"Téléchargement GTFS: {self.source_name}")
        self.logger.info(f"URL: {self.url}")
        self.logger.info("=" * 60)
        
        try:
            # Téléchargement
            self.logger.info("Téléchargement en cours...")
            response = requests.get(self.url, timeout=120)
            response.raise_for_status()
            
            self.logger.info(f"✓ Téléchargé: {len(response.content) / 1024 / 1024:.2f} MB")
            
            # Extraction ZIP
            self.extract_path.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                z.extractall(self.extract_path)
                self.logger.info(f"✓ Extraction réussie: {len(z.namelist())} fichiers")
                
                # Log des fichiers extraits
                for name in z.namelist()[:10]:
                    self.logger.info(f"  - {name}")
                if len(z.namelist()) > 10:
                    self.logger.info(f"  ... et {len(z.namelist()) - 10} autres fichiers")
            
            # Lecture des fichiers TXT
            self.logger.info("Lecture des fichiers GTFS...")
            
            for gtfs_file in self.GTFS_FILES:
                file_path = self.extract_path / f"{gtfs_file}.txt"
                
                if file_path.exists():
                    try:
                        df = pd.read_csv(file_path, dtype=str, low_memory=False)
                        self.data[gtfs_file] = df
                        self.logger.info(f"✓ {gtfs_file}.txt: {len(df)} lignes")
                    except Exception as e:
                        self.logger.warning(f"⚠ Erreur lecture {gtfs_file}.txt: {e}")
                else:
                    self.logger.warning(f"⚠ Fichier manquant: {gtfs_file}.txt")
            
            self.logger.info("-" * 60)
            self.logger.info(f"Extraction terminée: {len(self.data)} fichiers GTFS")
            self.logger.info("=" * 60)
            
            return self.data
            
        except requests.RequestException as e:
            self.logger.error(f"✗ Erreur téléchargement GTFS: {str(e)}")
            raise
        except zipfile.BadZipFile as e:
            self.logger.error(f"✗ Fichier ZIP invalide: {str(e)}")
            raise
    
    def validate(self) -> bool:
        """
        Valide la structure GTFS.
        
        Vérifie que les fichiers requis sont présents selon la spécification GTFS.
        
        Returns:
            bool: True si la structure est valide, False sinon
        """
        self.logger.info(f"Validation GTFS: {self.source_name}")
        
        # Fichiers requis selon la spec GTFS
        required_files = ['agency', 'stops', 'routes', 'trips', 'stop_times']
        
        missing_files = []
        for req_file in required_files:
            if req_file not in self.data:
                missing_files.append(req_file)
                self.logger.error(f"✗ Fichier GTFS requis manquant: {req_file}.txt")
        
        if missing_files:
            self.logger.error(f"✗ Validation échouée: {len(missing_files)} fichiers manquants")
            return False
        
        # Vérification des données non vides
        empty_files = []
        for req_file in required_files:
            if self.data[req_file].empty:
                empty_files.append(req_file)
                self.logger.error(f"✗ Fichier vide: {req_file}.txt")
        
        if empty_files:
            self.logger.error(f"✗ Validation échouée: {len(empty_files)} fichiers vides")
            return False
        
        self.logger.info("✓ Validation GTFS réussie")
        return True
    
    def get_feed_info(self) -> Optional[Dict[str, str]]:
        """
        Retourne les informations du flux GTFS si disponibles.
        
        Returns:
            Dict[str, str]: Métadonnées du flux ou None
        """
        if 'feed_info' in self.data and not self.data['feed_info'].empty:
            return self.data['feed_info'].iloc[0].to_dict()
        return None
