# Configuration du projet ETL ObRail Europe

import os
import getpass
from pathlib import Path

# =============================================================================
# SOURCES DE DONNÉES GTFS
# =============================================================================

GTFS_SOURCES = {
    # --- France ---
    "sncf": {
        "name": "SNCF TGV, Intercités, TER",
        "url": "https://eu.ftp.opendatasoft.com/sncf/plandata/export-opendata-sncf-gtfs.zip",
        "description": "Horaires des trains SNCF en France (TGV, Intercités, TER)",
        "country": "France",
        "priority": 1
    },
    
    # --- International depuis la France ---
    "eurostar": {
        "name": "Eurostar",
        "url": "https://integration-storage.dm.eurostar.com/gtfs-prod/gtfs_static_commercial_v2.zip",
        "description": "Liaisons Eurostar (France, Belgique, UK, Pays-Bas)",
        "country": "International",
        "priority": 1
    },
    
    "trenitalia": {
        "name": "Trenitalia France",
        "url": "https://thello.axelor.com/public/gtfs/gtfs.zip",
        "description": "Trains Trenitalia en France (ex-Thello)",
        "country": "France/Italie",
        "priority": 2
    },
    
    "renfe_ave": {
        "name": "Renfe AVE International",
        "url": "https://ssl.renfe.com/gtransit/Fichero_AV_INT/Renfe_AVE_Int.zip",
        "description": "Trains AVE Espagne-France",
        "country": "Espagne/France",
        "priority": 2
    },
    
    # --- Europe ---
    "flixbus_europe": {
        "name": "FlixBus & FlixTrain Europe",
        "url": "https://gtfs.gis.flix.tech/gtfs_generic_eu.zip",
        "description": "Réseau européen FlixBus et FlixTrain",
        "country": "Europe",
        "priority": 2
    },
}

# =============================================================================
# AUTRES SOURCES (Non-GTFS)
# =============================================================================

OTHER_SOURCES = {
    "back_on_track": {
        "name": "Back-on-Track Night Trains",
        "url": "https://back-on-track.eu/night-train-database/",
        "description": "Base de données des trains de nuit européens",
        "format": "HTML/JSON",
        "priority": 1
    },
    "transitland": {
        "name": "Transitland API",
        "url": "https://api.transit.land/api/v2/rest/",
        "description": "API mondiale de données GTFS",
        "format": "API REST",
        "priority": 3
    }
}

# =============================================================================
# CHEMINS DES DOSSIERS
# =============================================================================

# Chemin de base du projet ETL
BASE_DIR = Path(__file__).parent.parent

PATHS = {
    "base": BASE_DIR,
    "raw_data": BASE_DIR / "data" / "raw",
    "raw_archives": BASE_DIR / "data" / "raw" / "archives",
    "raw_extracted": BASE_DIR / "data" / "raw" / "extracted",
    "processed_data": BASE_DIR / "data" / "processed",
    "clean_data": BASE_DIR / "data" / "clean",
    "logs": BASE_DIR / "logs",
}

# Paths simplifiés pour l'orchestration
DATA_PATHS = {
    'raw': BASE_DIR / "data" / "raw",
    'clean': BASE_DIR / "data" / "clean",
}

# Créer les dossiers s'ils n'existent pas
for path in PATHS.values():
    if isinstance(path, Path):
        path.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURATION BASE DE DONNÉES
# =============================================================================

# Sur macOS avec Homebrew, l'utilisateur par défaut est le nom d'utilisateur système
DEFAULT_USER = os.getenv('USER', getpass.getuser())

DATABASE = {
    "host": "localhost",
    "port": 5432,
    "name": "obrail_europe",
    "user": DEFAULT_USER,
    "password": ""  # Pas de mot de passe par défaut avec Homebrew
}

# =============================================================================
# CONFIGURATION EXTRACTION
# =============================================================================

EXTRACTION_CONFIG = {
    "timeout": 120,  # Timeout en secondes pour les téléchargements
    "retry_count": 3,  # Nombre de tentatives en cas d'échec
    "retry_delay": 5,  # Délai entre les tentatives (secondes)
    "user_agent": "ObRail-Europe-ETL/1.0 (EPSI MSPR Project)"
}
