#!/usr/bin/env python3
"""
Script d'extraction des données GTFS pour le projet ObRail Europe.

Ce script télécharge automatiquement toutes les sources de données GTFS
définies dans la configuration et les extrait dans le dossier data/raw.

Usage:
    python extract_all.py              # Télécharge toutes les sources
    python extract_all.py --source sncf  # Télécharge une source spécifique
    python extract_all.py --list       # Liste les sources disponibles
"""

import os
import sys
import zipfile
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import requests
from tqdm import tqdm

# Ajouter le chemin parent pour importer la config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import GTFS_SOURCES, PATHS, EXTRACTION_CONFIG

# =============================================================================
# CONFIGURATION DU LOGGING
# =============================================================================

def setup_logging():
    """Configure le système de logging."""
    log_dir = PATHS.get("logs", Path("logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


# =============================================================================
# FONCTIONS D'EXTRACTION
# =============================================================================

def download_file(url: str, dest_path: Path, logger: logging.Logger) -> bool:
    """
    Télécharge un fichier depuis une URL avec barre de progression.
    
    Args:
        url: URL du fichier à télécharger
        dest_path: Chemin de destination
        logger: Logger pour les messages
        
    Returns:
        True si le téléchargement a réussi, False sinon
    """
    config = EXTRACTION_CONFIG
    headers = {"User-Agent": config.get("user_agent", "ObRail-ETL")}
    
    for attempt in range(config.get("retry_count", 3)):
        try:
            logger.info(f"Téléchargement: {url}")
            logger.info(f"Tentative {attempt + 1}/{config.get('retry_count', 3)}")
            
            response = requests.get(
                url, 
                headers=headers,
                stream=True, 
                timeout=config.get("timeout", 120)
            )
            response.raise_for_status()
            
            # Obtenir la taille totale si disponible
            total_size = int(response.headers.get('content-length', 0))
            
            # Téléchargement avec barre de progression
            with open(dest_path, 'wb') as f:
                with tqdm(
                    total=total_size, 
                    unit='B', 
                    unit_scale=True, 
                    desc=dest_path.name,
                    disable=total_size == 0
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            
            logger.info(f"✅ Téléchargé: {dest_path.name} ({total_size / 1024 / 1024:.2f} MB)")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Erreur téléchargement: {e}")
            if attempt < config.get("retry_count", 3) - 1:
                import time
                time.sleep(config.get("retry_delay", 5))
            else:
                logger.error(f"❌ Échec après {config.get('retry_count', 3)} tentatives")
                return False
    
    return False


def extract_zip(zip_path: Path, extract_dir: Path, logger: logging.Logger) -> bool:
    """
    Extrait un fichier ZIP.
    
    Args:
        zip_path: Chemin du fichier ZIP
        extract_dir: Dossier de destination
        logger: Logger pour les messages
        
    Returns:
        True si l'extraction a réussi, False sinon
    """
    try:
        logger.info(f"Extraction: {zip_path.name} -> {extract_dir}")
        
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Lister les fichiers
            file_list = zip_ref.namelist()
            logger.info(f"  Fichiers: {len(file_list)}")
            
            # Extraire avec barre de progression
            for file in tqdm(file_list, desc="Extraction", unit="fichier"):
                zip_ref.extract(file, extract_dir)
        
        logger.info(f"✅ Extrait: {len(file_list)} fichiers dans {extract_dir.name}")
        return True
        
    except zipfile.BadZipFile as e:
        logger.error(f"❌ Fichier ZIP invalide: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Erreur extraction: {e}")
        return False


def download_gtfs_source(source_id: str, source_config: dict, logger: logging.Logger) -> bool:
    """
    Télécharge et extrait une source GTFS.
    
    Args:
        source_id: Identifiant de la source
        source_config: Configuration de la source
        logger: Logger pour les messages
        
    Returns:
        True si tout a réussi, False sinon
    """
    print("\n" + "=" * 60)
    logger.info(f"🚂 Source: {source_config['name']}")
    logger.info(f"   Description: {source_config['description']}")
    logger.info(f"   Pays: {source_config.get('country', 'N/A')}")
    print("=" * 60)
    
    raw_dir = PATHS.get("raw_data", Path("data/raw"))
    archives_dir = PATHS.get("raw_archives", raw_dir / "archives")
    extracted_dir = PATHS.get("raw_extracted", raw_dir / "extracted")

    archives_dir.mkdir(parents=True, exist_ok=True)
    extracted_dir.mkdir(parents=True, exist_ok=True)
    
    # Chemins
    zip_path = archives_dir / f"{source_id}_gtfs.zip"
    extract_dir = extracted_dir / f"{source_id}_gtfs"
    
    # Téléchargement
    if not download_file(source_config['url'], zip_path, logger):
        return False
    
    # Extraction
    if not extract_zip(zip_path, extract_dir, logger):
        return False
    
    # Optionnel: supprimer le ZIP après extraction
    # zip_path.unlink()
    
    return True


def list_sources():
    """Affiche la liste des sources disponibles."""
    print("\n📋 Sources GTFS disponibles:")
    print("-" * 60)
    
    for source_id, config in sorted(GTFS_SOURCES.items(), key=lambda x: x[1].get('priority', 99)):
        priority_emoji = "⭐" * (4 - config.get('priority', 3))
        print(f"  {source_id:20} {priority_emoji}")
        print(f"      📍 {config['name']}")
        print(f"      📝 {config['description']}")
        print()


def download_all_sources(logger: logging.Logger) -> dict:
    """
    Télécharge toutes les sources GTFS.
    
    Returns:
        Dictionnaire avec le statut de chaque source
    """
    results = {}
    
    # Trier par priorité
    sorted_sources = sorted(
        GTFS_SOURCES.items(), 
        key=lambda x: x[1].get('priority', 99)
    )
    
    logger.info(f"\n🚀 Début de l'extraction de {len(sorted_sources)} sources GTFS")
    
    for source_id, config in sorted_sources:
        success = download_gtfs_source(source_id, config, logger)
        results[source_id] = "✅ Succès" if success else "❌ Échec"
    
    return results


def download_single_source(source_id: str, logger: logging.Logger) -> bool:
    """
    Télécharge une source spécifique.
    
    Args:
        source_id: Identifiant de la source
        logger: Logger pour les messages
        
    Returns:
        True si réussi, False sinon
    """
    if source_id not in GTFS_SOURCES:
        logger.error(f"❌ Source inconnue: {source_id}")
        logger.info(f"   Sources disponibles: {', '.join(GTFS_SOURCES.keys())}")
        return False
    
    return download_gtfs_source(source_id, GTFS_SOURCES[source_id], logger)


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

def main():
    """Point d'entrée principal du script."""
    parser = argparse.ArgumentParser(
        description="Extraction des données GTFS pour ObRail Europe",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python extract_all.py              # Télécharge toutes les sources
  python extract_all.py --source sncf   # Télécharge SNCF uniquement
  python extract_all.py --list       # Liste les sources disponibles
        """
    )
    
    parser.add_argument(
        '--source', '-s',
        type=str,
        help="Source spécifique à télécharger (ex: sncf, eurostar)"
    )
    
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help="Liste les sources disponibles"
    )
    
    args = parser.parse_args()
    
    # Liste des sources
    if args.list:
        list_sources()
        return
    
    # Configuration du logging
    logger = setup_logging()
    
    print("\n" + "🚂" * 30)
    print("   ObRail Europe - Extraction des données GTFS")
    print("🚂" * 30 + "\n")
    
    start_time = datetime.now()
    
    # Téléchargement
    if args.source:
        success = download_single_source(args.source, logger)
        results = {args.source: "✅ Succès" if success else "❌ Échec"}
    else:
        results = download_all_sources(logger)
    
    # Résumé
    duration = datetime.now() - start_time
    
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ DE L'EXTRACTION")
    print("=" * 60)
    
    for source_id, status in results.items():
        print(f"  {source_id:20} {status}")
    
    print("-" * 60)
    print(f"  Durée totale: {duration}")
    print(f"  Données dans: {PATHS.get('raw_data', 'data/raw')}")
    print("=" * 60 + "\n")
    
    # Code de sortie
    failed = sum(1 for s in results.values() if "Échec" in s)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
