#!/usr/bin/env python3
"""
Script principal ETL pour ObRail Europe.

Ce script orchestre l'ensemble du processus ETL:
1. Extraction des données des sources
2. Transformation et nettoyage
3. Classification jour/nuit
4. Chargement dans PostgreSQL

Usage:
    python main.py [--source SOURCE] [--full]

Options:
    --source SOURCE  Exécuter uniquement pour une source spécifique
    --full           Exécution complète avec toutes les sources
"""

import argparse
import sys
from pathlib import Path
import pandas as pd

# Ajout du répertoire parent au path
sys.path.insert(0, str(Path(__file__).parent))

from config.logging_config import setup_logging
from config.sources import SOURCES
from extractors.back_on_track import BackOnTrackExtractor
from extractors.gtfs_extractor import GTFSExtractor
from extractors.mobility_catalog import MobilityCatalogExtractor
from transformers.data_cleaner import DataCleaner
from transformers.data_normalizer import DataNormalizer
from transformers.day_night_classifier import DayNightClassifier
from transformers.data_merger import DataMerger
from loaders.database_loader import DatabaseLoader


def run_extraction(logger, source_filter: str = None) -> dict:
    """
    Exécute la phase d'extraction.
    
    Args:
        logger: Logger
        source_filter: Filtre sur une source spécifique
        
    Returns:
        dict: Données extraites par source
    """
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 1: EXTRACTION DES DONNÉES")
    logger.info("=" * 70 + "\n")
    
    extracted_data = {}
    
    # 1. Back-on-Track (trains de nuit)
    if source_filter is None or source_filter == 'back_on_track':
        logger.info("📥 Extraction Back-on-Track...")
        try:
            extractor = BackOnTrackExtractor()
            data = extractor.extract()
            
            if extractor.validate():
                extracted_data['back_on_track'] = {
                    'data': data,
                    'country': 'EU'
                }
                logger.info("✅ Back-on-Track: Extraction réussie\n")
            else:
                logger.error("❌ Back-on-Track: Validation échouée\n")
        except Exception as e:
            logger.error(f"❌ Back-on-Track: {str(e)}\n")
    
    # 2. Mobility Catalog (référence)
    if source_filter is None or source_filter == 'mobility_catalog':
        logger.info("📥 Extraction Mobility Catalog...")
        try:
            extractor = MobilityCatalogExtractor()
            data = extractor.extract()
            
            if extractor.validate():
                extracted_data['mobility_catalog'] = {'catalog': data}
                european = extractor.get_european_feeds()
                logger.info(f"✅ Mobility Catalog: {len(european)} flux européens\n")
            else:
                logger.error("❌ Mobility Catalog: Validation échouée\n")
        except Exception as e:
            logger.error(f"❌ Mobility Catalog: {str(e)}\n")
    
    # 3. Sources GTFS nationales
    gtfs_sources = [
        ('sncf_transilien', 'SNCF Transilien', 'FR'),
        ('sncf_intercites', 'SNCF Intercités', 'FR'),
        ('gtfs_de', 'GTFS Allemagne', 'DE'),
        ('oebb', 'ÖBB Autriche', 'AT'),
        ('renfe', 'Renfe Espagne', 'ES'),
        ('trenitalia', 'Trenitalia Italie', 'IT'),
    ]
    
    for source_key, source_name, country in gtfs_sources:
        if source_filter is None or source_filter == source_key:
            if source_key in SOURCES:
                logger.info(f"📥 Extraction {source_name}...")
                try:
                    extractor = GTFSExtractor(
                        source_name=source_name,
                        url=SOURCES[source_key]['url'],
                        country_code=country
                    )
                    data = extractor.extract()
                    
                    if extractor.validate():
                        extracted_data[source_key] = {
                            'gtfs': data,
                            'country': country
                        }
                        logger.info(f"✅ {source_name}: Extraction réussie\n")
                    else:
                        logger.warning(f"⚠️ {source_name}: Validation partielle\n")
                except Exception as e:
                    logger.error(f"❌ {source_name}: {str(e)}\n")
    
    logger.info(f"📊 Total sources extraites: {len(extracted_data)}")
    return extracted_data


def run_transformation(logger, extracted_data: dict) -> dict:
    """
    Exécute la phase de transformation.
    
    Args:
        logger: Logger
        extracted_data: Données extraites
        
    Returns:
        dict: Données transformées
    """
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 2: TRANSFORMATION DES DONNÉES")
    logger.info("=" * 70 + "\n")
    
    transformed_data = {}
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    classifier = DayNightClassifier()
    merger = DataMerger()
    
    # Traitement de chaque source
    for source_name, data in extracted_data.items():
        logger.info(f"🔧 Transformation de {source_name}...")
        
        if source_name == 'back_on_track':
            # Traitement Back-on-Track
            transformed = process_back_on_track(data['data'], cleaner, normalizer, classifier)
            transformed_data[source_name] = transformed
            
        elif source_name != 'mobility_catalog':
            # Traitement GTFS
            transformed = process_gtfs_source(source_name, data['gtfs'], data['country'], cleaner, normalizer, classifier)
            transformed_data[source_name] = transformed
    
    # Fusion des données
    logger.info("\n🔗 Fusion des données...")
    for source_name, data in transformed_data.items():
        merger.add_source(source_name, data)
    
    operators = merger.merge_operators()
    stations = merger.merge_stations()
    trains = merger.merge_trains(operators)
    schedules = merger.merge_schedules(trains, stations)
    
    merged = {
        'operators': operators,
        'stations': stations,
        'trains': trains,
        'schedules': schedules
    }
    
    logger.info("✅ Transformation terminée\n")
    return merged


def process_back_on_track(data: dict, cleaner, normalizer, classifier) -> dict:
    """Traite les données Back-on-Track."""
    result = {}
    
    # Nettoyage des agences
    if 'agencies' in data:
        result['agencies'] = cleaner.clean_routes(data['agencies'])
        result['agencies']['country'] = 'EU'
    
    # Nettoyage des gares
    if 'stops' in data:
        result['stops'] = cleaner.clean_stations(data['stops'])
        result['stops'] = normalizer.normalize_coordinates(result['stops'])
    
    # Traitement des routes (trains de nuit)
    if 'routes' in data:
        result['routes'] = data['routes'].copy()
        result['routes']['train_type'] = 'night'  # Back-on-Track = trains de nuit
    
    return result


def process_gtfs_source(source_name: str, data: dict, country: str, cleaner, normalizer, classifier) -> dict:
    """Traite une source GTFS."""
    result = {}
    
    # Nettoyage agency
    if 'agency' in data:
        result['agency'] = data['agency'].copy()
        result['agency']['country'] = country
    
    # Nettoyage stops
    if 'stops' in data:
        result['stops'] = cleaner.clean_stations(data['stops'])
        result['stops'] = normalizer.normalize_coordinates(result['stops'])
    
    # Normalisation stop_times
    if 'stop_times' in data:
        result['stop_times'] = normalizer.normalize_stop_times(data['stop_times'])
        
        # Classification jour/nuit
        if not result['stop_times'].empty:
            result['stop_times'] = classifier.classify_from_timedelta(
                result['stop_times'],
                timedelta_column='departure_time'
            )
    
    # Routes
    if 'routes' in data:
        result['routes'] = normalizer.standardize_route_types(data['routes'])
    
    # Trips
    if 'trips' in data:
        result['trips'] = data['trips'].copy()
    
    return result


def run_loading(logger, transformed_data: dict) -> dict:
    """
    Exécute la phase de chargement.
    
    Args:
        logger: Logger
        transformed_data: Données transformées
        
    Returns:
        dict: Statistiques de chargement
    """
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 3: CHARGEMENT DANS POSTGRESQL")
    logger.info("=" * 70 + "\n")
    
    loader = DatabaseLoader()
    
    # Chargement des opérateurs
    if 'operators' in transformed_data and not transformed_data['operators'].empty:
        loader.load_operators(transformed_data['operators'])
    
    # Chargement des gares
    if 'stations' in transformed_data and not transformed_data['stations'].empty:
        loader.load_stations(transformed_data['stations'])
    
    # Chargement des trains
    if 'trains' in transformed_data and not transformed_data['trains'].empty:
        loader.load_trains(transformed_data['trains'])
    
    # Chargement des dessertes
    if 'schedules' in transformed_data and not transformed_data['schedules'].empty:
        loader.load_schedules(transformed_data['schedules'])
    
    # Vérification
    counts = loader.verify_counts()
    
    logger.info("\n📊 Statistiques finales:")
    logger.info(f"  - Opérateurs: {counts.get('operators', 0)}")
    logger.info(f"  - Gares: {counts.get('stations', 0)}")
    logger.info(f"  - Trains: {counts.get('trains', 0)}")
    logger.info(f"  - Dessertes: {counts.get('schedules', 0)}")
    
    return counts


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(description='ETL ObRail Europe')
    parser.add_argument('--source', type=str, help='Source spécifique à traiter')
    parser.add_argument('--full', action='store_true', help='Exécution complète')
    args = parser.parse_args()
    
    # Configuration du logging
    logger = setup_logging("obrail_etl")
    
    logger.info("\n" + "🚂" * 35)
    logger.info("  ETL OBRAIL EUROPE - DÉMARRAGE")
    logger.info("🚂" * 35 + "\n")
    
    try:
        # Phase 1: Extraction
        extracted_data = run_extraction(logger, args.source)
        
        if not extracted_data:
            logger.error("❌ Aucune donnée extraite. Arrêt.")
            return 1
        
        # Phase 2: Transformation
        transformed_data = run_transformation(logger, extracted_data)
        
        # Phase 3: Chargement
        counts = run_loading(logger, transformed_data)
        
        logger.info("\n" + "✅" * 35)
        logger.info("  ETL TERMINÉ AVEC SUCCÈS")
        logger.info("✅" * 35 + "\n")
        
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ ERREUR FATALE: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
