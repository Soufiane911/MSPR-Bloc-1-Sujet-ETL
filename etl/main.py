#!/usr/bin/env python3
"""
Script principal ETL pour ObRail Europe.

Ce script orchestre l'ensemble du processus ETL:
1. Téléchargement/Vérification des sources (avec fraîcheur)
2. Extraction des données
3. Transformation et nettoyage
4. Classification jour/nuit
5. Chargement dans PostgreSQL

Usage:
    python main.py                     # Normal : vérifie fraîcheur, skip si à jour
    python main.py --refresh           # Force le check + téléchargement si obsolète
    python main.py --force             # Force le téléchargement même si à jour
    python main.py --status            # Affiche l'état de fraîcheur des sources (rapide)
    python main.py --source renfe      # Traite une seule source

Optimisations:
- Imports lazy pour démarrage rapide
- Mode --status : cache uniquement, pas de réseau
- Mode normal : cache uniquement, réseau uniquement avec --refresh
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

# Ajout du répertoire parent au path
sys.path.insert(0, str(Path(__file__).parent))

# Imports légers pour CLI rapide
from config.logging_config import setup_logging
from config.sources import SOURCES

# Imports lourds seront faits dans les fonctions (lazy loading)
pd = None  # pandas - importé dans les fonctions concernées


def lazy_import_pandas():
    """Import pandas seulement quand nécessaire."""
    global pd
    if pd is None:
        import pandas as pd_module

        pd = pd_module
    return pd


def lazy_import_extractors():
    """Import des extracteurs seulement quand nécessaire."""
    from extractors.back_on_track import BackOnTrackExtractor
    from extractors.gtfs_extractor import GTFSExtractor
    from extractors.mobility_catalog import MobilityCatalogExtractor

    return BackOnTrackExtractor, GTFSExtractor, MobilityCatalogExtractor


def lazy_import_transformers():
    """Import des transformers seulement quand nécessaire."""
    from transformers.data_cleaner import DataCleaner
    from transformers.data_normalizer import DataNormalizer
    from transformers.day_night_classifier import DayNightClassifier
    from transformers.data_merger import DataMerger
    from transformers.business_validator import BusinessValidator

    return DataCleaner, DataNormalizer, DayNightClassifier, DataMerger, BusinessValidator


def lazy_import_loaders():
    """Import des loaders seulement quand nécessaire."""
    from loaders.database_loader import DatabaseLoader

    return DatabaseLoader


def show_status_fast(logger):
    """Affiche le statut rapide depuis le cache local (pas de requête réseau)."""
    import json
    from datetime import datetime, timezone

    logger.info("\n" + "=" * 70)
    logger.info("STATUT RAPIDE DES SOURCES (cache local uniquement)")
    logger.info("=" * 70 + "\n")

    CACHE_DIR = Path(__file__).parent.parent / "data" / ".cache"

    if not CACHE_DIR.exists():
        print("⚠️  Aucun cache trouvé. Lancez avec --refresh pour initialiser.")
        return 1

    total = 0
    up_to_date = 0
    expired = 0
    never = 0

    for source_name, config in SOURCES.items():
        if not config.get("enabled", True):
            continue

        cache_path = CACHE_DIR / f"{source_name}.json"
        total += 1

        if cache_path.exists():
            try:
                with open(cache_path, "r") as f:
                    cache = json.load(f)

                last_download = cache.get("last_download", "Inconnu")
                if last_download and last_download != "Inconnu":
                    dt = datetime.fromisoformat(last_download.replace("Z", "+00:00"))
                    age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600

                    if age_hours > config.get("max_age_hours", 168):
                        status = "⏰ Expiré"
                        expired += 1
                    else:
                        status = "✅ OK"
                        up_to_date += 1

                    date_str = dt.strftime("%d/%m/%Y %H:%M")
                    print(f"  {source_name:<25} {date_str:<20} {status}")
                else:
                    print(f"  {source_name:<25} {'Jamais':<20} ⚠️  Jamais")
                    never += 1
            except Exception as e:
                print(f"  {source_name:<25} {'Erreur':<20} ❌ {str(e)[:20]}")
                never += 1
        else:
            print(f"  {source_name:<25} {'Inconnu':<20} ⚠️  Pas de cache")
            never += 1

    print()
    print(f"{'Source':<25} {'Dernier DL':<20} {'Statut':<15}")
    print("-" * 70)
    print(
        f"\nRésumé: {up_to_date} ✅ à jour, {expired} ⏰ expirées, {never} ⚠️ jamais/sans cache"
    )
    print()
    print("💡 Pour vérifier le réseau: python main.py --refresh --status")

    return 0


def run_download(
    logger,
    source_filter: Optional[str] = None,
    refresh: bool = False,
    force: bool = False,
):
    """
    Exécute la phase de téléchargement/vérification.

    OPTIMISATION: En mode normal (sans --refresh), vérifie uniquement le cache
    local sans faire de requête réseau. Le réseau n'est interrogé qu'avec --refresh.

    Args:
        logger: Logger
        source_filter: Filtre sur une source spécifique
        refresh: Vérifier et télécharger si obsolète (requête réseau)
        force: Forcer le téléchargement

    Returns:
        dict: Résultats par source
    """
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 0: VÉRIFICATION/TÉLÉCHARGEMENT DES SOURCES")
    logger.info("=" * 70 + "\n")

    # Filtrer les sources si nécessaire
    sources_to_process = {}
    for name, config in SOURCES.items():
        if not config.get("enabled", True):
            logger.info(f"[DISABLED] {name}: Desactive")
            continue
        if source_filter and name != source_filter:
            continue
        sources_to_process[name] = config

    if not sources_to_process:
        logger.warning("Aucune source à traiter")
        return {}

    # Mode de fonctionnement
    if force:
        logger.info("[MODE] FORCE - Telechargement de toutes les sources")
    elif refresh:
        logger.info(
            "[MODE] RAFRAICHISSEMENT - Vérification réseau + téléchargement si obsolète"
        )
    else:
        logger.info("[MODE] NORMAL - Cache local uniquement (pas de requête réseau)")
        logger.info(
            "         Utilisez --refresh pour vérifier les mises à jour distantes"
        )

    # Import lazy du downloader seulement si nécessaire
    from extractors.downloader import download_all_sources

    results = download_all_sources(sources_to_process, refresh=refresh, force=force)

    # Afficher les résultats
    for source_name, result in results.items():
        if result.get("skipped"):
            logger.info(f"[SKIP] {source_name}: {result.get('reason')}")
        elif result.get("downloaded"):
            logger.info(
                f"[OK] {source_name}: Telecharge ({len(result.get('files', []))} fichiers)"
            )
        elif result.get("used_cache"):
            logger.info(f"[CACHE] {source_name}: Cache utilise")
        else:
            logger.error(
                f"[ERROR] {source_name}: {result.get('error', 'Erreur inconnue')}"
            )

    return results


def run_extraction(logger, source_filter: Optional[str] = None) -> dict:
    """
    Exécute la phase d'extraction.

    Args:
        logger: Logger
        source_filter: Filtre sur une source spécifique

    Returns:
        dict: Données extraites par source
    """
    # Import lazy des extracteurs seulement quand extraction demandée
    BackOnTrackExtractor, GTFSExtractor, MobilityCatalogExtractor = (
        lazy_import_extractors()
    )

    logger.info("\n" + "=" * 70)
    logger.info("PHASE 1: EXTRACTION DES DONNÉES")
    logger.info("=" * 70 + "\n")

    extracted_data = {}

    # 1. Back-on-Track (trains de nuit)
    if source_filter is None or source_filter == "back_on_track":
        logger.info("[START] Extraction Back-on-Track...")
        try:
            extractor = BackOnTrackExtractor()
            data = extractor.extract()["catalog"]

            if extractor.validate():
                extracted_data["back_on_track"] = {"data": data, "country": "EU"}
                logger.info("[OK] Back-on-Track: Extraction reussie\n")
            else:
                logger.error("[ERROR] Back-on-Track: Validation echouee\n")
        except Exception as e:
            logger.error(f"[ERROR] Back-on-Track: {str(e)}\n")

    # 2. Mobility Catalog (référence)
    if source_filter is None or source_filter == "mobility_catalog":
        logger.info("[START] Extraction Mobility Catalog...")
        try:
            extractor = MobilityCatalogExtractor()
            data = extractor.extract()

            if extractor.validate():
                extracted_data["mobility_catalog"] = {"catalog": data}
                european = extractor.get_european_feeds()
                logger.info(f"[OK] Mobility Catalog: {len(european)} flux europeens\n")
            else:
                logger.error("[ERROR] Mobility Catalog: Validation echouee\n")
        except Exception as e:
            logger.error(f"[ERROR] Mobility Catalog: {str(e)}\n")

    # 3. Sources GTFS nationales
    gtfs_sources = [
        ("sncf_intercites", "FR"),
        ("db_fernverkehr", "DE"),
        ("renfe", "ES"),
        ("trenitalia", "IT"),
        ("cff_sbb", "CH"),
        ("sncb", "BE"),
    ]

    for source_key, country in gtfs_sources:
        if source_filter is None or source_filter == source_key:
            if source_key in SOURCES and SOURCES[source_key].get("enabled", True):
                logger.info(f"[START] Extraction {source_key}...")
                try:
                    extractor = GTFSExtractor(
                        source_name=source_key, country_code=country
                    )
                    data = extractor.extract()

                    if extractor.validate():
                        extracted_data[source_key] = {"gtfs": data, "country": country}
                        logger.info(f"[OK] {source_key}: Extraction reussie\n")
                    else:
                        logger.warning(f"[WARN] {source_key}: Validation partielle\n")
                except Exception as e:
                    logger.error(f"[ERROR] {source_key}: {str(e)}\n")

    logger.info(f"Total sources extraites: {len(extracted_data)}")
    return extracted_data


def run_transformation(logger, extracted_data: dict) -> dict:
    """Exécute la phase de transformation."""
    # Import lazy des transformers
    DataCleaner, DataNormalizer, DayNightClassifier, DataMerger, BusinessValidator = (
        lazy_import_transformers()
    )

    logger.info("\n" + "=" * 70)
    logger.info("PHASE 2: TRANSFORMATION DES DONNÉES")
    logger.info("=" * 70 + "\n")

    transformed_data = {}
    cleaner = DataCleaner()
    normalizer = DataNormalizer()
    classifier = DayNightClassifier()
    merger = DataMerger()
    validator = BusinessValidator()

    for source_name, data in extracted_data.items():
        logger.info(f"[STEP] Transformation de {source_name}...")

        if source_name == "back_on_track":
            transformed = process_back_on_track(
                data["data"], cleaner, normalizer, classifier
            )
            transformed_data[source_name] = transformed
        elif source_name != "mobility_catalog":
            transformed = process_gtfs_source(
                source_name,
                data["gtfs"],
                data["country"],
                cleaner,
                normalizer,
                classifier,
            )
            transformed_data[source_name] = transformed

    logger.info("\nFusion des donnees...")
    for source_name, data in transformed_data.items():
        merger.add_source(source_name, data)

    operators = merger.merge_operators()
    stations = merger.merge_stations()
    trains = merger.merge_trains(operators)
    schedules = merger.merge_schedules(trains, stations)

    # Phase 2b: Validation metier avant chargement
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 2b: VALIDATION METIER")
    logger.info("=" * 70 + "\n")

    merged = validator.validate_all({
        "operators": operators,
        "stations": stations,
        "trains": trains,
        "schedules": schedules,
    })

    logger.info("[OK] Transformation et validation terminées\n")
    return merged


def process_back_on_track(data: dict, cleaner, normalizer, classifier) -> dict:
    """Traite les données Back-on-Track."""
    pd = lazy_import_pandas()

    result = {}
    if "agencies" in data:
        result["agencies"] = cleaner.clean_routes(data["agencies"])
        result["agencies"]["country"] = None
    if "stops" in data:
        result["stops"] = cleaner.clean_stations(data["stops"])
        result["stops"] = normalizer.normalize_coordinates(result["stops"])
    if "routes" in data:
        result["routes"] = data["routes"].copy()
        result["routes"]["train_type"] = "night"
        result["routes"]["train_type_rule"] = "night"
        result["routes"]["train_type_heuristic"] = "night"
        result["routes"]["train_type_ml"] = pd.NA
        result["routes"]["classification_method"] = "rule"
        result["routes"]["classification_reason"] = "back_on_track_reference"
        result["routes"]["classification_confidence"] = 1.0
        result["routes"]["ml_night_probability"] = pd.NA
        result["routes"]["night_percentage"] = 100.0
        result["routes"]["needs_manual_review"] = False

    # CORRECTION: Ajouter trips et trip_stop pour générer des schedules
    if "trips" in data:
        result["trips"] = data["trips"].copy()
        # Classification night pour tous les trips Back-on-Track
        result["trips"]["train_type"] = "night"
        result["trips"]["train_type_rule"] = "night"
        result["trips"]["train_type_heuristic"] = pd.NA
        result["trips"]["train_type_ml"] = pd.NA
        result["trips"]["classification_method"] = "rule"
        result["trips"]["classification_reason"] = "back_on_track_reference"
        result["trips"]["classification_confidence"] = 1.0
        result["trips"]["ml_night_probability"] = pd.NA
        result["trips"]["night_percentage"] = 100.0
        result["trips"]["needs_manual_review"] = False

    if "trip_stop" in data:
        # trip_stop est l'équivalent de stop_times pour Back-on-Track
        result["stop_times"] = data["trip_stop"].copy()

    return result


def process_gtfs_source(
    source_name: str, data: dict, country: str, cleaner, normalizer, classifier
) -> dict:
    """Traite une source GTFS avec filtrage des données non-ferroviaires."""
    pd = lazy_import_pandas()

    result = {}
    if "agency" in data:
        result["agency"] = data["agency"].copy()
        result["agency"]["country"] = country

    # Étape 1: Filtrer les routes pour ne garder que le rail
    if "routes" in data:
        routes_standardized = normalizer.standardize_route_types(data["routes"])
        result["routes"] = normalizer.filter_rail_routes_only(routes_standardized)

    # Étape 2: Filtrer les trips pour ne garder que ceux des routes ferroviaires
    if "trips" in data:
        result["trips"] = data["trips"].copy()

        # Filtrer les trips par route_id si on a filtré les routes
        if "routes" in result and not result["routes"].empty:
            rail_route_ids = set(result["routes"]["route_id"].unique())
            initial_trips_count = len(result["trips"])
            result["trips"] = result["trips"][
                result["trips"]["route_id"].isin(rail_route_ids)
            ].copy()
            removed_trips = initial_trips_count - len(result["trips"])
            if removed_trips > 0:
                logger = setup_logging("transformer.filter")
                logger.info(
                    f"[FILTER] {source_name}: {removed_trips}/{initial_trips_count} "
                    f"trips supprimés (non-ferroviaires)"
                )

    # Étape 3: Filtrer les stop_times pour ne garder que ceux des trips filtrés
    if "stop_times" in data:
        result["stop_times"] = normalizer.normalize_stop_times(data["stop_times"])

        if "trips" in result and not result["trips"].empty:
            rail_trip_ids = set(result["trips"]["trip_id"].unique())
            initial_st_count = len(result["stop_times"])
            result["stop_times"] = result["stop_times"][
                result["stop_times"]["trip_id"].isin(rail_trip_ids)
            ].copy()
            removed_st = initial_st_count - len(result["stop_times"])
            if removed_st > 0:
                logger = setup_logging("transformer.filter")
                logger.info(
                    f"[FILTER] {source_name}: {removed_st:,}/{initial_st_count:,} "
                    f"stop_times supprimés (trips non-ferroviaires)"
                )

    if "stops" in data:
        result["stops"] = cleaner.clean_stations(data["stops"])
        result["stops"] = normalizer.normalize_coordinates(result["stops"])

    # Classification jour/nuit (après filtrage)
    if "trips" in result and not result["trips"].empty:
        if "stop_times" in result and not result["stop_times"].empty:
            trip_classification = classifier.classify_gtfs_trips(
                stop_times=result["stop_times"],
                trips=result["trips"],
                routes=result.get("routes"),
                source_name=source_name,
            )
            result["trips"] = result["trips"].merge(
                trip_classification,
                on="trip_id",
                how="left",
            )
        else:
            result["trips"]["train_type"] = "day"
            result["trips"]["train_type_rule"] = pd.NA
            result["trips"]["train_type_heuristic"] = pd.NA
            result["trips"]["train_type_ml"] = pd.NA
            result["trips"]["classification_method"] = "default_day"
            result["trips"]["classification_reason"] = "missing_stop_times"
            result["trips"]["classification_confidence"] = 0.5
            result["trips"]["ml_night_probability"] = pd.NA
            result["trips"]["night_percentage"] = pd.NA
            result["trips"]["needs_manual_review"] = True

    return result


def run_loading(logger, transformed_data: dict) -> tuple:
    """Exécute la phase de chargement."""
    DatabaseLoader = lazy_import_loaders()

    logger.info("\n" + "=" * 70)
    logger.info("PHASE 3: CHARGEMENT DANS POSTGRESQL (OPTIMISE)")
    logger.info("=" * 70 + "\n")

    loader = DatabaseLoader()

    if "operators" in transformed_data and not transformed_data["operators"].empty:
        loader.load_operators(transformed_data["operators"])
    if "stations" in transformed_data and not transformed_data["stations"].empty:
        loader.load_stations(transformed_data["stations"])
    if "trains" in transformed_data and not transformed_data["trains"].empty:
        loader.load_trains(transformed_data["trains"])
    if "schedules" in transformed_data and not transformed_data["schedules"].empty:
        loader.load_schedules(transformed_data["schedules"])
        loader.calculate_distances()

    counts = loader.verify_counts()

    logger.info("\nStatistiques finales:")
    logger.info(f"  - Opérateurs: {counts.get('operators', 0)}")
    logger.info(f"  - Gares: {counts.get('stations', 0)}")
    logger.info(f"  - Trains: {counts.get('trains', 0)}")
    logger.info(f"  - Dessertes: {counts.get('schedules', 0)}")

    # Afficher le rapport de performance
    logger.info(loader.get_performance_report())

    return counts, loader


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(
        description="ETL ObRail Europe avec vérification de fraîcheur"
    )
    parser.add_argument("--source", type=str, help="Source spécifique à traiter")
    parser.add_argument(
        "--refresh", action="store_true", help="Vérifier et télécharger si obsolète"
    )
    parser.add_argument("--force", action="store_true", help="Forcer le téléchargement")
    parser.add_argument(
        "--status", action="store_true", help="Afficher le statut des sources"
    )
    parser.add_argument("--full", action="store_true", help="Exécution complète")
    parser.add_argument(
        "--skip-download", action="store_true", help="Skip la phase de téléchargement"
    )
    args = parser.parse_args()

    logger = setup_logging("obrail_etl")

    logger.info("\n" + "=" * 35)
    logger.info("  ETL OBRAIL EUROPE - DEMARRAGE")
    logger.info("=" * 35 + "\n")

    try:
        # Mode --status rapide (cache uniquement, pas de requête réseau)
        if args.status:
            if not args.refresh:
                return show_status_fast(logger)
            else:
                # Avec --refresh, on fait le check réseau complet
                from config.freshness import get_all_sources_status, format_status_table

                logger.info("\n" + "=" * 70)
                logger.info("STATUT COMPLET DES SOURCES (avec vérification réseau)")
                logger.info("=" * 70 + "\n")
                status = get_all_sources_status(SOURCES)
                print(format_status_table(status))
                return 0

        # Phase 0: Téléchargement (sauf si --skip-download)
        if not args.skip_download:
            download_results = run_download(
                logger, args.source, args.refresh, args.force
            )

        # Phase 1: Extraction
        extracted_data = run_extraction(logger, args.source)

        if not extracted_data:
            logger.error("[ERROR] Aucune donnee extraite. Arret.")
            return 1

        # Phase 2: Transformation
        transformed_data = run_transformation(logger, extracted_data)

        # Phase 3: Chargement
        counts, loader = run_loading(logger, transformed_data)

        # Phase 4: Rapport qualité
        from config.quality_report import quality_report
        logger.info("\n" + quality_report.format_report())

        logger.info("\n" + "=" * 35)
        logger.info("  ETL TERMINE AVEC SUCCES")
        logger.info("=" * 35 + "\n")

        return 0

    except Exception as e:
        logger.error(f"\n[ERROR] ERREUR FATALE: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
