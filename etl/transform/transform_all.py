#!/usr/bin/env python3
"""
Script principal pour transformer toutes les données (GTFS, JSON, etc.)
Orchestre la transformation de Raw vers Clean avec consolidation.
"""

import pandas as pd
from pathlib import Path
import logging
import shutil

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def consolidate_csv_files(source_dirs, file_pattern, output_path, source_name):
    """
    Consolide plusieurs fichiers CSV en un seul.
    """
    all_dataframes = []
    
    for source_dir in source_dirs:
        filepath = source_dir / file_pattern
        if filepath.exists():
            try:
                df = pd.read_csv(filepath)
                df['source'] = source_dir.name
                all_dataframes.append(df)
                logger.info(f"   ✅ {file_pattern}: {len(df):,} lignes ({source_dir.name})")
            except Exception as e:
                logger.error(f"   ❌ Erreur lecture {filepath}: {e}")
    
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        combined_df.to_csv(output_path, index=False)
        logger.info(f"   📁 Consolidé: {output_path.name} ({len(combined_df):,} lignes total)")
        return len(combined_df)
    else:
        logger.warning(f"   ⚠️  Aucun fichier trouvé pour {file_pattern}")
        return 0


def main():
    """Fonction principale - Transforme et consolide toutes les données"""
    print("="*70)
    print("🔄 TRANSFORMATION + CONSOLIDATION (RAW -> CLEAN)")
    print("="*70)
    
    # --- 1. Définition des Chemins ---
    BASE_DIR = Path(__file__).parent.parent
    RAW_DIR = BASE_DIR / "data" / "raw"
    EXTRACTED_DIR = RAW_DIR / "extracted"
    ARCHIVES_DIR = RAW_DIR / "archives"
    CLEAN_DIR = BASE_DIR / "data" / "clean"
    
    # Dossier temporaire pour les fichiers transformés
    TEMP_DIR = CLEAN_DIR / "_temp_transform"
    
    # Créer les dossiers
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(exist_ok=True)
    
    print(f"📁 Dossier Raw Extracted : {EXTRACTED_DIR}")
    print(f"📁 Dossier Clean Output : {CLEAN_DIR}")
    print("="*70)
    
    # --- 2. Transformation GTFS (vers dossier temporaire) ---
    print("\n" + "="*70)
    print("📦 ÉTAPE 1: TRANSFORMATION GTFS")
    print("="*70)
    
    from .transform_gtfs import GTFSTransformer
    
    gtfs_sources_count = 0
    if EXTRACTED_DIR.exists():
        for source_dir in EXTRACTED_DIR.iterdir():
            if source_dir.is_dir():
                if (source_dir / "agency.txt").exists() or (source_dir / "stops.txt").exists():
                    source_name = source_dir.name
                    print(f"\n🔹 Traitement: {source_name}")
                    
                    source_temp_dir = TEMP_DIR / source_name
                    source_temp_dir.mkdir(exist_ok=True)
                    
                    transformer = GTFSTransformer(source_dir, source_temp_dir, source_name)
                    
                    try:
                        transformer.transform_agency()
                        transformer.transform_routes()
                        transformer.transform_stops()
                        transformer.transform_trips()
                        transformer.transform_calendar_dates()
                        transformer.transform_stop_times()
                        gtfs_sources_count += 1
                    except Exception as e:
                        logger.error(f"❌ Erreur {source_name}: {e}")
    
    # --- 3. Transformation Back-on-Track ---
    print("\n" + "="*70)
    print("🌙 ÉTAPE 2: TRANSFORMATION BACK-ON-TRACK")
    print("="*70)
    
    bot_temp_dir = TEMP_DIR / "back_on_track"
    bot_json_path = ARCHIVES_DIR / "back_on_track_database.json"
    
    if bot_json_path.exists():
        print(f"🔹 Traitement: back_on_track")
        from .transform_backontrack import BackOnTrackTransformer
        bot_transformer = BackOnTrackTransformer(bot_json_path, bot_temp_dir)
        try:
            bot_transformer.transform_night_trains()
            print("   ✅ Back-on-Track terminé")
        except Exception as e:
            logger.error(f"❌ Erreur Back-on-Track: {e}")
    
    # --- 4. Consolidation ---
    print("\n" + "="*70)
    print("🔗 ÉTAPE 3: CONSOLIDATION")
    print("="*70)
    
    # Créer les dossiers de sortie
    output_folders = {
        'agency': 'agency',
        'routes': 'routes', 
        'stops': 'stops',
        'trips': 'trips',
        'calendar_dates': 'calendar_dates',
        'stop_times': 'stop_times',
        'night_trains': 'night_trains',
        'night_train_countries': 'night_trains',
        'night_train_operators': 'night_trains',
        'night_train_stations': 'night_trains',
    }
    
    for folder_name in set(output_folders.values()):
        (CLEAN_DIR / folder_name).mkdir(exist_ok=True)
    
    # Collecter tous les dossiers sources
    source_dirs = [d for d in TEMP_DIR.iterdir() if d.is_dir()]
    
    total_rows = 0
    
    # Consolidation GTFS
    total_rows += consolidate_csv_files(source_dirs, "agency.csv", CLEAN_DIR / "agency" / "agency.csv", "agency")
    total_rows += consolidate_csv_files(source_dirs, "routes.csv", CLEAN_DIR / "routes" / "routes.csv", "routes")
    total_rows += consolidate_csv_files(source_dirs, "stops.csv", CLEAN_DIR / "stops" / "stops.csv", "stops")
    total_rows += consolidate_csv_files(source_dirs, "trips.csv", CLEAN_DIR / "trips" / "trips.csv", "trips")
    total_rows += consolidate_csv_files(source_dirs, "calendar_dates.csv", CLEAN_DIR / "calendar_dates" / "calendar_dates.csv", "calendar_dates")
    total_rows += consolidate_csv_files(source_dirs, "stop_times.csv", CLEAN_DIR / "stop_times" / "stop_times.csv", "stop_times")
    
    # Consolidation Back-on-Track
    bot_source_dir = TEMP_DIR / "back_on_track"
    if bot_source_dir.exists():
        total_rows += consolidate_csv_files([bot_source_dir], "night_trains.csv", CLEAN_DIR / "night_trains" / "night_trains.csv", "night_trains")
        total_rows += consolidate_csv_files([bot_source_dir], "night_train_countries.csv", CLEAN_DIR / "night_trains" / "night_train_countries.csv", "night_train_countries")
        total_rows += consolidate_csv_files([bot_source_dir], "night_train_operators.csv", CLEAN_DIR / "night_trains" / "night_train_operators.csv", "night_train_operators")
        total_rows += consolidate_csv_files([bot_source_dir], "night_train_stations.csv", CLEAN_DIR / "night_trains" / "night_train_stations.csv", "night_train_stations")
    
    # --- 5. Nettoyage ---
    print("\n" + "="*70)
    print("🧹 NETTOYAGE")
    print("="*70)
    
    # Supprimer le dossier temporaire
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)
        print("   🗑️ Dossier temporaire supprimé")
    
    # --- 6. Résumé ---
    print("\n" + "="*70)
    print("✅ TRANSFORMATION + CONSOLIDATION TERMINÉE")
    print("="*70)
    print(f"   📊 Total lignes: {total_rows:,}")
    print(f"   📁 Emplacement: {CLEAN_DIR}")
    print(f"\n📂 Structure finale:")
    
    for folder in sorted(CLEAN_DIR.iterdir()):
        if folder.is_dir():
            csv_files = list(folder.glob("*.csv"))
            if csv_files:
                for f in csv_files:
                    try:
                        df = pd.read_csv(f)
                        print(f"   📁 {folder.name}/{f.name}: {len(df):,} lignes, {len(df.columns)} colonnes")
                        if 'source' in df.columns:
                            sources = df['source'].unique()
                            print(f"      Sources: {', '.join(sources)}")
                    except:
                        pass
    
    print("="*70)


if __name__ == "__main__":
    main()
