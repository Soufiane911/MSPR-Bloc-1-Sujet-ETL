#!/usr/bin/env python3
"""
Script d'exploration des données ETL ObRail Europe
Analyse les données GTFS SNCF et Back-on-Track
"""

import pandas as pd
import json
import os
from pathlib import Path

# Chemins des données
BASE_DIR = Path(__file__).parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
SNCF_GTFS_DIR = RAW_DATA_DIR / "sncf_gtfs"
BACKONTRACK_DIR = RAW_DATA_DIR / "back_on_track"


def explore_gtfs_file(filepath, filename):
    """Explore un fichier GTFS et affiche ses statistiques"""
    print(f"\n{'='*60}")
    print(f"📄 Analyse de {filename}")
    print(f"{'='*60}")
    
    try:
        df = pd.read_csv(filepath, low_memory=False)
        
        print(f"\n📊 Statistiques générales :")
        print(f"   - Nombre de lignes : {len(df):,}")
        print(f"   - Nombre de colonnes : {len(df.columns)}")
        print(f"   - Taille mémoire : {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
        
        print(f"\n📋 Colonnes :")
        for col in df.columns:
            dtype = df[col].dtype
            null_count = df[col].isna().sum()
            null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0.0
            print(f"   - {col:30s} | Type: {str(dtype):10s} | Nulls: {null_count:6,} ({null_pct:5.2f}%)")
        
        print(f"\n🔍 Aperçu des données (5 premières lignes) :")
        print(df.head().to_string())
        
        # Détecter les doublons
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            print(f"\n⚠️  Doublons détectés : {duplicates:,}")
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture de {filename}: {e}")
        return None


def explore_backontrack():
    """Explore les données Back-on-Track"""
    print(f"\n{'='*60}")
    print(f"🌙 Analyse des données Back-on-Track")
    print(f"{'='*60}")
    
    # CSV
    csv_path = BACKONTRACK_DIR / "night_trains.csv"
    if csv_path.exists():
        print(f"\n📄 Fichier CSV :")
        df_csv = pd.read_csv(csv_path)
        print(f"   - Nombre de trains de nuit : {len(df_csv)}")
        print(f"   - Colonnes : {', '.join(df_csv.columns)}")
        print(f"\n   Aperçu :")
        print(df_csv.head(3).to_string())
        
        # Statistiques par pays
        if 'countries' in df_csv.columns:
            print(f"\n🌍 Répartition par pays :")
            countries = df_csv['countries'].str.split(', ').explode()
            print(countries.value_counts().head(10))
        
        # Statistiques par opérateur
        if 'operators' in df_csv.columns:
            print(f"\n🚂 Répartition par opérateur :")
            operators = df_csv['operators'].str.split(', ').explode()
            print(operators.value_counts().head(10))
    
    # JSON
    json_path = BACKONTRACK_DIR / "night_trains.json"
    if json_path.exists():
        print(f"\n📄 Fichier JSON :")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"   - Nombre de trains de nuit : {len(data)}")
        print(f"   - Clés du premier élément : {list(data[0].keys()) if data else 'Aucune donnée'}")


def explore_gtfs_relationships():
    """Explore les relations entre les fichiers GTFS"""
    print(f"\n{'='*60}")
    print(f"🔗 Analyse des relations GTFS")
    print(f"{'='*60}")
    
    # Charger les fichiers principaux
    files_to_load = {
        'agency': 'agency.txt',
        'routes': 'routes.txt',
        'stops': 'stops.txt',
        'trips': 'trips.txt',
        'stop_times': 'stop_times.txt',
    }
    
    dataframes = {}
    
    for key, filename in files_to_load.items():
        filepath = SNCF_GTFS_DIR / filename
        if filepath.exists():
            dataframes[key] = pd.read_csv(filepath, low_memory=False)
    
    # Relations
    if 'agency' in dataframes and 'routes' in dataframes:
        print(f"\n🔗 Agency → Routes :")
        print(f"   - Nombre d'agencies : {len(dataframes['agency'])}")
        print(f"   - Nombre de routes : {len(dataframes['routes'])}")
        if 'agency_id' in dataframes['routes'].columns:
            agency_counts = dataframes['routes']['agency_id'].value_counts()
            print(f"   - Routes par agency :")
            for agency_id, count in agency_counts.head(5).items():
                print(f"     * {agency_id}: {count} routes")
    
    if 'routes' in dataframes and 'trips' in dataframes:
        print(f"\n🔗 Routes → Trips :")
        print(f"   - Nombre de routes : {len(dataframes['routes'])}")
        print(f"   - Nombre de trips : {len(dataframes['trips'])}")
        if 'route_id' in dataframes['trips'].columns:
            route_counts = dataframes['trips']['route_id'].value_counts()
            print(f"   - Trips par route (top 5) :")
            for route_id, count in route_counts.head(5).items():
                print(f"     * {route_id[:50]}... : {count} trips")
    
    if 'trips' in dataframes and 'stop_times' in dataframes:
        print(f"\n🔗 Trips → StopTimes :")
        print(f"   - Nombre de trips : {len(dataframes['trips'])}")
        print(f"   - Nombre de stop_times : {len(dataframes['stop_times'])}")
        if 'trip_id' in dataframes['stop_times'].columns:
            trip_counts = dataframes['stop_times']['trip_id'].value_counts()
            print(f"   - StopTimes par trip (moyenne) : {trip_counts.mean():.2f}")
            print(f"   - StopTimes par trip (min/max) : {trip_counts.min()} / {trip_counts.max()}")
    
    if 'stops' in dataframes and 'stop_times' in dataframes:
        print(f"\n🔗 Stops → StopTimes :")
        print(f"   - Nombre de stops : {len(dataframes['stops'])}")
        print(f"   - Nombre de stop_times : {len(dataframes['stop_times'])}")
        if 'stop_id' in dataframes['stop_times'].columns:
            stop_counts = dataframes['stop_times']['stop_id'].value_counts()
            print(f"   - StopTimes par stop (top 5) :")
            for stop_id, count in stop_counts.head(5).items():
                print(f"     * {stop_id[:50]}... : {count} passages")


def main():
    """Fonction principale"""
    print("="*60)
    print("🔍 EXPLORATION DES DONNÉES ETL OBRAIL EUROPE")
    print("="*60)
    
    # Vérifier que les dossiers existent
    if not SNCF_GTFS_DIR.exists():
        print(f"❌ Dossier GTFS introuvable : {SNCF_GTFS_DIR}")
        return
    
    if not BACKONTRACK_DIR.exists():
        print(f"⚠️  Dossier Back-on-Track introuvable : {BACKONTRACK_DIR}")
    
    # Explorer les fichiers GTFS
    print(f"\n{'='*60}")
    print("📦 EXPLORATION DES DONNÉES GTFS SNCF")
    print(f"{'='*60}")
    
    gtfs_files = [
        'agency.txt',
        'routes.txt',
        'stops.txt',
        'trips.txt',
        'stop_times.txt',
        'calendar_dates.txt',
        'transfers.txt',
        'feed_info.txt'
    ]
    
    for filename in gtfs_files:
        filepath = SNCF_GTFS_DIR / filename
        if filepath.exists():
            explore_gtfs_file(filepath, filename)
        else:
            print(f"⚠️  Fichier introuvable : {filename}")
    
    # Explorer les relations
    explore_gtfs_relationships()
    
    # Explorer Back-on-Track
    if BACKONTRACK_DIR.exists():
        explore_backontrack()
    
    print(f"\n{'='*60}")
    print("✅ Exploration terminée !")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

