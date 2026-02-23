#!/usr/bin/env python3
"""
ETL COMPLET ObRail Europe - TOUTES LES SOURCES
Sans filtrage préalable - on garde tout pour analyse
"""

import pandas as pd
import json
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
NOTE_SOURCES_DIR = Path('note/sources-data')
OUTPUT_DIR = Path('data/processed')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def parse_time(time_str):
    """Parse HH:MM:SS"""
    try:
        parts = str(time_str).split(':')
        return int(parts[0]) * 60 + int(parts[1])
    except:
        return None

def calculer_poids_nocturne(dep_min, arr_min):
    """Calcule % du trajet entre 22h et 6h"""
    NUIT_DEBUT, NUIT_FIN = 22*60, 6*60
    
    if arr_min < dep_min:
        duree_totale = (24*60 - dep_min) + arr_min
    else:
        duree_totale = arr_min - dep_min
    
    if duree_totale == 0:
        return 0, 0
    
    temps_nuit = 0
    current = dep_min
    while current != arr_min:
        if current >= NUIT_DEBUT or current < NUIT_FIN:
            temps_nuit += 1
        current = (current + 1) % (24*60)
        if current == dep_min:
            break
    
    return (temps_nuit / duree_totale) * 100, duree_totale / 60

def classify_train(dep_time, arr_time, train_code="", has_couchette=False):
    """Classification poids temporel"""
    dep_min = parse_time(dep_time)
    arr_min = parse_time(arr_time)
    
    if dep_min is None or arr_min is None:
        return 'unknown', 0, 'invalid', 0, 0
    
    poids_nuit, duree = calculer_poids_nocturne(dep_min, arr_min)
    
    if has_couchette or any(x in str(train_code).upper() for x in ['NJ', 'EN', 'NIGHT', 'SLEEPER', 'NATTÅG']):
        return 'night', 1.0, 'explicit_night', poids_nuit, duree
    
    if duree < 4:
        return 'day', 0.95, 'too_short', poids_nuit, duree
    
    if poids_nuit >= 50:
        return 'night', 0.95, 'majority_night', poids_nuit, duree
    
    if poids_nuit >= 30 and duree >= 6:
        return 'night', 0.85, 'significant_night', poids_nuit, duree
    
    return 'day', 0.90, 'majority_day', poids_nuit, duree

def extract_back_on_track():
    """1. BACK-ON-TRACK (Europe nuit)"""
    logger.info("=" * 60)
    logger.info("1. EXTRACTION: Back-on-Track")
    logger.info("=" * 60)
    
    with open(NOTE_SOURCES_DIR / 'back-on-track-eu/data/latest/trips.json') as f:
        trips = json.load(f)
    with open(NOTE_SOURCES_DIR / 'back-on-track-eu/data/latest/agencies.json') as f:
        agencies = json.load(f)
    
    records = []
    erreurs = 0
    
    for trip_id, trip in trips.items():
        try:
            dep = trip['origin_departure_time'].split('T')[1][:8]
            arr = trip['destination_arrival_time'].split('T')[1][:8]
            distance = float(trip.get('distance', 0)) if trip.get('distance') else 0
            
            classes = trip.get('classes', '').lower()
            has_couchette = 'sleeper' in classes or 'couchette' in classes
            
            type_jn, conf, rule, poids, duree = classify_train(dep, arr, trip.get('trip_short_name', ''), has_couchette)
            
            agency = agencies.get(trip.get('agency_id', ''), {})
            
            records.append({
                'source': 'back_on_track',
                'country': agency.get('agency_state', ''),
                'operator': agency.get('agency_name', ''),
                'train_number': trip.get('trip_short_name', ''),
                'train_type': 'night_train' if type_jn == 'night' else 'long_distance',
                'departure_time': dep,
                'arrival_time': arr,
                'duration_hours': duree,
                'distance_km': distance,
                'night_percentage': poids,
                'classification': type_jn,
                'confidence': conf,
                'rule': rule,
                'has_couchette': has_couchette,
                'route': f"{trip.get('trip_origin', '')} - {trip.get('trip_headsign', '')}",
            })
        except Exception as e:
            erreurs += 1
    
    logger.info(f"✓ Extraits: {len(records)} trains")
    logger.info(f"⚠ Erreurs: {erreurs} trains")
    return pd.DataFrame(records)

def extract_gtfs_source(folder_name, source_name, country, operator_name):
    """Extraction générique pour sources GTFS"""
    logger.info(f"\n{'=' * 60}")
    logger.info(f"EXTRACTION: {source_name}")
    logger.info("=" * 60)
    
    base_path = NOTE_SOURCES_DIR / folder_name
    
    try:
        stop_times = pd.read_csv(base_path / 'stop_times.txt', low_memory=False)
        trips = pd.read_csv(base_path / 'trips.txt', low_memory=False)
        routes = pd.read_csv(base_path / 'routes.txt', low_memory=False)
        
        # Prendre tous les trips (pas d'échantillon)
        logger.info(f"Total trips dans source: {len(trips)}")
        
        records = []
        erreurs = 0
        
        for _, trip in trips.iterrows():
            try:
                trip_stops = stop_times[stop_times['trip_id'] == trip['trip_id']].sort_values('stop_sequence')
                if len(trip_stops) < 2:
                    continue
                
                first = trip_stops.iloc[0]
                last = trip_stops.iloc[-1]
                dep = first['departure_time']
                arr = last['arrival_time']
                
                type_jn, conf, rule, poids, duree = classify_train(dep, arr, str(trip.get('trip_headsign', '')))
                
                # Récupérer info route
                route_info = routes[routes['route_id'] == trip['route_id']]
                if len(route_info) == 0:
                    continue
                route = route_info.iloc[0]
                
                # Déterminer type de transport
                rtype = route.get('route_type', 2)
                transport_mode = {0: 'tram', 1: 'metro', 2: 'rail', 3: 'bus', 4: 'ferry'}.get(rtype, 'unknown')
                
                records.append({
                    'source': source_name.lower().replace(' ', '_'),
                    'country': country,
                    'operator': operator_name,
                    'train_number': str(trip.get('trip_headsign', ''))[:50],
                    'train_type': transport_mode,
                    'departure_time': dep,
                    'arrival_time': arr,
                    'duration_hours': duree,
                    'distance_km': duree * 80,  # Estimation
                    'night_percentage': poids,
                    'classification': type_jn,
                    'confidence': conf,
                    'rule': rule,
                    'has_couchette': False,
                    'route': str(route.get('route_long_name', ''))[:100],
                })
            except:
                erreurs += 1
        
        logger.info(f"✓ Extraits: {len(records)} trains")
        logger.info(f"⚠ Erreurs/ignorés: {erreurs}")
        return pd.DataFrame(records)
        
    except Exception as e:
        logger.error(f"Erreur extraction {source_name}: {e}")
        return pd.DataFrame()

def extract_sncf_intercites():
    """2. SNCF INTERCITÉS"""
    return extract_gtfs_source('sncf-intercites', 'SNCF Intercités', 'FR', 'SNCF VOYAGEURS')

def extract_sncf_transilien():
    """3. SNCF TRANSILIEN (TOUT - y compris RER)"""
    return extract_gtfs_source('sncf-transilien', 'SNCF Transilien', 'FR', 'SNCF Transilien/TER/RER')

def extract_renfe():
    """4. RENFE ESPAGNE"""
    return extract_gtfs_source('renfe', 'Renfe', 'ES', 'RENFE OPERADORA')

def extract_germany():
    """5. ALLEMAGNE (TOUT - 88k trips)"""
    logger.info(f"\n{'=' * 60}")
    logger.info("5. EXTRACTION: Allemagne (GTFS.de)")
    logger.info("⚠ Source très volumineuse - patience...")
    logger.info("=" * 60)
    
    base_path = NOTE_SOURCES_DIR / 'gtfs-de'
    
    try:
        stop_times = pd.read_csv(base_path / 'stop_times.txt', low_memory=False)
        trips = pd.read_csv(base_path / 'trips.txt', low_memory=False)
        routes = pd.read_csv(base_path / 'routes.txt', low_memory=False)
        agency = pd.read_csv(base_path / 'agency.txt', low_memory=False)
        
        logger.info(f"Total trips: {len(trips):,}")
        
        # Dictionnaire agences
        agency_dict = dict(zip(agency['agency_id'], agency['agency_name']))
        
        records = []
        compteur = 0
        erreurs = 0
        
        for _, trip in trips.iterrows():
            compteur += 1
            if compteur % 10000 == 0:
                logger.info(f"  Traitement... {compteur:,} / {len(trips):,}")
            
            try:
                trip_stops = stop_times[stop_times['trip_id'] == trip['trip_id']].sort_values('stop_sequence')
                if len(trip_stops) < 2:
                    continue
                
                first = trip_stops.iloc[0]
                last = trip_stops.iloc[-1]
                dep = first['departure_time']
                arr = last['arrival_time']
                
                type_jn, conf, rule, poids, duree = classify_train(dep, arr)
                
                # Info route
                route_info = routes[routes['route_id'] == trip['route_id']]
                route_name = str(route_info.iloc[0].get('route_long_name', ''))[:100] if len(route_info) > 0 else ''
                
                # Opérateur
                agency_name = agency_dict.get(trip.get('agency_id', ''), 'DB')
                
                records.append({
                    'source': 'germany_gtfs',
                    'country': 'DE',
                    'operator': agency_name[:50],
                    'train_number': str(trip.get('trip_headsign', ''))[:30],
                    'train_type': 'rail',
                    'departure_time': dep,
                    'arrival_time': arr,
                    'duration_hours': duree,
                    'distance_km': duree * 70,  # Estimation Allemagne
                    'night_percentage': poids,
                    'classification': type_jn,
                    'confidence': conf,
                    'rule': rule,
                    'has_couchette': False,
                    'route': route_name,
                })
            except:
                erreurs += 1
        
        logger.info(f"✓ Extraits: {len(records):,} trains")
        return pd.DataFrame(records)
        
    except Exception as e:
        logger.error(f"Erreur Allemagne: {e}")
        return pd.DataFrame()

def extract_trenitalia():
    """6. TRENITALIA ITALIE"""
    logger.info(f"\n{'=' * 60}")
    logger.info("6. EXTRACTION: Trenitalia (Sardaigne)")
    logger.info("=" * 60)
    
    base_path = NOTE_SOURCES_DIR / 'trenitalia'
    
    try:
        stop_times = pd.read_csv(base_path / 'stop_times.txt', low_memory=False)
        trips = pd.read_csv(base_path / 'trips.txt', low_memory=False)
        routes = pd.read_csv(base_path / 'routes.txt', low_memory=False)
        
        records = []
        
        for _, trip in trips.iterrows():
            try:
                trip_stops = stop_times[stop_times['trip_id'] == trip['trip_id']].sort_values('stop_sequence')
                if len(trip_stops) < 2:
                    continue
                
                first = trip_stops.iloc[0]
                last = trip_stops.iloc[-1]
                
                dep = first['departure_time']
                arr = last['arrival_time']
                
                type_jn, conf, rule, poids, duree = classify_train(dep, arr)
                
                route = routes[routes['route_id'] == trip['route_id']].iloc[0]
                transport = 'bus' if route.get('route_type') == 3 else 'rail'
                
                records.append({
                    'source': 'trenitalia',
                    'country': 'IT',
                    'operator': 'TRENITALIA',
                    'train_number': str(trip.get('trip_headsign', ''))[:30],
                    'train_type': transport,
                    'departure_time': dep,
                    'arrival_time': arr,
                    'duration_hours': duree,
                    'distance_km': duree * 60,
                    'night_percentage': poids,
                    'classification': type_jn,
                    'confidence': conf,
                    'rule': rule,
                    'has_couchette': False,
                    'route': str(route.get('route_long_name', ''))[:100],
                })
            except:
                pass
        
        logger.info(f"✓ Extraits: {len(records)} trains")
        return pd.DataFrame(records)
        
    except Exception as e:
        logger.error(f"Erreur Trenitalia: {e}")
        return pd.DataFrame()

def main():
    """Fonction principale - extraction complète"""
    logger.info("\n" + "=" * 80)
    logger.info("ETL COMPLET - TOUTES LES SOURCES")
    logger.info("=" * 80)
    
    # Extraction de toutes les sources
    sources = [
        extract_back_on_track(),
        extract_sncf_intercites(),
        extract_sncf_transilien(),
        extract_renfe(),
        extract_germany(),
        extract_trenitalia(),
    ]
    
    # Combinaison
    df_total = pd.concat([s for s in sources if not s.empty], ignore_index=True)
    
    # Statistiques complètes
    logger.info("\n" + "=" * 80)
    logger.info("STATISTIQUES GLOBALES - TOUTES SOURCES")
    logger.info("=" * 80)
    logger.info(f"TOTAL: {len(df_total):,} trains")
    logger.info(f"\nPar source:")
    for source in df_total['source'].unique():
        count = len(df_total[df_total['source'] == source])
        night = len(df_total[(df_total['source'] == source) & (df_total['classification'] == 'night')])
        logger.info(f"  {source:25s}: {count:6,} trains ({night:4,} nuit)")
    
    logger.info(f"\nPar pays:")
    for country in df_total['country'].unique():
        count = len(df_total[df_total['country'] == country])
        logger.info(f"  {country}: {count:,} trains")
    
    logger.info(f"\nPar classification:")
    for cls in df_total['classification'].unique():
        count = len(df_total[df_total['classification'] == cls])
        logger.info(f"  {cls}: {count:,} trains")
    
    logger.info(f"\nPar type de transport:")
    for ttype in df_total['train_type'].unique():
        count = len(df_total[df_total['train_type'] == ttype])
        logger.info(f"  {ttype}: {count:,} trains")
    
    logger.info(f"\nDurée moyenne: {df_total['duration_hours'].mean():.1f}h")
    logger.info(f"Durée médiane: {df_total['duration_hours'].median():.1f}h")
    
    # Export
    output_file = OUTPUT_DIR / 'obrail_all_sources_raw.csv'
    df_total.to_csv(output_file, index=False)
    logger.info(f"\n✓ Export: {output_file}")
    logger.info(f"  Dimensions: {df_total.shape}")
    
    # Aperçu
    logger.info("\nAperçu des données:")
    print(df_total[['source', 'country', 'train_number', 'duration_hours', 'classification']].head(10).to_string())
    
    return df_total

if __name__ == '__main__':
    os.chdir('/Users/soufianehamzaoui/Desktop/EPSI/MSPR/1/obrail-mspr')
    df = main()
