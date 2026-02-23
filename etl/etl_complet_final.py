#!/usr/bin/env python3
"""
ETL COMPLET - TOUTES SOURCES + TOUTES LES COLONNES
Garde toute l'information pour analyse complète
"""

import pandas as pd
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

NOTE_SOURCES_DIR = Path('note/sources-data')
OUTPUT_DIR = Path('data/processed')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Facteurs CO2
CO2_FACTORS = {
    'tgv': 2.5, 'night_train': 5.0, 'intercites': 4.0,
    'regional': 8.0, 'plane_short': 220.0, 'plane_long': 150.0
}

def parse_time(time_str):
    try:
        parts = str(time_str).split(':')
        return int(parts[0]) * 60 + int(parts[1])
    except:
        return None

def calc_night_weight(dep_min, arr_min):
    if arr_min < dep_min:
        dur = (24*60 - dep_min) + arr_min
    else:
        dur = arr_min - dep_min
    if dur == 0:
        return 0, 0
    
    night_mins = 0
    NUIT_DEBUT, NUIT_FIN = 22*60, 6*60
    cur = dep_min
    for _ in range(int(dur)):
        if cur >= NUIT_DEBUT or cur < NUIT_FIN:
            night_mins += 1
        cur = (cur + 1) % (24*60)
    
    return (night_mins / dur) * 100, dur / 60

def classify_train(dep_time, arr_time, train_code="", has_couchette=False):
    dep = parse_time(dep_time)
    arr = parse_time(arr_time)
    if dep is None or arr is None:
        return 'unknown', 0, 'invalid_time', 0, 0
    
    poids, duree = calc_night_weight(dep, arr)
    
    # Exception: couchettes confirmées
    if has_couchette:
        return 'night', 1.0, 'couchette_confirmed', poids, duree
    
    # Exception: codes explicites
    if any(x in str(train_code).upper() for x in ['NJ', 'EN', 'NIGHT', 'SLEEPER', 'NATTÅG']):
        return 'night', 1.0, 'explicit_night_code', poids, duree
    
    # Filtre durée minimum pour nuit
    if duree < 4:
        return 'day', 0.95, 'too_short_for_night', poids, duree
    
    if poids >= 50:
        return 'night', 0.95, 'majority_night_time', poids, duree
    
    if poids >= 30 and duree >= 6:
        return 'night', 0.85, 'significant_night_long', poids, duree
    
    return 'day', 0.90, 'majority_day_time', poids, duree

def calc_co2(distance_km, train_category):
    if not distance_km or distance_km <= 0:
        return None, None, None
    
    factor = CO2_FACTORS.get(train_category, 10.0)
    emission_train = (distance_km * factor) / 1000
    
    plane_factor = CO2_FACTORS['plane_long'] if distance_km > 1500 else CO2_FACTORS['plane_short']
    emission_plane = (distance_km * plane_factor) / 1000
    
    eco_kg = emission_plane - emission_train
    eco_pct = (eco_kg / emission_plane) * 100
    
    return emission_train, eco_kg, eco_pct

# ============================================================
# EXTRACTIONS
# ============================================================

def extract_back_on_track():
    """1. Back-on-Track avec toutes les colonnes"""
    logger.info("1. Back-on-Track...")
    
    with open(NOTE_SOURCES_DIR / 'back-on-track-eu/data/latest/trips.json') as f:
        trips = json.load(f)
    with open(NOTE_SOURCES_DIR / 'back-on-track-eu/data/latest/agencies.json') as f:
        agencies = json.load(f)
    
    records = []
    for tid, trip in trips.items():
        try:
            dep_time = trip['origin_departure_time'].split('T')[1][:8]
            arr_time = trip['destination_arrival_time'].split('T')[1][:8]
            distance = float(trip.get('distance', 0)) if trip.get('distance') else 0
            
            classes = trip.get('classes', '').lower()
            has_couchette = 'sleeper' in classes or 'couchette' in classes
            
            cls, conf, rule, poids, duree = classify_train(dep_time, arr_time, 
                                                           trip.get('trip_short_name', ''), 
                                                           has_couchette)
            
            agency = agencies.get(trip.get('agency_id', {}), {})
            
            emission, eco_kg, eco_pct = calc_co2(distance, 'night_train')
            
            records.append({
                'train_id': tid,
                'train_number': trip.get('trip_short_name', ''),
                'operator': agency.get('agency_name', ''),
                'operator_country': agency.get('agency_state', ''),
                'train_type': 'night_train',
                'departure_station': trip.get('trip_origin', ''),
                'arrival_station': trip.get('trip_headsign', ''),
                'departure_time': dep_time,
                'arrival_time': arr_time,
                'duration_hours': round(duree, 2),
                'distance_km': distance,
                'night_percentage': round(poids, 1),
                'day_night_classification': cls,
                'confidence_score': conf,
                'classification_rule': rule,
                'co2_emission_kg': round(emission, 3) if emission else None,
                'co2_vs_plane_kg': round(eco_kg, 3) if eco_kg else None,
                'co2_saving_percent': round(eco_pct, 2) if eco_pct else None,
                'data_source': 'back_on_track',
                'has_couchette': has_couchette,
                'has_sleeper': 'sleeper' in classes,
                'route': f"{trip.get('trip_origin', '')} - {trip.get('trip_headsign', '')}",
            })
        except Exception as e:
            pass
    
    logger.info(f"   ✓ {len(records)} trains")
    return pd.DataFrame(records)

def extract_gtfs_complete(folder, source_name, country, operator_default, train_cat):
    """Extraction complète pour sources GTFS avec TOUTES les colonnes"""
    logger.info(f"{source_name}...")
    
    base = NOTE_SOURCES_DIR / folder
    
    # Lire tous les fichiers nécessaires
    stop_times = pd.read_csv(base / 'stop_times.txt', low_memory=False)
    trips = pd.read_csv(base / 'trips.txt', low_memory=False)
    routes = pd.read_csv(base / 'routes.txt', low_memory=False)
    
    # Agence si disponible
    agency = None
    if (base / 'agency.txt').exists():
        agency = pd.read_csv(base / 'agency.txt')
        agency_dict = dict(zip(agency['agency_id'], agency['agency_name'])) if 'agency_id' in agency.columns else {}
    else:
        agency_dict = {}
    
    # Calculer premier/dernier arrêt
    first_stops = stop_times.groupby('trip_id')['departure_time'].first()
    last_stops = stop_times.groupby('trip_id')['arrival_time'].last()
    
    records = []
    for _, trip in trips.iterrows():
        try:
            tid = trip['trip_id']
            if tid not in first_stops.index or tid not in last_stops.index:
                continue
            
            dep = first_stops[tid]
            arr = last_stops[tid]
            
            cls, conf, rule, poids, duree = classify_train(dep, arr, str(trip.get('trip_headsign', '')))
            
            # Info route
            route_info = routes[routes['route_id'] == trip['route_id']]
            if len(route_info) == 0:
                continue
            route = route_info.iloc[0]
            route_name = str(route.get('route_long_name', ''))
            
            # Stations (si disponible dans stops.txt)
            stops_str = route_name if ' - ' in route_name else f"{dep} - {arr}"
            dep_station = stops_str.split(' - ')[0] if ' - ' in stops_str else 'Unknown'
            arr_station = stops_str.split(' - ')[1] if ' - ' in stops_str and len(stops_str.split(' - ')) > 1 else 'Unknown'
            
            # Distance estimée
            distance = duree * 80  # Estimation
            
            # CO2
            emission, eco_kg, eco_pct = calc_co2(distance, train_cat)
            
            # Opérateur
            op = agency_dict.get(trip.get('agency_id', ''), operator_default)
            
            # Type de transport
            rtype = route.get('route_type', 2)
            transport_mode = {0: 'tram', 1: 'metro', 2: 'rail', 3: 'bus', 4: 'ferry'}.get(rtype, 'unknown')
            
            records.append({
                'train_id': tid,
                'train_number': str(trip.get('trip_headsign', ''))[:50],
                'operator': op[:100],
                'operator_country': country,
                'train_type': transport_mode,
                'departure_station': dep_station[:100],
                'arrival_station': arr_station[:100],
                'departure_time': dep,
                'arrival_time': arr,
                'duration_hours': round(duree, 2),
                'distance_km': round(distance, 2),
                'night_percentage': round(poids, 1),
                'day_night_classification': cls,
                'confidence_score': conf,
                'classification_rule': rule,
                'co2_emission_kg': round(emission, 3) if emission else None,
                'co2_vs_plane_kg': round(eco_kg, 3) if eco_kg else None,
                'co2_saving_percent': round(eco_pct, 2) if eco_pct else None,
                'data_source': source_name.lower().replace(' ', '_'),
                'has_couchette': False,
                'has_sleeper': False,
                'route': route_name[:200],
            })
        except:
            pass
    
    logger.info(f"   ✓ {len(records)} trains")
    return pd.DataFrame(records)

def extract_germany_chunked():
    """Allemagne avec traitement par chunks pour performance"""
    logger.info("Allemagne (optimisé)...")
    base = NOTE_SOURCES_DIR / 'gtfs-de'
    
    stop_times = pd.read_csv(base / 'stop_times.txt', usecols=['trip_id', 'departure_time', 'arrival_time'])
    trips = pd.read_csv(base / 'trips.txt')
    routes = pd.read_csv(base / 'routes.txt')
    
    first = stop_times.groupby('trip_id')['departure_time'].first()
    last = stop_times.groupby('trip_id')['arrival_time'].last()
    times_df = pd.DataFrame({'dep': first, 'arr': last}).reset_index()
    
    merged = trips.merge(times_df, on='trip_id').merge(routes[['route_id', 'route_long_name']], on='route_id')
    
    records = []
    for _, row in merged.iterrows():
        try:
            cls, conf, rule, poids, duree = classify_train(row['dep'], row['arr'])
            distance = duree * 70
            emission, eco_kg, eco_pct = calc_co2(distance, 'regional')
            
            records.append({
                'train_id': row['trip_id'],
                'train_number': str(row.get('trip_headsign', ''))[:50],
                'operator': 'DB/Regional',
                'operator_country': 'DE',
                'train_type': 'rail',
                'departure_station': 'Unknown',
                'arrival_station': str(row.get('route_long_name', ''))[:100],
                'departure_time': row['dep'],
                'arrival_time': row['arr'],
                'duration_hours': round(duree, 2),
                'distance_km': round(distance, 2),
                'night_percentage': round(poids, 1),
                'day_night_classification': cls,
                'confidence_score': conf,
                'classification_rule': rule,
                'co2_emission_kg': round(emission, 3) if emission else None,
                'co2_vs_plane_kg': round(eco_kg, 3) if eco_kg else None,
                'co2_saving_percent': round(eco_pct, 2) if eco_pct else None,
                'data_source': 'germany_gtfs',
                'has_couchette': False,
                'has_sleeper': False,
                'route': str(row.get('route_long_name', ''))[:200],
            })
        except:
            pass
    
    logger.info(f"   ✓ {len(records)} trains")
    return pd.DataFrame(records)

def main():
    logger.info("=" * 70)
    logger.info("ETL COMPLET - TOUTES SOURCES + TOUTES COLONNES")
    logger.info("=" * 70)
    
    # Extraire toutes les sources
    all_data = []
    
    all_data.append(extract_back_on_track())
    all_data.append(extract_gtfs_complete('sncf-intercites', 'SNCF Intercités', 'FR', 'SNCF', 'intercites'))
    all_data.append(extract_gtfs_complete('sncf-transilien', 'SNCF Transilien', 'FR', 'SNCF', 'transilien'))
    all_data.append(extract_gtfs_complete('renfe', 'Renfe', 'ES', 'RENFE', 'ave'))
    all_data.append(extract_gtfs_complete('trenitalia', 'Trenitalia', 'IT', 'Trenitalia', 'regional'))
    all_data.append(extract_germany_chunked())
    
    # Combiner
    df_total = pd.concat([df for df in all_data if not df.empty], ignore_index=True)
    
    # Stats
    logger.info("\n" + "=" * 70)
    logger.info("RÉSULTATS")
    logger.info("=" * 70)
    logger.info(f"Total: {len(df_total):,} trains")
    logger.info(f"Colonnes: {len(df_total.columns)}")
    logger.info(f"\nColonnes présentes: {list(df_total.columns)}")
    
    # Export
    output_file = OUTPUT_DIR / 'OBRAIL_COMPLETE_FINAL.csv'
    df_total.to_csv(output_file, index=False)
    logger.info(f"\n✅ Export: {output_file}")
    
    return df_total

if __name__ == '__main__':
    import os
    os.chdir('/Users/soufianehamzaoui/Desktop/EPSI/MSPR/1/obrail-mspr')
    df = main()
