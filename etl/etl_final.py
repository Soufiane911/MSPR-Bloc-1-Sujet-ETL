#!/usr/bin/env python3
"""
ETL Final ObRail Europe
Filtrage des trains longue distance + Classification poids temporel
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

# Seuils de filtrage
MIN_DURATION_HOURS = 2  # Minimum 2h pour être comparable à l'avion
MIN_DISTANCE_KM = 100   # Minimum 100km
MIN_NIGHT_PERCENTAGE = 50  # % minimum pour classer "nuit"
MIN_NIGHT_DURATION = 4  # Heures minimum pour un service de nuit

# Facteurs CO2 (g CO2e/km/passager)
CO2_FACTORS = {
    'tgv': 2.5,
    'intercites_elec': 4.0,
    'intercites_therm': 25.0,
    'night_train': 5.0,
    'regional_elec': 8.0,
    'regional_therm': 30.0,
    'plane_short': 220.0,  # < 1500km
    'plane_long': 150.0,   # > 1500km
}

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

def parse_time_to_minutes(time_str):
    """Convertit HH:MM:SS en minutes depuis minuit"""
    try:
        parts = str(time_str).split(':')
        return int(parts[0]) * 60 + int(parts[1])
    except:
        return None

def calculer_duree_heures(dep_min, arr_min):
    """Calcule la durée en heures (gestion passage minuit)"""
    if arr_min < dep_min:
        duree_min = (24 * 60 - dep_min) + arr_min
    else:
        duree_min = arr_min - dep_min
    return duree_min / 60

def calculer_poids_nocturne(dep_min, arr_min):
    """
    Calcule le pourcentage du trajet entre 22h (1320min) et 6h (360min)
    """
    NUIT_DEBUT = 22 * 60  # 1320
    NUIT_FIN = 6 * 60     # 360
    
    # Durée totale
    if arr_min < dep_min:
        duree_totale = (24 * 60 - dep_min) + arr_min
    else:
        duree_totale = arr_min - dep_min
    
    if duree_totale == 0:
        return 0, 0
    
    # Calcul temps dans la nuit
    temps_nuit = 0
    
    # Méthode: parcourir chaque minute (simplifié)
    current = dep_min
    while current != arr_min:
        # Vérifier si dans période nocturne
        if current >= NUIT_DEBUT or current < NUIT_FIN:
            temps_nuit += 1
        
        current = (current + 1) % (24 * 60)
        if current == dep_min:  # Sécurité boucle infinie
            break
    
    poids = (temps_nuit / duree_totale) * 100
    return poids, duree_totale / 60

def classify_train_poids_temporel(dep_time, arr_time, train_code="", has_couchette=False):
    """
    Classification finale basée sur poids temporel
    """
    dep_min = parse_time_to_minutes(dep_time)
    arr_min = parse_time_to_minutes(arr_time)
    
    if dep_min is None or arr_min is None:
        return 'unknown', 0, 'invalid_time', 0, 0
    
    poids_nuit, duree = calculer_poids_nocturne(dep_min, arr_min)
    
    # Exception: Train avec couchettes confirmé
    if has_couchette:
        return 'night', 1.0, 'couchette_confirmed', poids_nuit, duree
    
    # Exception: Code explicite de nuit
    code_upper = str(train_code).upper()
    if any(x in code_upper for x in ['NJ', 'EN', 'NIGHT', 'SLEEPER', 'NATTÅG']):
        return 'night', 1.0, 'explicit_night_code', poids_nuit, duree
    
    # Filtre durée: minimum 4h pour être un "train de nuit"
    if duree < MIN_NIGHT_DURATION:
        return 'day', 0.95, 'too_short_for_night', poids_nuit, duree
    
    # Règle principale: > 50% du temps en nocturne
    if poids_nuit >= MIN_NIGHT_PERCENTAGE:
        return 'night', 0.95, 'majority_night_time', poids_nuit, duree
    
    # Seconde règle: 30-50% nocturne + durée longue (> 6h)
    if poids_nuit >= 30 and duree >= 6:
        return 'night', 0.85, 'significant_night_long', poids_nuit, duree
    
    # Sinon: Jour
    return 'day', 0.90, 'majority_day_time', poids_nuit, duree

def calculer_emissions_co2(distance_km, train_type):
    """Calcule les émissions CO2 et l'économie vs avion"""
    if not distance_km or distance_km <= 0:
        return None, None, None
    
    # Choisir le facteur approprié
    if train_type in ['tgv', 'ave']:
        factor = CO2_FACTORS['tgv']
    elif train_type in ['night_train', 'nightjet']:
        factor = CO2_FACTORS['night_train']
    elif train_type in ['intercites', 'ic', 'ec']:
        factor = CO2_FACTORS['intercites_elec']
    else:
        factor = CO2_FACTORS['regional_elec']
    
    # Calcul émissions
    emission_train = (distance_km * factor) / 1000  # kg CO2
    
    # Avion (court ou long courrier)
    plane_factor = CO2_FACTORS['plane_long'] if distance_km > 1500 else CO2_FACTORS['plane_short']
    emission_plane = (distance_km * plane_factor) / 1000  # kg CO2
    
    # Économie
    economie_kg = emission_plane - emission_train
    economie_percent = (economie_kg / emission_plane) * 100
    ratio = emission_plane / emission_train
    
    return emission_train, economie_kg, economie_percent

# ============================================================
# EXTRACTEURS PAR SOURCE
# ============================================================

def extract_back_on_track():
    """Extrait et transforme Back-on-Track"""
    logger.info("Extraction Back-on-Track...")
    
    data_path = Path('note/sources-data/back-on-track-eu/data/latest')
    
    with open(data_path / 'trips.json') as f:
        trips = json.load(f)
    
    with open(data_path / 'agencies.json') as f:
        agencies = json.load(f)
    
    records = []
    for trip_id, trip in trips.items():
        try:
            # Parser heures
            dep_time = trip['origin_departure_time'].split('T')[1][:8]
            arr_time = trip['destination_arrival_time'].split('T')[1][:8]
            
            # Distance
            distance = float(trip.get('distance', 0))
            
            # Vérifier couchette
            classes = trip.get('classes', '').lower()
            has_couchette = 'sleeper' in classes or 'couchette' in classes
            
            # Classification
            type_jn, conf, rule, poids, duree = classify_train_poids_temporel(
                dep_time, arr_time, trip.get('trip_short_name', ''), has_couchette
            )
            
            # Filtre longue distance
            if duree < MIN_DURATION_HOURS and distance < MIN_DISTANCE_KM:
                continue
            
            # CO2
            emission, eco_kg, eco_pct = calculer_emissions_co2(distance, 'night_train')
            
            # Opérateur
            agency_id = trip.get('agency_id', '')
            agency_name = agencies.get(agency_id, {}).get('agency_name', agency_id)
            
            records.append({
                'train_id': trip_id,
                'train_number': trip.get('trip_short_name', ''),
                'operator': agency_name,
                'operator_country': agencies.get(agency_id, {}).get('agency_state', ''),
                'train_type': 'night_train',
                'departure_station': trip.get('trip_origin', ''),
                'arrival_station': trip.get('trip_headsign', ''),
                'departure_time': dep_time,
                'arrival_time': arr_time,
                'duration_hours': duree,
                'distance_km': distance,
                'night_percentage': poids,
                'day_night_classification': type_jn,
                'confidence_score': conf,
                'classification_rule': rule,
                'co2_emission_kg': emission,
                'co2_vs_plane_kg': eco_kg,
                'co2_saving_percent': eco_pct,
                'data_source': 'back_on_track',
                'has_couchette': has_couchette,
                'has_sleeper': 'sleeper' in classes,
            })
        except Exception as e:
            logger.warning(f"Erreur traitement {trip_id}: {e}")
    
    logger.info(f"Back-on-Track: {len(records)} trains extraits")
    return pd.DataFrame(records)

def extract_sncf_intercites():
    """Extrait SNCF Intercités"""
    logger.info("Extraction SNCF Intercités...")
    
    base_path = Path('note/sources-data/sncf-intercites')
    
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
            
            dep_time = first['departure_time']
            arr_time = last['arrival_time']
            
            # Classification
            type_jn, conf, rule, poids, duree = classify_train_poids_temporel(dep_time, arr_time)
            
            # Filtre longue distance
            if duree < MIN_DURATION_HOURS:
                continue
            
            # Route info
            route = routes[routes['route_id'] == trip['route_id']].iloc[0]
            
            # Estimation distance (approximation)
            distance = duree * 80  # ~80 km/h moyenne Intercités
            
            # CO2
            train_type = 'intercites'
            if 'night' in rule or type_jn == 'night':
                train_type = 'night_train'
            emission, eco_kg, eco_pct = calculer_emissions_co2(distance, train_type)
            
            records.append({
                'train_id': trip['trip_id'],
                'train_number': route['route_short_name'],
                'operator': 'SNCF VOYAGEURS',
                'operator_country': 'FR',
                'train_type': train_type,
                'departure_station': route['route_long_name'].split(' - ')[0] if ' - ' in route['route_long_name'] else '',
                'arrival_station': route['route_long_name'].split(' - ')[1] if ' - ' in route['route_long_name'] else '',
                'departure_time': dep_time,
                'arrival_time': arr_time,
                'duration_hours': duree,
                'distance_km': distance,
                'night_percentage': poids,
                'day_night_classification': type_jn,
                'confidence_score': conf,
                'classification_rule': rule,
                'co2_emission_kg': emission,
                'co2_vs_plane_kg': eco_kg,
                'co2_saving_percent': eco_pct,
                'data_source': 'sncf_intercites',
                'has_couchette': False,
                'has_sleeper': False,
            })
        except Exception as e:
            continue
    
    logger.info(f"SNCF Intercités: {len(records)} trains extraits")
    return pd.DataFrame(records)

def extract_renfe():
    """Extrait Renfe Espagne"""
    logger.info("Extraction Renfe...")
    
    base_path = Path('note/sources-data/renfe')
    
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
            
            dep_time = first['departure_time']
            arr_time = last['arrival_time']
            
            # Classification
            type_jn, conf, rule, poids, duree = classify_train_poids_temporel(dep_time, arr_time)
            
            # Filtre longue distance
            if duree < MIN_DURATION_HOURS:
                continue
            
            # Route
            route = routes[routes['route_id'] == trip['route_id']].iloc[0]
            
            # Détecter type AVE/TGV
            route_name = str(route.get('route_long_name', ''))
            if 'AVE' in route_name:
                train_type = 'tgv'
            else:
                train_type = 'intercites'
            
            # Distance estimée
            distance = duree * 90  # ~90 km/h pour AVE
            
            # CO2
            emission, eco_kg, eco_pct = calculer_emissions_co2(distance, train_type)
            
            records.append({
                'train_id': trip['trip_id'],
                'train_number': trip['trip_headsign'],
                'operator': 'RENFE OPERADORA',
                'operator_country': 'ES',
                'train_type': train_type,
                'departure_station': '',
                'arrival_station': route_name[:50],
                'departure_time': dep_time,
                'arrival_time': arr_time,
                'duration_hours': duree,
                'distance_km': distance,
                'night_percentage': poids,
                'day_night_classification': type_jn,
                'confidence_score': conf,
                'classification_rule': rule,
                'co2_emission_kg': emission,
                'co2_vs_plane_kg': eco_kg,
                'co2_saving_percent': eco_pct,
                'data_source': 'renfe',
                'has_couchette': False,
                'has_sleeper': False,
            })
        except:
            continue
    
    logger.info(f"Renfe: {len(records)} trains extraits")
    return pd.DataFrame(records)

# ============================================================
# FONCTION PRINCIPALE
# ============================================================

def run_etl():
    """Exécute l'ETL complet"""
    logger.info("=" * 70)
    logger.info("DÉMARRAGE ETL OBRAIL EUROPE")
    logger.info("=" * 70)
    
    # Extraction
    df_bot = extract_back_on_track()
    df_ic = extract_sncf_intercites()
    df_renfe = extract_renfe()
    
    # Combinaison
    df_final = pd.concat([df_bot, df_ic, df_renfe], ignore_index=True)
    
    # Statistiques
    logger.info("\n" + "=" * 70)
    logger.info("STATISTIQUES FINALES")
    logger.info("=" * 70)
    
    logger.info(f"Total trains: {len(df_final)}")
    logger.info(f"Trains de nuit: {len(df_final[df_final['day_night_classification'] == 'night'])}")
    logger.info(f"Trains de jour: {len(df_final[df_final['day_night_classification'] == 'day'])}")
    
    logger.info("\nRépartition par source:")
    for source in df_final['data_source'].unique():
        count = len(df_final[df_final['data_source'] == source])
        night = len(df_final[(df_final['data_source'] == source) & (df_final['day_night_classification'] == 'night')])
        logger.info(f"  {source}: {count} trains ({night} nuit)")
    
    logger.info("\nRépartition par pays:")
    for country in df_final['operator_country'].unique():
        count = len(df_final[df_final['operator_country'] == country])
        logger.info(f"  {country}: {count} trains")
    
    # Export
    output_dir = Path('data/processed')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / 'obrail_trains_final.csv'
    df_final.to_csv(output_file, index=False)
    logger.info(f"\nExport: {output_file}")
    logger.info(f"Dimensions: {df_final.shape}")
    
    # Exemple
    logger.info("\nExemple de données:")
    print(df_final.head(3).to_string())
    
    return df_final

if __name__ == '__main__':
    os.chdir('/Users/soufianehamzaoui/Desktop/EPSI/MSPR/1/obrail-mspr')
    df = run_etl()
