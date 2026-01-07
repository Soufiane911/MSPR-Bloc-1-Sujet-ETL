#!/usr/bin/env python3
"""
Script de consolidation Gold
Fusionne les données Silver (GTFS multiples + Back-on-Track) en un fichier unique "Flat File"
pour l'analyse BI.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from math import radians, cos, sin, asin, sqrt
import glob

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GoldConsolidator:
    def __init__(self, silver_dir, gold_dir):
        self.silver_dir = Path(silver_dir)
        self.gold_dir = Path(gold_dir)
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        
        # Facteurs d'émission (gCO2e/pkm) - Hypothèses
        self.emission_factors = {
            'TGV': 2.4,
            'Intercités': 5.8,
            'TER': 29.2,
            'Transilien': 4.7,
            'Train de nuit': 14.0, # Hypothèse mixte
            'Autocar': 30.0,
            'Inconnu': 10.0
        }

    def haversine_distance(self, lon1, lat1, lon2, lat2):
        """Calcul la distance grand cercle entre deux points en km"""
        # Convertir en radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # Formule
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        r = 6371 # Rayon terre km
        return c * r

    def process_gtfs_source(self, source_path):
        """Traite une source GTFS Silver et renvoie un DataFrame format Gold"""
        source_name = source_path.name
        logger.info(f"   🏗️  Traitement GTFS : {source_name}")
        
        try:
            # Charger les fichiers nécessaires
            trips = pd.read_csv(source_path / "trips.csv")
            routes = pd.read_csv(source_path / "routes.csv")
            agency = pd.read_csv(source_path / "agency.csv")
            stops = pd.read_csv(source_path / "stops.csv")
            stop_times = pd.read_csv(source_path / "stop_times.csv")
            
            # 1. Joindre Routes et Agency
            # routes.txt contient agency_id, agency.txt contient agency_id et agency_name
            if 'agency_id' in routes.columns and 'agency_id' in agency.columns:
                routes = routes.merge(agency[['agency_id', 'agency_name']], on='agency_id', how='left')
            else:
                # Si pas de agency_id, on prend le nom de la première agence
                default_agency = agency['agency_name'].iloc[0] if not agency.empty else source_name
                routes['agency_name'] = default_agency

            # 2. Joindre Trips et Routes
            trips_routes = trips.merge(routes, on='route_id', how='left')
            
            # Gestion du nom de route (long ou short)
            if 'route_long_name' in trips_routes.columns:
                # Si long_name est vide, utiliser short_name
                trips_routes['route_name'] = trips_routes['route_long_name'].fillna(trips_routes.get('route_short_name', 'Inconnu'))
            elif 'route_short_name' in trips_routes.columns:
                trips_routes['route_name'] = trips_routes['route_short_name']
            else:
                trips_routes['route_name'] = f"Route {trips_routes['route_id']}"
            
            # 3. Agréger stop_times pour avoir origine/destination par trip
            # On cherche le min sequence (départ) et max sequence (arrivée)
            # Optimisation : on ne garde que les colonnes utiles
            st_lite = stop_times[['trip_id', 'stop_id', 'stop_sequence', 'arrival_time', 'departure_time']]
            
            # Trier par trip et sequence
            st_lite = st_lite.sort_values(['trip_id', 'stop_sequence'])
            
            # Récupérer premier et dernier
            first_stops = st_lite.groupby('trip_id').first().reset_index()
            last_stops = st_lite.groupby('trip_id').last().reset_index()
            
            # Joindre les infos d'arrêts (nom, lat, lon)
            stops_map = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].set_index('stop_id')
            
            # Fonction helper pour enrichir
            def enrich_stops(df, prefix):
                df = df.merge(stops_map, on='stop_id', how='left')
                df = df.rename(columns={
                    'stop_name': f'{prefix}_stop_name',
                    'stop_lat': f'{prefix}_lat',
                    'stop_lon': f'{prefix}_lon',
                    'departure_time': f'{prefix}_time' if prefix == 'origin' else 'ignore_dep',
                    'arrival_time': f'{prefix}_time' if prefix == 'destination' else 'ignore_arr'
                })
                return df

            first_stops = enrich_stops(first_stops, 'origin')
            last_stops = enrich_stops(last_stops, 'destination')
            
            # Fusionner Origine et Destination
            trip_bounds = first_stops[['trip_id', 'origin_stop_name', 'origin_lat', 'origin_lon', 'origin_time']].merge(
                last_stops[['trip_id', 'destination_stop_name', 'destination_lat', 'destination_lon', 'destination_time']],
                on='trip_id'
            )
            
            # 4. Créer le Dataset final pour cette source
            gold_df = trips_routes.merge(trip_bounds, on='trip_id', how='inner')
            
            # 5. Calculs et Enrichissements
            
            # Distance
            gold_df['distance_km'] = gold_df.apply(
                lambda x: self.haversine_distance(x['origin_lon'], x['origin_lat'], x['destination_lon'], x['destination_lat']) 
                if pd.notnull(x['origin_lat']) and pd.notnull(x['destination_lat']) else 0,
                axis=1
            )
            
            # Durée (parsing simple des heures HH:MM:SS)
            # Attention : GTFS peut avoir des heures > 24:00:00
            def parse_gtfs_time(t_str):
                if pd.isna(t_str): return 0
                parts = t_str.split(':')
                return int(parts[0]) + int(parts[1])/60 + int(parts[2])/3600
                
            gold_df['duration_h'] = gold_df.apply(
                lambda x: parse_gtfs_time(x['destination_time']) - parse_gtfs_time(x['origin_time']),
                axis=1
            )
            
            # Type de train (Mapping simple basé sur route_type)
            # 0: Tram, 1: Métro, 2: Rail, 3: Bus
            type_map = {0: 'Tram', 1: 'Métro', 2: 'Train', 3: 'Bus', 11: 'Trolleybus', 7: 'Funiculaire'}
            gold_df['train_type'] = gold_df['route_type'].map(type_map).fillna('Autre')
            
            # Service Type (Nuit/Jour)
            gold_df['service_type'] = gold_df['origin_time'].apply(
                lambda x: 'Nuit' if (int(x.split(':')[0]) >= 21 or int(x.split(':')[0]) <= 5) else 'Jour'
            )
            
            # Émissions
            gold_df['emission_gco2e_pkm'] = gold_df['train_type'].map(
                lambda x: self.emission_factors.get('Autocar', 30) if x == 'Bus' else self.emission_factors.get('TER', 29.2)
            )
            # Affinage TGV
            if 'TGV' in source_name.upper():
                 gold_df['emission_gco2e_pkm'] = self.emission_factors['TGV']
                 gold_df['train_type'] = 'TGV'

            gold_df['total_emission_kgco2e'] = (gold_df['distance_km'] * gold_df['emission_gco2e_pkm']) / 1000
            
            # --- NOUVEAUX ENRICHISSEMENTS (KPIs CALCULÉS) ---
            
            # 1. Vitesse Commerciale Moyenne (km/h)
            # On évite la division par zéro
            gold_df['avg_speed_kmh'] = gold_df.apply(
                lambda x: round(x['distance_km'] / x['duration_h'], 1) if x['duration_h'] > 0 else 0,
                axis=1
            )
            
            # 2. Efficacité Carbone (kgCO2/heure)
            gold_df['carbon_efficiency_kg_h'] = gold_df.apply(
                lambda x: round(x['total_emission_kgco2e'] / x['duration_h'], 2) if x['duration_h'] > 0 else 0,
                axis=1
            )
            
            # 3. Green Score (Note sur 100)
            # Logique arbitraire pour l'exemple :
            # - On part de 100
            # - On perd 2 points par kg de CO2
            # - On gagne 10 points si c'est un train de nuit
            # - Minimum 0, Maximum 100
            def calculate_green_score(row):
                score = 100
                score -= (row['total_emission_kgco2e'] * 2)
                if row['service_type'] == 'Nuit':
                    score += 10
                return max(0, min(100, round(score)))
                
            gold_df['green_score'] = gold_df.apply(calculate_green_score, axis=1)

            # Colonnes finales
            gold_df['source_dataset'] = source_name
            gold_df['frequency_per_week'] = 7 # Simplification (faudrait lire calendar.txt)
            gold_df['origin_country'] = 'FR' # Par défaut pour SNCF, à améliorer avec lat/lon
            gold_df['destination_country'] = 'FR' 
            gold_df['traction'] = 'électrique' # Hypothèse

            # Sélection finale
            cols = [
                'trip_id', 'agency_name', 'route_name', 'train_type', 'service_type',
                'origin_stop_name', 'origin_lat', 'origin_lon', 'origin_country',
                'destination_stop_name', 'destination_lat', 'destination_lon', 'destination_country',
                'departure_time', 'arrival_time', 'distance_km', 'duration_h', 'avg_speed_kmh',
                'emission_gco2e_pkm', 'total_emission_kgco2e', 'carbon_efficiency_kg_h', 'green_score',
                'frequency_per_week', 'source_dataset', 'traction'
            ]
            
            # Renommer origin_time/destination_time en departure_time/arrival_time si ce n'est pas déjà fait
            gold_df = gold_df.rename(columns={'origin_time': 'departure_time', 'destination_time': 'arrival_time'})
            
            # Filtrer colonnes existantes
            final_cols = [c for c in cols if c in gold_df.columns]
            
            return gold_df[final_cols]

        except Exception as e:
            logger.error(f"❌ Erreur processing GTFS {source_name}: {e}")
            return pd.DataFrame()

    def process_backontrack(self, bot_dir):
        """Traite les données Back-on-Track Silver"""
        logger.info("   🌙 Traitement Back-on-Track")
        try:
            df = pd.read_csv(bot_dir / "night_trains.csv")
            
            # Mapping vers format Gold
            gold_df = pd.DataFrame()
            gold_df['trip_id'] = df['nighttrain']
            gold_df['agency_name'] = df['operators']
            gold_df['route_name'] = df['itinerary']
            gold_df['train_type'] = 'Train de nuit'
            gold_df['service_type'] = 'Nuit'
            
            # Parsing origine/destination depuis itinerary "Ville A - Ville B"
            # C'est fragile, mais c'est le mieux qu'on a
            def get_od(itin):
                if pd.isna(itin): return None, None
                parts = str(itin).split('–') # Tiret long souvent utilisé
                if len(parts) < 2: parts = str(itin).split('-')
                if len(parts) >= 2:
                    return parts[0].strip(), parts[-1].strip()
                return itin, itin
            
            od = df['itinerary'].apply(get_od)
            gold_df['origin_stop_name'] = [x[0] for x in od]
            gold_df['destination_stop_name'] = [x[1] for x in od]
            
            gold_df['origin_country'] = df['countries'].apply(lambda x: str(x).split(',')[0] if pd.notnull(x) else 'EU')
            gold_df['destination_country'] = df['countries'].apply(lambda x: str(x).split(',')[-1] if pd.notnull(x) else 'EU')
            
            gold_df['origin_lat'] = None
            gold_df['origin_lon'] = None
            gold_df['destination_lat'] = None
            gold_df['destination_lon'] = None
            gold_df['departure_time'] = None # Pas d'info
            gold_df['arrival_time'] = None
            gold_df['distance_km'] = 0 # Pas de coords
            gold_df['duration_h'] = 0
            gold_df['emission_gco2e_pkm'] = self.emission_factors['Train de nuit']
            gold_df['total_emission_kgco2e'] = 0
            
            # --- NOUVEAUX ENRICHISSEMENTS (Pour Back-on-Track) ---
            # Comme on n'a pas la distance/durée exacte, on met des valeurs par défaut ou NULL
            gold_df['avg_speed_kmh'] = 0
            gold_df['carbon_efficiency_kg_h'] = 0
            gold_df['green_score'] = 100 # Bonus max car c'est un train de nuit et on n'a pas le calcul précis
            
            gold_df['frequency_per_week'] = 0
            gold_df['source_dataset'] = 'Back-on-Track'
            gold_df['traction'] = 'mixte'
            
            return gold_df
            
        except Exception as e:
            logger.error(f"❌ Erreur processing Back-on-Track: {e}")
            return pd.DataFrame()

    def consolidate(self):
        """Exécute la consolidation"""
        logger.info("🔨 Démarrage de la consolidation Gold...")
        
        all_dfs = []
        
        # 1. Parcourir les dossiers GTFS Silver
        for source_dir in self.silver_dir.iterdir():
            if source_dir.is_dir() and source_dir.name != "back_on_track":
                df = self.process_gtfs_source(source_dir)
                if not df.empty:
                    all_dfs.append(df)
        
        # 2. Traiter Back-on-Track
        bot_dir = self.silver_dir / "back_on_track"
        if bot_dir.exists():
            df_bot = self.process_backontrack(bot_dir)
            if not df_bot.empty:
                all_dfs.append(df_bot)
        
        # 3. Fusionner
        if not all_dfs:
            logger.warning("⚠️  Aucune donnée à consolider")
            return
            
        final_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"   📊 Total lignes consolidées : {len(final_df)}")
        
        # 4. Sauvegarder
        output_path = self.gold_dir / "consolidated_trips.csv"
        final_df.to_csv(output_path, index=False)
        logger.info(f"   ✅ Fichier Gold généré : {output_path}")
        
        # Petit aperçu
        print("\n🔎 Aperçu des données Gold :")
        print(final_df[['agency_name', 'route_name', 'origin_stop_name', 'destination_stop_name']].head())

if __name__ == "__main__":
    BASE_DIR = Path(__file__).parent.parent
    SILVER_DIR = BASE_DIR / "data" / "processed" / "silver"
    GOLD_DIR = BASE_DIR / "data" / "processed" / "gold"
    
    consolidator = GoldConsolidator(SILVER_DIR, GOLD_DIR)
    consolidator.consolidate()
