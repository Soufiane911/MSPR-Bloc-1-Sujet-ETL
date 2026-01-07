#!/usr/bin/env python3
"""
Script de transformation des données GTFS SNCF
Nettoie, valide et transforme les fichiers GTFS pour la base de données
"""

import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GTFSTransformer:
    """Classe pour transformer les données GTFS"""
    
    def __init__(self, gtfs_dir, processed_dir, source_name="unknown"):
        """
        Initialise le transformateur pour une source GTFS donnée
        
        Args:
            gtfs_dir: Chemin vers le dossier contenant les .txt
            processed_dir: Chemin vers le dossier de sortie (Silver)
            source_name: Nom de la source (ex: 'sncf', 'eurostar') pour le traçage
        """
        self.gtfs_dir = Path(gtfs_dir)
        self.processed_dir = Path(processed_dir)
        self.source_name = source_name
        
        # Créer le dossier processed s'il n'existe pas
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"[{self.source_name}] 📁 Source : {self.gtfs_dir}")
        logger.info(f"[{self.source_name}] 📁 Cible : {self.processed_dir}")
    
    def transform_agency(self):
        """
        Transforme agency.txt (le fichier le plus simple)
        Pas de nettoyage nécessaire, juste validation
        """
        logger.info("🔄 Transformation de agency.txt...")
        
        filepath = self.gtfs_dir / "agency.txt"
        if not filepath.exists():
            logger.warning(f"⚠️  Fichier introuvable : {filepath}")
            return None
        
        # Lire le fichier
        df = pd.read_csv(filepath)
        logger.info(f"   📊 {len(df)} agencies chargées")
        
        # Validation : vérifier qu'il n'y a pas de valeurs manquantes
        missing = df.isna().sum()
        if missing.sum() > 0:
            logger.warning(f"   ⚠️  Valeurs manquantes détectées :\n{missing[missing > 0]}")
        
        # Sauvegarder
        output_path = self.processed_dir / "agency.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df)} lignes)")
        
        return df
    
    def transform_routes(self):
        """
        Transforme routes.txt
        - Supprime les colonnes vides (route_desc, route_url à 100% NULL)
        - Gère les valeurs NULL pour route_color et route_text_color
        """
        logger.info("🔄 Transformation de routes.txt...")
        
        filepath = self.gtfs_dir / "routes.txt"
        if not filepath.exists():
            logger.warning(f"⚠️  Fichier introuvable : {filepath}")
            return None
        
        # Lire le fichier
        df = pd.read_csv(filepath)
        logger.info(f"   📊 {len(df)} routes chargées")
        
        # Supprimer les colonnes vides (100% NULL)
        cols_to_drop = []
        for col in df.columns:
            null_pct = (df[col].isna().sum() / len(df)) * 100
            if null_pct == 100.0:
                cols_to_drop.append(col)
                logger.info(f"   🗑️  Suppression colonne vide : {col}")
        
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        
        # Gérer les valeurs NULL pour route_color et route_text_color
        # On peut les laisser en NULL (c'est OK pour PostgreSQL)
        if 'route_color' in df.columns:
            null_count = df['route_color'].isna().sum()
            if null_count > 0:
                logger.info(f"   ℹ️  {null_count} routes sans couleur (OK, sera NULL en DB)")
        
        if 'route_text_color' in df.columns:
            null_count = df['route_text_color'].isna().sum()
            if null_count > 0:
                logger.info(f"   ℹ️  {null_count} routes sans couleur texte (OK, sera NULL en DB)")
        
        # Sauvegarder
        output_path = self.processed_dir / "routes.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df)} lignes, {len(df.columns)} colonnes)")
        
        return df
    
    def transform_stops(self):
        """
        Transforme stops.txt
        - Supprime les colonnes vides (stop_desc, zone_id, stop_url à 100% NULL)
        - Gère parent_station (peut être NULL, c'est normal)
        """
        logger.info("🔄 Transformation de stops.txt...")
        
        filepath = self.gtfs_dir / "stops.txt"
        if not filepath.exists():
            logger.warning(f"⚠️  Fichier introuvable : {filepath}")
            return None
        
        # Lire le fichier
        df = pd.read_csv(filepath)
        logger.info(f"   📊 {len(df)} stops chargés")
        
        # Supprimer les colonnes vides (100% NULL)
        cols_to_drop = []
        for col in df.columns:
            null_pct = (df[col].isna().sum() / len(df)) * 100
            if null_pct == 100.0:
                cols_to_drop.append(col)
                logger.info(f"   🗑️  Suppression colonne vide : {col}")
        
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        
        # Gérer parent_station (peut être NULL, c'est normal pour les StopArea)
        if 'parent_station' in df.columns:
            null_count = df['parent_station'].isna().sum()
            if null_count > 0:
                logger.info(f"   ℹ️  {null_count} stops sans parent_station (OK, ce sont des StopArea)")
        
        # Vérifier les coordonnées (ne doivent pas être NULL)
        if 'stop_lat' in df.columns:
            null_lat = df['stop_lat'].isna().sum()
            if null_lat > 0:
                logger.warning(f"   ⚠️  {null_lat} stops sans latitude !")
        
        if 'stop_lon' in df.columns:
            null_lon = df['stop_lon'].isna().sum()
            if null_lon > 0:
                logger.warning(f"   ⚠️  {null_lon} stops sans longitude !")
        
        # Sauvegarder
        output_path = self.processed_dir / "stops.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df)} lignes, {len(df.columns)} colonnes)")
        
        return df
    
    def transform_trips(self):
        """
        Transforme trips.txt
        - Convertit trip_headsign de int64 en string
        - Gère direction_id NULL (1.13% des cas)
        - Supprime shape_id (100% NULL)
        """
        logger.info("🔄 Transformation de trips.txt...")
        
        filepath = self.gtfs_dir / "trips.txt"
        if not filepath.exists():
            logger.warning(f"⚠️  Fichier introuvable : {filepath}")
            return None
        
        # Lire le fichier
        df = pd.read_csv(filepath)
        logger.info(f"   📊 {len(df)} trips chargés")
        
        # Supprimer shape_id (100% NULL)
        if 'shape_id' in df.columns:
            null_pct = (df['shape_id'].isna().sum() / len(df)) * 100
            if null_pct == 100.0:
                df = df.drop(columns=['shape_id'])
                logger.info(f"   🗑️  Suppression colonne vide : shape_id")
        
        # Convertir trip_headsign de int64 en string
        if 'trip_headsign' in df.columns:
            if df['trip_headsign'].dtype in ['int64', 'float64']:
                df['trip_headsign'] = df['trip_headsign'].astype(str)
                logger.info(f"   🔄 Conversion trip_headsign : int64 → string")
        
        # Gérer direction_id NULL
        if 'direction_id' in df.columns:
            null_count = df['direction_id'].isna().sum()
            if null_count > 0:
                logger.info(f"   ℹ️  {null_count} trips sans direction_id (OK, sera NULL en DB)")
        
        # Sauvegarder
        output_path = self.processed_dir / "trips.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df)} lignes, {len(df.columns)} colonnes)")
        
        return df
    
    def transform_calendar_dates(self):
        """
        Transforme calendar_dates.txt
        - Convertit date de int64 (YYYYMMDD) en format DATE lisible
        """
        logger.info("🔄 Transformation de calendar_dates.txt...")
        
        filepath = self.gtfs_dir / "calendar_dates.txt"
        if not filepath.exists():
            logger.warning(f"⚠️  Fichier introuvable : {filepath}")
            return None
        
        # Lire le fichier
        df = pd.read_csv(filepath)
        logger.info(f"   📊 {len(df)} calendar_dates chargés")
        
        # Convertir date de int64 (YYYYMMDD) en string format YYYY-MM-DD
        if 'date' in df.columns:
            if df['date'].dtype in ['int64', 'float64']:
                # Convertir en string puis formater
                df['date'] = df['date'].astype(str).str[:4] + '-' + \
                            df['date'].astype(str).str[4:6] + '-' + \
                            df['date'].astype(str).str[6:8]
                logger.info(f"   🔄 Conversion date : int64 (YYYYMMDD) → string (YYYY-MM-DD)")
        
        # Sauvegarder
        output_path = self.processed_dir / "calendar_dates.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df)} lignes, {len(df.columns)} colonnes)")
        
        return df
    
    def transform_stop_times(self, chunk_size=50000):
        """
        Transforme stop_times.txt (GROS FICHIER - 429k lignes)
        - Utilise des chunks pour optimiser la mémoire
        - Supprime les colonnes vides (stop_headsign, shape_dist_traveled)
        - Convertit les heures en format TIME
        """
        logger.info("🔄 Transformation de stop_times.txt (avec chunks)...")
        
        filepath = self.gtfs_dir / "stop_times.txt"
        if not filepath.exists():
            logger.warning(f"⚠️  Fichier introuvable : {filepath}")
            return None
        
        # Lire l'en-tête pour vérifier les colonnes disponibles
        first_row = pd.read_csv(filepath, nrows=0)
        available_cols = first_row.columns.tolist()
        
        # Colonnes désirées
        desired_cols = [
            'trip_id', 'arrival_time', 'departure_time', 
            'stop_id', 'stop_sequence', 'pickup_type', 'drop_off_type'
        ]
        
        # Intersection
        useful_cols = [c for c in desired_cols if c in available_cols]
        
        # Lire par chunks
        chunks = []
        total_rows = 0
        
        logger.info(f"   📖 Lecture par chunks de {chunk_size} lignes...")
        
        for chunk in tqdm(pd.read_csv(filepath, chunksize=chunk_size, usecols=useful_cols),
                         desc="   Traitement"):
            total_rows += len(chunk)
            chunks.append(chunk)
        
        logger.info(f"   📊 {total_rows} stop_times chargés en {len(chunks)} chunks")
        
        # Concaténer tous les chunks
        logger.info("   🔗 Concaténation des chunks...")
        df = pd.concat(chunks, ignore_index=True)
        
        # Les heures sont déjà en format HH:MM:SS, on peut les garder telles quelles
        # (PostgreSQL acceptera ce format pour le type TIME)
        
        # Sauvegarder
        output_path = self.processed_dir / "stop_times.csv"
        logger.info(f"   💾 Sauvegarde en cours...")
        df.to_csv(output_path, index=False)
        logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df)} lignes, {len(df.columns)} colonnes)")
        
        return df


def main():
    """Fonction principale - TOUTES LES ÉTAPES GTFS"""
    print("="*60)
    print("🔄 TRANSFORMATION GTFS - TOUTES LES ÉTAPES")
    print("="*60)
    print("📋 Fichiers à transformer :")
    print("   1. agency.txt (simple, validation)")
    print("   2. routes.txt (nettoyage colonnes vides)")
    print("   3. stops.txt (nettoyage, gestion parent_station)")
    print("   4. trips.txt (conversion trip_headsign)")
    print("   5. calendar_dates.txt (conversion date)")
    print("   6. stop_times.txt (GROS FICHIER - avec chunks)")
    print("="*60)
    
    # Chemins
    BASE_DIR = Path(__file__).parent.parent
    RAW_DATA_DIR = BASE_DIR / "data" / "raw"
    PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
    
    # Créer le transformateur
    transformer = GTFSTransformer(RAW_DATA_DIR, PROCESSED_DATA_DIR)
    
    # ÉTAPE 1 : Agency et Routes
    print("\n" + "="*60)
    print("📦 ÉTAPE 1 : Agency et Routes")
    print("="*60)
    agency_df = transformer.transform_agency()
    routes_df = transformer.transform_routes()
    
    # ÉTAPE 2 : Stops et Trips
    print("\n" + "="*60)
    print("📦 ÉTAPE 2 : Stops et Trips")
    print("="*60)
    stops_df = transformer.transform_stops()
    trips_df = transformer.transform_trips()
    
    # ÉTAPE 3 : Calendar Dates et Stop Times
    print("\n" + "="*60)
    print("📦 ÉTAPE 3 : Calendar Dates et Stop Times")
    print("="*60)
    calendar_df = transformer.transform_calendar_dates()
    stop_times_df = transformer.transform_stop_times()
    
    print("\n" + "="*60)
    print("✅ TOUTES LES ÉTAPES GTFS TERMINÉES !")
    print("="*60)
    print("\n📊 Résumé :")
    if agency_df is not None:
        print(f"   - Agencies : {len(agency_df):,} lignes")
    if routes_df is not None:
        print(f"   - Routes : {len(routes_df):,} lignes")
    if stops_df is not None:
        print(f"   - Stops : {len(stops_df):,} lignes")
    if trips_df is not None:
        print(f"   - Trips : {len(trips_df):,} lignes")
    if calendar_df is not None:
        print(f"   - Calendar Dates : {len(calendar_df):,} lignes")
    if stop_times_df is not None:
        print(f"   - Stop Times : {len(stop_times_df):,} lignes")
    print(f"\n📁 Fichiers sauvegardés dans : {PROCESSED_DATA_DIR}")
    print("\n➡️  Prochaine étape : Transformer Back-on-Track")


if __name__ == "__main__":
    main()

