#!/usr/bin/env python3
"""
Script de transformation des données Back-on-Track (Trains de nuit)
Nettoie et normalise les données des trains de nuit européens
"""

import pandas as pd
import json
import re
from pathlib import Path
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BackOnTrackTransformer:
    """Classe pour transformer les données Back-on-Track"""
    
    def __init__(self, json_file_path, processed_dir):
        """
        Initialise le transformateur
        
        Args:
            json_file_path: Chemin vers le fichier JSON
            processed_dir: Chemin vers le dossier de sortie (Silver)
        """
        self.json_path = Path(json_file_path)
        self.processed_dir = Path(processed_dir)
        
        # Créer le dossier processed s'il n'existe pas
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📁 Source JSON : {self.json_path}")
        logger.info(f"📁 Cible : {self.processed_dir}")
    
    def parse_html_itinerary(self, itinerary_long):
        """
        Parse le champ itinerarylong qui contient du HTML
        Extrait les gares de chaque direction
        
        Args:
            itinerary_long: String avec HTML (<br><br>)
        
        Returns:
            dict avec 'direction_1' et 'direction_2' (listes de gares)
        """
        if pd.isna(itinerary_long) or not itinerary_long:
            return {'direction_1': [], 'direction_2': []}
        
        # Séparer par <br><br> (séparateur entre les deux directions)
        parts = re.split(r'<br><br>', str(itinerary_long))
        
        directions = []
        for part in parts:
            # Extraire le numéro de train et les gares
            # Format: "IR 11502: Gare1 - Gare2 - Gare3"
            match = re.match(r'^(\w+\s*\d+):\s*(.+)$', part.strip())
            if match:
                train_number = match.group(1)
                stations_str = match.group(2)
                # Séparer les gares par " - "
                stations = [s.strip() for s in stations_str.split(' - ')]
                directions.append({
                    'train_number': train_number,
                    'stations': stations
                })
        
        # Retourner les deux directions
        result = {
            'direction_1': directions[0]['stations'] if len(directions) > 0 else [],
            'direction_2': directions[1]['stations'] if len(directions) > 1 else []
        }
        
        return result
    
    def normalize_countries(self, countries_str):
        """
        Normalise la colonne countries (peut contenir plusieurs pays)
        
        Args:
            countries_str: String avec pays séparés par virgules
        
        Returns:
            Liste de codes pays
        """
        if pd.isna(countries_str) or not countries_str:
            return []
        
        # Séparer par virgule et nettoyer
        countries = [c.strip() for c in str(countries_str).split(',')]
        return countries
    
    def normalize_operators(self, operators_str):
        """
        Normalise la colonne operators (peut contenir plusieurs opérateurs)
        
        Args:
            operators_str: String avec opérateurs séparés par virgules
        
        Returns:
            Liste d'opérateurs
        """
        if pd.isna(operators_str) or not operators_str:
            return []
        
        # Séparer par virgule et nettoyer
        operators = [o.strip() for o in str(operators_str).split(',')]
        return operators
    
    def transform_night_trains(self):
        """
        Transforme les données des trains de nuit
        - Parse itinerarylong (HTML)
        - Normalise countries et operators
        - Crée des tables de liaison pour les relations many-to-many
        """
        logger.info("🔄 Transformation de night_trains...")
        
        # Lire le JSON
        if not self.json_path.exists():
            logger.warning(f"⚠️  Fichier introuvable : {self.json_path}")
            return None
        
        try:
            df = pd.read_json(self.json_path)
            logger.info(f"   📊 {len(df)} trains de nuit chargés")
        except ValueError as e:
            logger.error(f"   ❌ Erreur de lecture JSON : {e}")
            return None
        
        # Créer une copie pour les transformations
        df_clean = df.copy()
        
        # Parser itinerarylong et créer des colonnes supplémentaires
        logger.info("   🔄 Parsing de itinerarylong...")
        parsed_itineraries = df_clean['itinerarylong'].apply(self.parse_html_itinerary)
        
        # Ajouter les colonnes avec les gares
        df_clean['direction_1_stations'] = parsed_itineraries.apply(lambda x: x['direction_1'])
        df_clean['direction_2_stations'] = parsed_itineraries.apply(lambda x: x['direction_2'])
        df_clean['num_stations_direction_1'] = df_clean['direction_1_stations'].apply(len)
        df_clean['num_stations_direction_2'] = df_clean['direction_2_stations'].apply(len)
        
        # Normaliser countries et operators
        logger.info("   🔄 Normalisation de countries et operators...")
        df_clean['countries_list'] = df_clean['countries'].apply(self.normalize_countries)
        df_clean['operators_list'] = df_clean['operators'].apply(self.normalize_operators)
        df_clean['num_countries'] = df_clean['countries_list'].apply(len)
        df_clean['num_operators'] = df_clean['operators_list'].apply(len)
        
        # Garder les colonnes principales pour la table principale
        main_cols = [
            'nighttrain', 'routelongname', 'itinerary', 'routeid',
            'itinerarylong', 'countries', 'operators', 'source',
            'num_stations_direction_1', 'num_stations_direction_2',
            'num_countries', 'num_operators'
        ]
        df_main = df_clean[main_cols].copy()
        
        # Sauvegarder la table principale
        output_path = self.processed_dir / "night_trains.csv"
        df_main.to_csv(output_path, index=False)
        logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df_main)} lignes)")
        
        # Créer les tables de liaison pour les relations many-to-many
        
        # Table night_train_countries
        logger.info("   🔄 Création de la table night_train_countries...")
        countries_rows = []
        for idx, row in df_clean.iterrows():
            routeid = row['routeid']
            for country in row['countries_list']:
                countries_rows.append({
                    'routeid': routeid,
                    'country': country
                })
        df_countries = pd.DataFrame(countries_rows)
        if len(df_countries) > 0:
            output_path = self.processed_dir / "night_train_countries.csv"
            df_countries.to_csv(output_path, index=False)
            logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df_countries)} lignes)")
        
        # Table night_train_operators
        logger.info("   🔄 Création de la table night_train_operators...")
        operators_rows = []
        for idx, row in df_clean.iterrows():
            routeid = row['routeid']
            for operator in row['operators_list']:
                operators_rows.append({
                    'routeid': routeid,
                    'operator': operator
                })
        df_operators = pd.DataFrame(operators_rows)
        if len(df_operators) > 0:
            output_path = self.processed_dir / "night_train_operators.csv"
            df_operators.to_csv(output_path, index=False)
            logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df_operators)} lignes)")
        
        # Table night_train_stations (gares par direction)
        logger.info("   🔄 Création de la table night_train_stations...")
        stations_rows = []
        for idx, row in df_clean.iterrows():
            routeid = row['routeid']
            # Direction 1
            for seq, station in enumerate(row['direction_1_stations'], 1):
                stations_rows.append({
                    'routeid': routeid,
                    'direction': 1,
                    'sequence': seq,
                    'station': station
                })
            # Direction 2
            for seq, station in enumerate(row['direction_2_stations'], 1):
                stations_rows.append({
                    'routeid': routeid,
                    'direction': 2,
                    'sequence': seq,
                    'station': station
                })
        df_stations = pd.DataFrame(stations_rows)
        if len(df_stations) > 0:
            output_path = self.processed_dir / "night_train_stations.csv"
            df_stations.to_csv(output_path, index=False)
            logger.info(f"   ✅ Sauvegardé : {output_path} ({len(df_stations)} lignes)")
        
        return df_main, df_countries, df_operators, df_stations


def main():
    """Fonction principale"""
    print("="*60)
    print("🌙 TRANSFORMATION BACK-ON-TRACK")
    print("="*60)
    print("📋 Transformation des trains de nuit européens")
    print("="*60)
    
    # Chemins
    BASE_DIR = Path(__file__).parent.parent
    RAW_DATA_DIR = BASE_DIR / "data" / "raw"
    PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
    
    # Créer le transformateur
    transformer = BackOnTrackTransformer(RAW_DATA_DIR, PROCESSED_DATA_DIR)
    
    # Transformer
    print("\n" + "="*60)
    results = transformer.transform_night_trains()
    
    if results:
        df_main, df_countries, df_operators, df_stations = results
        
        print("\n" + "="*60)
        print("✅ TRANSFORMATION BACK-ON-TRACK TERMINÉE !")
        print("="*60)
        print("\n📊 Résumé :")
        print(f"   - Trains de nuit : {len(df_main)} lignes")
        print(f"   - Relations pays : {len(df_countries)} lignes")
        print(f"   - Relations opérateurs : {len(df_operators)} lignes")
        print(f"   - Gares par direction : {len(df_stations)} lignes")
        print(f"\n📁 Fichiers sauvegardés dans : {PROCESSED_DATA_DIR}")
        print("\n✅ Toutes les transformations sont terminées !")


if __name__ == "__main__":
    main()

