#!/usr/bin/env python3
"""
Script de chargement des données transformées dans PostgreSQL
Charge toutes les données GTFS et Back-on-Track dans la base obrail_europe
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
import logging
from tqdm import tqdm
import sys

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Classe pour charger les données dans PostgreSQL"""
    
    def __init__(self, db_config, clean_data_dir):
        """
        Initialise le chargeur de données
        
        Args:
            db_config: Dictionnaire avec les paramètres de connexion DB
            clean_data_dir: Chemin vers les données clean
        """
        self.db_config = db_config
        self.clean_dir = Path(clean_data_dir)
        self.conn = None
        
        logger.info(f"📁 Données clean : {self.clean_dir}")
    
    def connect(self):
        """Établit la connexion à PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['name'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            logger.info(f"✅ Connecté à PostgreSQL : {self.db_config['name']}")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur de connexion : {e}")
            return False
    
    def disconnect(self):
        """Ferme la connexion"""
        if self.conn:
            self.conn.close()
            logger.info("🔌 Connexion fermée")
    
    def load_table(self, table_name, folder_name, csv_file=None, chunk_size=10000, if_exists='replace'):
        """
        Charge un fichier CSV dans une table PostgreSQL
        
        Args:
            table_name: Nom de la table
            folder_name: Nom du dossier dans clean/
            csv_file: Nom du fichier CSV (si None, utilise table_name.csv)
            chunk_size: Taille des chunks pour les gros fichiers
            if_exists: 'replace' ou 'append'
        """
        if csv_file is None:
            csv_file = f"{table_name}.csv"
        
        csv_path = self.clean_dir / folder_name / csv_file
        
        if not csv_path.exists():
            logger.warning(f"⚠️  Fichier introuvable : {csv_path}")
            return False
        
        try:
            # Lire le CSV
            logger.info(f"📖 Lecture de {folder_name}/{csv_file}...")
            df = pd.read_csv(csv_path, low_memory=False)
            total_rows = len(df)
            logger.info(f"   📊 {total_rows:,} lignes à charger")
            
            if total_rows == 0:
                logger.warning(f"   ⚠️  Fichier vide, ignoré")
                return True
            
            # Pour les gros fichiers, charger par chunks
            if total_rows > chunk_size:
                logger.info(f"   🔄 Chargement par chunks de {chunk_size} lignes...")
                
                # Vider la table si replace
                if if_exists == 'replace':
                    with self.conn.cursor() as cur:
                        cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
                    self.conn.commit()
                
                # Charger par chunks
                chunks = [df[i:i+chunk_size] for i in range(0, total_rows, chunk_size)]
                
                for i, chunk in enumerate(tqdm(chunks, desc=f"   Chargement {table_name}")):
                    self._insert_chunk(table_name, chunk, first_chunk=(i == 0) and if_exists == 'replace')
                
            else:
                # Charger tout d'un coup
                logger.info(f"   💾 Chargement direct...")
                self._insert_dataframe(table_name, df, if_exists)
            
            logger.info(f"   ✅ {table_name} : {total_rows:,} lignes chargées")
            return True
            
        except Exception as e:
            logger.error(f"   ❌ Erreur lors du chargement de {table_name}: {e}")
            # Rollback en cas d'erreur
            self.conn.rollback()
            return False
    
    def _insert_dataframe(self, table_name, df, if_exists='replace'):
        """Insère un DataFrame dans une table"""
        if if_exists == 'replace':
            # Vider la table
            with self.conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
            self.conn.commit()
        
        # Utiliser execute_values pour les gros volumes (plus robuste)
        if len(df) > 10000:
            self._insert_with_execute_values(table_name, df)
        else:
            # Utiliser pandas.to_sql pour les petits volumes
            from sqlalchemy import create_engine
            engine = create_engine(
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
                f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['name']}"
            )
            
            df.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
    
    def _insert_chunk(self, table_name, chunk, first_chunk=False):
        """Insère un chunk dans une table"""
        if first_chunk:
            # Vider la table au premier chunk
            with self.conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
            self.conn.commit()
        
        # Utiliser execute_values pour les gros volumes
        self._insert_with_execute_values(table_name, chunk)
    
    def _insert_with_execute_values(self, table_name, df):
        """Insère un DataFrame en utilisant execute_values (robuste pour gros volumes)"""
        from psycopg2.extras import execute_values
        
        # Convertir DataFrame en liste de tuples (gérer les NaN)
        import numpy as np
        tuples = []
        for row in df.itertuples(index=False):
            # Convertir les NaN en None pour PostgreSQL
            row_tuple = tuple(None if pd.isna(val) else val for val in row)
            tuples.append(row_tuple)
        
        # Colonnes
        cols = ','.join(list(df.columns))
        
        # Requête SQL
        query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
        
        # Insérer par chunks de 1000 lignes
        chunk_size = 1000
        cur = self.conn.cursor()
        try:
            for i in range(0, len(tuples), chunk_size):
                chunk_tuples = tuples[i:i+chunk_size]
                execute_values(cur, query, chunk_tuples)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()
    
    def load_all_gtfs(self):
        """Charge toutes les tables GTFS dans l'ordre correct"""
        logger.info("="*60)
        logger.info("📦 CHARGEMENT DES DONNÉES GTFS")
        logger.info("="*60)
        
        # Ordre important : respecter les dépendances
        tables = [
            ('agency', 'agency', 1000),  # Petit fichier
            ('routes', 'routes', 1000),  # Petit fichier
            ('stops', 'stops', 10000),   # Moyen fichier
            ('calendar_dates', 'calendar_dates', 50000),  # Gros fichier
            ('trips', 'trips', 10000),   # Moyen fichier
            ('stop_times', 'stop_times', 50000),  # TRÈS gros fichier
        ]
        
        success_count = 0
        for table_name, folder_name, chunk_size in tables:
            logger.info(f"\n🔄 Chargement de {table_name}...")
            if self.load_table(table_name, folder_name, chunk_size=chunk_size):
                success_count += 1
            else:
                logger.error(f"❌ Échec du chargement de {table_name}")
        
        logger.info(f"\n✅ GTFS : {success_count}/{len(tables)} tables chargées")
        return success_count == len(tables)
    
    def load_all_backontrack(self):
        """Charge toutes les tables Back-on-Track dans l'ordre correct"""
        logger.info("\n" + "="*60)
        logger.info("🌙 CHARGEMENT DES DONNÉES BACK-ON-TRACK")
        logger.info("="*60)
        
        # Ordre important : night_trains d'abord (table principale)
        tables = [
            ('night_trains', 'night_trains', 1000),
            ('night_train_countries', 'night_trains', 1000),
            ('night_train_operators', 'night_trains', 1000),
            ('night_train_stations', 'night_trains', 1000),
        ]
        
        success_count = 0
        for table_name, folder_name, chunk_size in tables:
            logger.info(f"\n🔄 Chargement de {table_name}...")
            if self.load_table(table_name, folder_name, chunk_size=chunk_size):
                success_count += 1
            else:
                logger.error(f"❌ Échec du chargement de {table_name}")
        
        logger.info(f"\n✅ Back-on-Track : {success_count}/{len(tables)} tables chargées")
        return success_count == len(tables)


def main():
    """Fonction principale"""
    print("="*70)
    print("💾 CHARGEMENT DES DONNÉES DANS POSTGRESQL")
    print("="*70)
    
    # Chemins
    BASE_DIR = Path(__file__).parent.parent
    CLEAN_DATA_DIR = BASE_DIR / "data" / "clean"
    
    # Configuration DB (à adapter selon votre environnement)
    import sys
    import os
    import getpass
    sys.path.append(str(BASE_DIR))
    from config.config import DATABASE
    
    # Utiliser l'utilisateur système par défaut si non spécifié
    default_user = os.getenv('USER', getpass.getuser())
    
    db_config = {
        'host': DATABASE.get('host', 'localhost'),
        'port': DATABASE.get('port', 5432),
        'name': DATABASE.get('name', 'obrail_europe'),
        'user': DATABASE.get('user', default_user),
        'password': DATABASE.get('password', '')
    }
    
    # Pas besoin de demander le mot de passe (Homebrew n'en utilise pas par défaut)
    if not db_config['password']:
        print("ℹ️  Aucun mot de passe configuré (tentative sans mot de passe)")
    
    # Créer le chargeur
    loader = DataLoader(db_config, CLEAN_DATA_DIR)
    
    # Se connecter
    if not loader.connect():
        print("❌ Impossible de se connecter à PostgreSQL")
        return
    
    try:
        # Charger les données GTFS
        gtfs_success = loader.load_all_gtfs()
        
        # Charger les données Back-on-Track
        backontrack_success = loader.load_all_backontrack()
        
        # Résumé final
        print("\n" + "="*70)
        print("✅ CHARGEMENT TERMINÉ !")
        print("="*70)
        print(f"\n📊 Résumé :")
        print(f"   - GTFS : {'✅ Succès' if gtfs_success else '❌ Échec'}")
        print(f"   - Back-on-Track : {'✅ Succès' if backontrack_success else '❌ Échec'}")
        
        if gtfs_success and backontrack_success:
            print("\n🎉 Toutes les données ont été chargées avec succès !")
            print(f"\n📁 Base de données : {db_config['name']}")
            print("\n➡️  Prochaine étape : Tester les requêtes ou développer l'API REST")
        else:
            print("\n⚠️  Certaines données n'ont pas pu être chargées")
            print("   Vérifiez les logs ci-dessus pour plus de détails")
    
    finally:
        loader.disconnect()


if __name__ == "__main__":
    main()

