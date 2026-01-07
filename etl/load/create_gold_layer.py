#!/usr/bin/env python3
"""
Script pour créer la couche Gold (vues agrégées)
Exécute le fichier create_gold_views.sql pour créer toutes les vues
"""

import psycopg2
from pathlib import Path
import logging
import sys
import os
import getpass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_gold_views(db_config, views_file):
    """
    Crée les vues Gold en exécutant le fichier SQL
    
    Args:
        db_config: Dictionnaire avec les paramètres de connexion DB
        views_file: Chemin vers le fichier create_gold_views.sql
    """
    views_path = Path(views_file)
    
    if not views_path.exists():
        logger.error(f"❌ Fichier SQL introuvable : {views_path}")
        return False
    
    try:
        # Se connecter
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['name'],
            user=db_config['user'],
            password=db_config['password']
        )
        conn.autocommit = True
        logger.info(f"✅ Connecté à PostgreSQL : {db_config['name']}")
        
        # Lire et exécuter le script SQL
        logger.info(f"📖 Lecture de {views_path}...")
        with open(views_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        logger.info("🥇 Création des vues Gold...")
        with conn.cursor() as cur:
            cur.execute(sql_script)
        
        logger.info("✅ Vues Gold créées avec succès !")
        
        # Lister les vues créées
        logger.info("\n📊 Vues créées :")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'v_%'
                ORDER BY table_name;
            """)
            views = cur.fetchall()
            for view in views:
                logger.info(f"   ✅ {view[0]}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création des vues Gold : {e}")
        return False


def main():
    """Fonction principale"""
    print("="*70)
    print("🥇 CRÉATION DE LA COUCHE GOLD (Vues Agrégées)")
    print("="*70)
    
    # Chemins
    BASE_DIR = Path(__file__).parent.parent
    VIEWS_FILE = BASE_DIR / "load" / "create_gold_views.sql"
    
    # Configuration DB
    sys.path.append(str(BASE_DIR))
    from config.config import DATABASE
    
    default_user = os.getenv('USER', getpass.getuser())
    
    db_config = {
        'host': DATABASE.get('host', 'localhost'),
        'port': DATABASE.get('port', 5432),
        'name': DATABASE.get('name', 'obrail_europe'),
        'user': DATABASE.get('user', default_user),
        'password': DATABASE.get('password', '')
    }
    
    if not db_config['password']:
        print("ℹ️  Aucun mot de passe configuré (tentative sans mot de passe)")
    
    # Créer les vues
    if create_gold_views(db_config, VIEWS_FILE):
        print("\n✅ Couche Gold créée avec succès !")
        print(f"\n📁 Base de données : {db_config['name']}")
        print("\n📊 Vues disponibles :")
        print("   - v_routes_stats : Statistiques par ligne")
        print("   - v_stops_popularity : Gares les plus fréquentées")
        print("   - v_trips_enriched : Trajets enrichis")
        print("   - v_night_trains_enriched : Trains de nuit enrichis")
        print("   - v_agency_stats : Statistiques par opérateur")
        print("   - v_top_routes_by_trips : Top lignes")
        print("   - v_top_stops_by_popularity : Top gares")
        print("   - v_longest_trips : Trajets les plus longs")
        print("   - v_most_international_night_trains : Trains les plus internationaux")
        print("\n➡️  Prochaine étape : Utiliser ces vues dans l'API REST")
    else:
        print("\n❌ Échec de la création de la couche Gold")
        print("   Vérifiez les logs ci-dessus pour plus de détails")


if __name__ == "__main__":
    main()

