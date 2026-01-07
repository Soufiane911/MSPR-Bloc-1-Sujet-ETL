#!/usr/bin/env python3
"""
Script pour créer le schéma de base de données
Exécute le fichier schema.sql pour créer toutes les tables
"""

import psycopg2
from pathlib import Path
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_schema(db_config, schema_file):
    """
    Crée le schéma de base de données en exécutant le fichier SQL
    
    Args:
        db_config: Dictionnaire avec les paramètres de connexion DB
        schema_file: Chemin vers le fichier schema.sql
    """
    schema_path = Path(schema_file)
    
    if not schema_path.exists():
        logger.error(f"❌ Fichier SQL introuvable : {schema_path}")
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
        logger.info(f"📖 Lecture de {schema_path}...")
        with open(schema_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        logger.info("🔨 Création du schéma...")
        with conn.cursor() as cur:
            cur.execute(sql_script)
        
        logger.info("✅ Schéma créé avec succès !")
        conn.close()
        return True
        
    except psycopg2.errors.DuplicateTable as e:
        logger.warning(f"⚠️  Certaines tables existent déjà : {e}")
        logger.info("   Les tables existantes seront ignorées (IF NOT EXISTS)")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création du schéma : {e}")
        return False


def main():
    """Fonction principale"""
    print("="*70)
    print("🔨 CRÉATION DU SCHÉMA DE BASE DE DONNÉES")
    print("="*70)
    
    # Chemins
    BASE_DIR = Path(__file__).parent.parent
    SCHEMA_FILE = BASE_DIR / "load" / "schema.sql"
    
    # Configuration DB
    sys.path.append(str(BASE_DIR))
    from config.config import DATABASE
    
    import os
    import getpass
    
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
    
    # Créer le schéma
    if create_schema(db_config, SCHEMA_FILE):
        print("\n✅ Schéma créé avec succès !")
        print(f"\n📁 Base de données : {db_config['name']}")
        print("\n➡️  Prochaine étape : Charger les données avec load_data.py")
    else:
        print("\n❌ Échec de la création du schéma")
        print("   Vérifiez les logs ci-dessus pour plus de détails")


if __name__ == "__main__":
    main()

