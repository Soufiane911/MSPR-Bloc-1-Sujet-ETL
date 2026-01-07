#!/usr/bin/env python3
"""
Script pour créer la base de données obrail_europe
Utilise psycopg2 pour se connecter à PostgreSQL et créer la base
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
from pathlib import Path

# Ajouter le chemin pour importer config
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR))

from config.config import DATABASE

def create_database():
    """Crée la base de données obrail_europe"""
    
    # Configuration de connexion (se connecter à la base 'postgres' par défaut)
    import os
    import getpass
    
    # Sur macOS avec Homebrew, l'utilisateur par défaut est souvent le nom d'utilisateur système
    default_user = os.getenv('USER', getpass.getuser())
    
    db_config = {
        'host': DATABASE.get('host', 'localhost'),
        'port': DATABASE.get('port', 5432),
        'database': 'postgres',  # Se connecter à postgres pour créer une nouvelle base
        'user': DATABASE.get('user', default_user),  # Utiliser l'utilisateur système par défaut
        'password': DATABASE.get('password', '')
    }
    
    # Vérifier le mot de passe (peut être vide pour certaines installations)
    import os
    if not db_config['password']:
        # Essayer de récupérer depuis variable d'environnement
        db_config['password'] = os.getenv('POSTGRES_PASSWORD', '')
        if not db_config['password']:
            print("ℹ️  Aucun mot de passe configuré (tentative sans mot de passe)")
    
    try:
        # Se connecter à PostgreSQL (base postgres)
        print(f"🔌 Connexion à PostgreSQL...")
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        # Vérifier si la base existe déjà
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM pg_database WHERE datname = 'obrail_europe'
            """)
            exists = cur.fetchone()
            
            if exists:
                print("⚠️  La base de données 'obrail_europe' existe déjà")
                response = input("   Voulez-vous la supprimer et la recréer ? (o/N): ")
                if response.lower() == 'o':
                    print("🗑️  Suppression de l'ancienne base...")
                    cur.execute("DROP DATABASE obrail_europe;")
                    print("✅ Ancienne base supprimée")
                else:
                    print("✅ Utilisation de la base existante")
                    conn.close()
                    return True
        
        # Créer la base de données
        print("🔨 Création de la base de données 'obrail_europe'...")
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE obrail_europe;")
        
        print("✅ Base de données 'obrail_europe' créée avec succès !")
        conn.close()
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ Erreur de connexion : {e}")
        print("\n💡 Vérifiez que :")
        print("   - PostgreSQL est installé et démarré")
        print("   - Le mot de passe est correct")
        print("   - L'utilisateur 'postgres' existe")
        return False
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


if __name__ == "__main__":
    print("="*70)
    print("🗄️  CRÉATION DE LA BASE DE DONNÉES")
    print("="*70)
    print("Base à créer : obrail_europe")
    print("="*70)
    
    if create_database():
        print("\n✅ ÉTAPE 1 TERMINÉE !")
        print("\n➡️  Prochaine étape : Exécuter create_schema.py")
    else:
        print("\n❌ Échec de la création de la base de données")

