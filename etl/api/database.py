"""
Connexion à la base de données PostgreSQL
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
import sys
import os
import getpass

# Ajouter le chemin pour importer config
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR))

from config.config import DATABASE


def get_db_connection():
    """
    Crée et retourne une connexion à la base de données
    
    Returns:
        psycopg2.connection: Connexion PostgreSQL
    """
    default_user = os.getenv('USER', getpass.getuser())
    
    db_config = {
        'host': DATABASE.get('host', 'localhost'),
        'port': DATABASE.get('port', 5432),
        'database': DATABASE.get('name', 'obrail_europe'),
        'user': DATABASE.get('user', default_user),
        'password': DATABASE.get('password', '')
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        raise Exception(f"Erreur de connexion à la base de données : {e}")


def get_db_cursor(conn):
    """
    Crée et retourne un curseur avec RealDictCursor (retourne des dictionnaires)
    
    Args:
        conn: Connexion PostgreSQL
        
    Returns:
        psycopg2.extras.RealDictCursor: Curseur avec dictionnaires
    """
    return conn.cursor(cursor_factory=RealDictCursor)

