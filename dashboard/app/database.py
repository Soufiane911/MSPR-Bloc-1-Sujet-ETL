"""
Configuration de la base de données pour le dashboard.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Charge les variables d'environnement dans Docker
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    DATABASE_URL = "postgresql://obrail:changeme@database:5432/obrail_db"

# Arguments de connexion
engine_kwargs = {
    "pool_pre_ping": True,
}

# Arguments spécifiques à PostgreSQL
if DATABASE_URL.startswith("postgresql"):
    engine_kwargs.update({
        "pool_size": 5,
        "max_overflow": 10,
    })

engine = create_engine(DATABASE_URL, **engine_kwargs)


def get_connection():
    """
    Retourne une connexion à la base de données.

    Returns:
        Connection: Connexion SQLAlchemy
    """
    return engine.connect()


def execute_query(query: str, params: dict = None) -> pd.DataFrame:
    """
    Exécute une requête SQL et retourne les résultats.

    Args:
        query: Requête SQL
        params: Paramètres de la requête

    Returns:
        pd.DataFrame: Résultats de la requête
    """
    return pd.read_sql(query, engine, params=params)
