"""
Configuration de la base de données pour le dashboard.
"""

import os
import pandas as pd
from sqlalchemy import create_engine

# Configuration PostgreSQL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://obrail:obrail_secure_password@localhost:5432/obrail_db"
)

# Création du moteur SQLAlchemy
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True
)


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
