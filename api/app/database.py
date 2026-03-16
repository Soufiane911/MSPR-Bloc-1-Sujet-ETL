"""
Configuration de la base de données pour l'API.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError(
        "La variable d'environnement DATABASE_URL est requise. "
        "Copiez .env.example vers .env et renseignez vos valeurs."
    )

# Création du moteur SQLAlchemy
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20, pool_pre_ping=True)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Générateur de sessions de base de données."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
