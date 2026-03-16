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

# Arguments de connexion par défaut
engine_kwargs = {
    "pool_pre_ping": True,
}

# Arguments spécifiques à PostgreSQL (non supportés par SQLite/SingletonThreadPool)
if DATABASE_URL.startswith("postgresql"):
    engine_kwargs.update({
        "pool_size": 10,
        "max_overflow": 20,
    })

# Création du moteur SQLAlchemy
engine = create_engine(DATABASE_URL, **engine_kwargs)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Générateur de sessions de base de données."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
