"""
Configuration de la base de données pour l'ETL ObRail Europe.

OPTIMISATION: Connexion lazy - l'engine est créé uniquement à la première utilisation.
Cela accélère le démarrage de l'ETL quand on n'a pas besoin de la base (ex: --status).
"""

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    DATABASE_URL = "postgresql://obrail:changeme@database:5432/obrail_db"

# Variables pour lazy loading
_engine = None
_SessionLocal = None


def get_engine():
    """Crée l'engine SQLAlchemy à la demande (lazy loading)."""
    global _engine
    if _engine is None:
        # Arguments de connexion
        engine_kwargs = {
            "pool_pre_ping": True,
            "echo": False
        }

        # Arguments spécifiques à PostgreSQL
        if DATABASE_URL.startswith("postgresql"):
            engine_kwargs.update({
                "pool_size": 10,
                "max_overflow": 20,
            })

        from sqlalchemy import create_engine
        _engine = create_engine(DATABASE_URL, **engine_kwargs)
    return _engine


def get_session_factory():
    """Retourne la session factory, créée à la demande."""
    global _SessionLocal
    if _SessionLocal is None:
        from sqlalchemy.orm import sessionmaker
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=get_engine()
        )
    return _SessionLocal


def get_db():
    """Générateur de sessions de base de données."""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
