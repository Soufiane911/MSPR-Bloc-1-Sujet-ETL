"""
Tests d'integration API + PostgreSQL.

Ces tests necessitent une base PostgreSQL accessible.
En CI, le service postgres est demarre automatiquement.
En local sans BDD, les tests sont ignores (skipped).
"""

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Skipper toute la suite si la BDD n'est pas accessible
DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    try:
        from api.app.database import DATABASE_URL as _DB_URL
        DATABASE_URL = _DB_URL
    except Exception:
        pass

_postgres_available = False
if DATABASE_URL and "postgresql" in DATABASE_URL:
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 3})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        _postgres_available = True
    except Exception:
        _postgres_available = False

if not _postgres_available:
    pytest.skip(
        "PostgreSQL non accessible — tests d'integration ignores. "
        "Demarrez la BDD avec : docker compose up -d database",
        allow_module_level=True,
    )


def _ensure_test_schema():
    """Initialise le schema metier si le service PostgreSQL CI est vide."""
    from sqlalchemy import create_engine, text

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        table_exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'operators'
                )
                """
            )
        ).scalar()

    if table_exists:
        return

    schema_path = Path(__file__).resolve().parents[1] / "sql" / "init" / "01-schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")

    raw_connection = engine.raw_connection()
    try:
        with raw_connection.cursor() as cursor:
            cursor.execute(schema_sql)
        raw_connection.commit()
    finally:
        raw_connection.close()


_ensure_test_schema()

from api.app.main import app

client = TestClient(app)


class TestHealthIntegration:
    """Tests de l'endpoint health avec BDD reelle."""

    def test_health_returns_connected(self):
        """L'endpoint /health doit indiquer que la base est connectee."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["database"] == "connected"


class TestStatsIntegration:
    """Tests des endpoints stats avec BDD reelle."""

    def test_stats_summary_returns_data(self):
        """/stats/summary doit retourner des stats globales."""
        response = client.get("/stats/summary")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        # Les cles attendues (memes si les valeurs sont 0 car BDD vide)
        assert "operators" in data
        assert "stations" in data
        assert "trains" in data
        assert "schedules" in data

    def test_stats_by_country_returns_list(self):
        """/stats/by-country doit retourner une liste."""
        response = client.get("/stats/by-country")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_stats_day_night_returns_list(self):
        """/stats/day-night doit retourner une liste."""
        response = client.get("/stats/day-night")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_stats_volumes_returns_data(self):
        """/stats/volumes doit retourner les volumes par table."""
        response = client.get("/stats/volumes")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "tables" in data
        assert "total_rows" in data
        assert "estimated_size_mb" in data
        assert isinstance(data["tables"], dict)


class TestTrainsIntegration:
    """Tests des endpoints trains avec BDD reelle."""

    def test_trains_list_returns_list(self):
        """/trains/ doit retourner une liste."""
        response = client.get("/trains/?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_trains_filter_by_type(self):
        """/trains/ avec filtre train_type doit fonctionner."""
        response = client.get("/trains/?train_type=day&limit=5")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Si des trains existent, ils doivent tous etre de type 'day'
        for train in data:
            assert train.get("train_type") == "day"

    def test_trains_invalid_type_returns_422(self):
        """Un parametre limite invalide doit retourner 422."""
        response = client.get("/trains/?limit=abc")
        assert response.status_code == 422


class TestTrajetsIntegration:
    """Tests des endpoints trajets avec BDD reelle."""

    def test_trajets_list_returns_list(self):
        """/trajets/ doit retourner une liste."""
        response = client.get("/trajets/?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_trajet_by_id_not_found_returns_404(self):
        """Un trajet inexistant doit retourner 404."""
        response = client.get("/trajets/999999")
        assert response.status_code == 404


class TestSchedulesIntegration:
    """Tests des endpoints schedules avec BDD reelle."""

    def test_schedules_list_returns_list(self):
        """/schedules/ doit retourner une liste."""
        response = client.get("/schedules/?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


class TestStationsIntegration:
    """Tests des endpoints stations avec BDD reelle."""

    def test_stations_list_returns_list(self):
        """/stations/ doit retourner une liste."""
        response = client.get("/stations/?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
