import pytest
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from unittest.mock import MagicMock

from loaders import database_loader as loader_module
from loaders.database_loader import DatabaseLoader

TEST_SCHEMA = "test_etl"


def test_load_stations_preparation_keeps_distinct_source_ids(monkeypatch):
    """Deux stop_id differents ne doivent pas etre dedupliques par nom."""
    loader = DatabaseLoader.__new__(DatabaseLoader)
    loader.logger = MagicMock()
    loader.stats = {"stations_loaded": 0}
    captured = {}

    def fake_insert(df, table_name, conflict_columns, update_columns):
        captured["df"] = df.copy()
        captured["conflict_columns"] = conflict_columns
        return len(df)

    loader._insert_with_tosql = fake_insert

    stations = pd.DataFrame(
        [
            {
                "stop_name": "Zurich HB",
                "country": "CH",
                "stop_id": "ZRH_PLATFORM_1",
                "source_name": "cff_sbb",
            },
            {
                "stop_name": "Zurich HB",
                "country": "CH",
                "stop_id": "ZRH_PLATFORM_2",
                "source_name": "cff_sbb",
            },
        ]
    )

    count = loader.load_stations(stations)

    assert count == 2
    assert captured["conflict_columns"] == ["uic_code", "source_name"]
    assert set(captured["df"]["uic_code"]) == {"ZRH_PLATFORM_1", "ZRH_PLATFORM_2"}


def test_load_schedules_rejects_impossible_train_speed():
    loader = DatabaseLoader.__new__(DatabaseLoader)
    loader.logger = MagicMock()
    loader.stats = {"schedules_loaded": 0}
    captured = {}

    def fake_bulk_insert(df, table_name, columns, conflict_columns, update_columns):
        captured["df"] = df.copy()
        return len(df)

    loader._bulk_insert = fake_bulk_insert

    schedules = pd.DataFrame(
        [
            {
                "train_id": 1,
                "origin_id": 10,
                "destination_id": 20,
                "departure_time": "2026-01-01T00:00:00+00:00",
                "arrival_time": "2026-01-01T00:13:00+00:00",
                "duration_min": 13,
                "distance_km": 1036,
                "frequency": "daily",
                "source_name": "back_on_track",
            },
            {
                "train_id": 2,
                "origin_id": 30,
                "destination_id": 40,
                "departure_time": "2026-01-01T00:00:00+00:00",
                "arrival_time": "2026-01-01T11:16:00+00:00",
                "duration_min": 676,
                "distance_km": 1036,
                "frequency": "daily",
                "source_name": "back_on_track",
            },
        ]
    )

    count = loader.load_schedules(schedules)

    assert count == 1
    assert captured["df"]["train_id"].tolist() == [2]


def _setup_test_schema(engine):
    """Crée un schéma de test isolé avec les tables PostgreSQL."""
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {TEST_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {TEST_SCHEMA}"))
        conn.execute(text("""
            CREATE TABLE operators (
                operator_id SERIAL PRIMARY KEY,
                name VARCHAR(150) NOT NULL,
                country VARCHAR(50),
                website VARCHAR(255),
                source_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(name, country, source_name)
            )
        """))
        conn.execute(text("""
            CREATE TABLE stations (
                station_id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                city VARCHAR(100),
                country VARCHAR(50),
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                uic_code VARCHAR(100),
                timezone VARCHAR(50),
                source_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(uic_code, source_name)
            )
        """))
        conn.execute(text("""
            CREATE TABLE trains (
                train_id SERIAL PRIMARY KEY,
                train_number VARCHAR(200) NOT NULL,
                operator_id INTEGER NOT NULL,
                train_type VARCHAR(10) NOT NULL,
                category VARCHAR(50),
                route_name VARCHAR(200),
                train_type_rule VARCHAR(10),
                train_type_heuristic VARCHAR(10),
                train_type_ml VARCHAR(10),
                classification_method VARCHAR(50),
                classification_reason VARCHAR(100),
                classification_confidence DECIMAL(4, 2),
                ml_night_probability DECIMAL(4, 2),
                night_percentage DECIMAL(5, 2),
                needs_manual_review BOOLEAN DEFAULT FALSE,
                source_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(train_number, operator_id, source_name)
            )
        """))
        conn.execute(text("""
            CREATE TABLE schedules (
                schedule_id SERIAL PRIMARY KEY,
                train_id INTEGER NOT NULL,
                origin_id INTEGER NOT NULL,
                destination_id INTEGER NOT NULL,
                departure_time TIMESTAMP WITH TIME ZONE,
                arrival_time TIMESTAMP WITH TIME ZONE,
                duration_min INTEGER NOT NULL,
                distance_km DECIMAL(10, 2),
                frequency VARCHAR(50),
                source_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(train_id, origin_id, destination_id, departure_time)
            )
        """))


def _teardown_test_schema(engine):
    """Supprime le schéma de test."""
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE"))


@pytest.mark.integration
def test_database_loader_is_idempotent(monkeypatch):
    """Test d'intégration PostgreSQL : vérifie l'idempotence du loader."""
    pytest.importorskip("psycopg2")

    try:
        engine = create_engine(
            "postgresql://obrail:changeme@localhost:5433/obrail_db",
            connect_args={"options": f"-c search_path={TEST_SCHEMA}"},
        )
        _setup_test_schema(engine)
    except OperationalError:
        pytest.skip("PostgreSQL not available - this is an integration test")

    monkeypatch.setattr(loader_module, "get_engine", lambda: engine)

    loader = DatabaseLoader()

    operators = pd.DataFrame(
        [
            {
                "agency_name": "SNCF",
                "country": "FR",
                "agency_url": "https://sncf.example",
                "source_name": "sncf_intercites",
            }
        ]
    )
    stations = pd.DataFrame(
        [
            {
                "stop_name": "Paris Gare de Lyon",
                "city": "Paris",
                "country": "FR",
                "stop_lat": 48.84,
                "stop_lon": 2.37,
                "stop_id": "PAR",
                "timezone": "Europe/Paris",
                "source_name": "sncf_intercites",
            },
            {
                "stop_name": "Lyon Part-Dieu",
                "city": "Lyon",
                "country": "FR",
                "stop_lat": 45.76,
                "stop_lon": 4.86,
                "stop_id": "LYO",
                "timezone": "Europe/Paris",
                "source_name": "sncf_intercites",
            },
        ]
    )

    loader.load_operators(operators)
    loader.load_operators(operators)
    loader.load_stations(stations)
    loader.load_stations(stations)

    with engine.connect() as conn:
        operator_id = conn.execute(
            text("SELECT operator_id FROM operators LIMIT 1")
        ).scalar_one()
        station_ids = (
            conn.execute(text("SELECT station_id FROM stations ORDER BY station_id"))
            .scalars()
            .all()
        )

    trains = pd.DataFrame(
        [
            {
                "trip_id": "T100",
                "operator_id": operator_id,
                "train_type": "day",
                "train_type_rule": "day",
                "train_type_heuristic": "day",
                "classification_method": "heuristic",
                "classification_reason": "low_night_share",
                "classification_confidence": 0.75,
                "night_percentage": 10.0,
                "needs_manual_review": False,
                "route_short_name": "TGV",
                "route_long_name": "Paris-Lyon",
                "source_name": "sncf_intercites",
            }
        ]
    )
    schedules = pd.DataFrame(
        [
            {
                "train_id": 1,
                "origin_id": station_ids[0],
                "destination_id": station_ids[1],
                "departure_time": "2025-01-01T08:00:00+00:00",
                "arrival_time": "2025-01-01T10:00:00+00:00",
                "duration_min": 120,
                "distance_km": 450.0,
                "frequency": "daily",
                "source_name": "sncf_intercites",
            }
        ]
    )

    loader.load_trains(trains)
    loader.load_trains(trains)
    loader.load_schedules(schedules)
    loader.load_schedules(schedules)

    counts = loader.verify_counts()

    # Nettoyage du schéma de test
    _teardown_test_schema(engine)

    assert counts == {
        "operators": 1,
        "stations": 2,
        "trains": 1,
        "schedules": 1,
    }


@pytest.mark.integration
def test_load_stations_keeps_same_name_with_distinct_source_ids(monkeypatch):
    """Les schedules referencent les stop_id source, pas seulement le nom de gare."""
    pytest.importorskip("psycopg2")

    try:
        engine = create_engine(
            "postgresql://obrail:changeme@localhost:5433/obrail_db",
            connect_args={"options": f"-c search_path={TEST_SCHEMA}"},
        )
        _setup_test_schema(engine)
    except OperationalError:
        pytest.skip("PostgreSQL not available - this is an integration test")

    monkeypatch.setattr(loader_module, "get_engine", lambda: engine)

    loader = DatabaseLoader()
    stations = pd.DataFrame(
        [
            {
                "stop_name": "Zurich HB",
                "country": "CH",
                "stop_lat": 47.378,
                "stop_lon": 8.54,
                "stop_id": "ZRH_PLATFORM_1",
                "source_name": "cff_sbb",
            },
            {
                "stop_name": "Zurich HB",
                "country": "CH",
                "stop_lat": 47.379,
                "stop_lon": 8.541,
                "stop_id": "ZRH_PLATFORM_2",
                "source_name": "cff_sbb",
            },
        ]
    )

    loader.load_stations(stations)

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM stations")).scalar_one()

    _teardown_test_schema(engine)

    assert count == 2
