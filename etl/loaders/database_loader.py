"""
Module de chargement des données pour l'ETL ObRail Europe - VERSION OPTIMISÉE.

Gère le chargement des données transformées dans PostgreSQL avec insertions en bulk.

Performance : ~10,000-50,000 lignes/sec avec COPY vs ~10 lignes/sec avec row-by-row.
"""

import pandas as pd
import io
import time
from typing import Dict, List, Optional
from sqlalchemy import text
from config.database import get_engine
from config.logging_config import setup_logging


class DatabaseLoader:
    """
    Classe de chargement des données dans PostgreSQL avec optimisations de performance.

    Optimisations:
    - COPY pour insertions massives (plus rapide)
    - Batch insert avec to_sql() comme fallback
    - Traitement par chunks pour gérer la mémoire
    - Upsert avec ON CONFLICT
    """

    # Taille des batchs pour le traitement
    BATCH_SIZE = 10000
    COPY_THRESHOLD = 5000  # Utiliser COPY si plus de 5000 lignes

    def __init__(self):
        """Initialise le loader de base de données."""
        self.logger = setup_logging("loader.database")
        self.engine = get_engine()
        self._ensure_conflict_indexes()
        self.stats = {
            "operators_loaded": 0,
            "stations_loaded": 0,
            "trains_loaded": 0,
            "schedules_loaded": 0,
            "airlines_loaded": 0,
            "airports_loaded": 0,
            "flights_loaded": 0,
            "flight_instances_loaded": 0,
        }
        self._performance_stats = {}

    def _ensure_conflict_indexes(self) -> None:
        """
        Garantit la présence d'index uniques compatibles avec ON CONFLICT.

        Cela corrige les bases déjà initialisées avec un ancien schéma (volume Docker
        persistant) où certaines contraintes uniques peuvent être absentes.
        """
        migration_statements = [
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS train_type_rule VARCHAR(10)",
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS train_type_heuristic VARCHAR(10)",
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS train_type_ml VARCHAR(10)",
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS classification_method VARCHAR(50)",
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS classification_reason VARCHAR(100)",
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS classification_confidence DECIMAL(4, 2)",
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS ml_night_probability DECIMAL(4, 2)",
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS night_percentage DECIMAL(5, 2)",
            "ALTER TABLE trains ADD COLUMN IF NOT EXISTS needs_manual_review BOOLEAN DEFAULT FALSE",
        ]

        statements = [
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_operators_conflict
            ON operators (name, country, source_name)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_stations_conflict
            ON stations (name, country, source_name)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_trains_conflict
            ON trains (train_number, operator_id, source_name)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_schedules_conflict
            ON schedules (train_id, origin_id, destination_id, departure_time)
            """,
            """
            CREATE TABLE IF NOT EXISTS airlines (
                airline_id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                alias VARCHAR(100),
                iata VARCHAR(10),
                icao VARCHAR(10),
                country VARCHAR(50),
                source_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_airline_unique UNIQUE (name, iata, icao)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS airports (
                airport_id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                city VARCHAR(100),
                country VARCHAR(50),
                iata VARCHAR(10),
                icao VARCHAR(10),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                timezone VARCHAR(100),
                source_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_airport_unique UNIQUE (name, iata, icao)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS flights (
                flight_id SERIAL PRIMARY KEY,
                airline_id INTEGER REFERENCES airlines(airline_id) ON DELETE SET NULL,
                flight_number VARCHAR(50),
                origin_airport_id INTEGER REFERENCES airports(airport_id) ON DELETE SET NULL,
                destination_airport_id INTEGER REFERENCES airports(airport_id) ON DELETE SET NULL,
                distance_km DOUBLE PRECISION,
                aircraft_type VARCHAR(50),
                source_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_flight_unique UNIQUE (airline_id, flight_number, origin_airport_id, destination_airport_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS flight_instances (
                instance_id SERIAL PRIMARY KEY,
                flight_id INTEGER REFERENCES flights(flight_id) ON DELETE CASCADE,
                flight_date DATE,
                scheduled_departure TIMESTAMP WITH TIME ZONE,
                scheduled_arrival TIMESTAMP WITH TIME ZONE,
                actual_departure TIMESTAMP WITH TIME ZONE,
                actual_arrival TIMESTAMP WITH TIME ZONE,
                status VARCHAR(50),
                source_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_airlines_conflict
            ON airlines (name, iata, icao)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_airports_conflict
            ON airports (name, iata, icao)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_flights_conflict
            ON flights (airline_id, flight_number, origin_airport_id, destination_airport_id)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_flight_instances_conflict
            ON flight_instances (flight_id, flight_date, scheduled_departure)
            """,
        ]

        with self.engine.begin() as conn:
            for stmt in migration_statements:
                conn.execute(text(stmt))
            for stmt in statements:
                conn.execute(text(stmt))

    def _bulk_insert(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: List[str],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Insertion en bulk optimisée avec COPY ou batch insert.

        Args:
            df: DataFrame à insérer
            table_name: Nom de la table
            columns: Colonnes à insérer
            conflict_columns: Colonnes pour la contrainte ON CONFLICT
            update_columns: Colonnes à mettre à jour en cas de conflit

        Returns:
            int: Nombre de lignes insérées
        """
        if df.empty:
            return 0

        start_time = time.time()
        count = len(df)

        # Vérifier que toutes les colonnes existent
        available_cols = [c for c in columns if c in df.columns]
        if not available_cols:
            self.logger.warning(f"[WARN] Aucune colonne disponible pour {table_name}")
            return 0

        df_load = df[available_cols].copy()

        try:
            # Pour les petits volumes (< COPY_THRESHOLD), utiliser to_sql
            if count < self.COPY_THRESHOLD:
                inserted = self._insert_with_tosql(
                    df_load, table_name, conflict_columns, update_columns
                )
            else:
                # Pour les gros volumes, utiliser COPY
                inserted = self._insert_with_copy(
                    df_load, table_name, conflict_columns, update_columns
                )

            elapsed = time.time() - start_time
            rate = count / elapsed if elapsed > 0 else 0
            self._performance_stats[table_name] = {
                "rows": count,
                "time": elapsed,
                "rate": rate,
            }

            self.logger.info(
                f"[OK] {table_name}: {inserted} lignes en {elapsed:.2f}s ({rate:.0f} lignes/sec)"
            )
            return inserted

        except Exception as e:
            self.logger.error(f"[ERROR] Erreur insertion {table_name}: {str(e)}")
            raise

    def _insert_with_tosql(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Insertion avec pandas to_sql et méthode multi (fallback).

        Args:
            df: DataFrame à insérer
            table_name: Nom de la table
            conflict_columns: Colonnes pour ON CONFLICT
            update_columns: Colonnes à mettre à jour

        Returns:
            int: Nombre de lignes insérées
        """
        count = len(df)

        # Créer une table temporaire
        temp_table = f"{table_name}_temp"

        with self.engine.begin() as conn:
            # Supprimer la table temporaire si elle existe
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

            # Créer la table temporaire avec la même structure
            conn.execute(
                text(
                    f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {table_name} WHERE 1=0"
                )
            )

            # Insérer dans la table temporaire avec to_sql
            df.to_sql(
                temp_table,
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=self.BATCH_SIZE,
            )

            # Construire la requête UPSERT
            conflict_str = ", ".join(conflict_columns)
            columns_str = ", ".join(df.columns)

            if update_columns:
                updates = ", ".join(
                    [f"{col} = EXCLUDED.{col}" for col in update_columns]
                )
                upsert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {columns_str} FROM {temp_table}
                    ON CONFLICT ({conflict_str})
                    DO UPDATE SET {updates}, updated_at = CURRENT_TIMESTAMP
                """
            else:
                upsert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {columns_str} FROM {temp_table}
                    ON CONFLICT ({conflict_str})
                    DO NOTHING
                """

            result = conn.execute(text(upsert_sql))

            # Supprimer la table temporaire
            conn.execute(text(f"DROP TABLE {temp_table}"))

        return count

    def _insert_with_copy(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Insertion avec COPY FROM (méthode la plus rapide pour PostgreSQL).

        Args:
            df: DataFrame à insérer
            table_name: Nom de la table
            conflict_columns: Colonnes pour ON CONFLICT
            update_columns: Colonnes à mettre à jour

        Returns:
            int: Nombre de lignes insérées
        """
        count = len(df)

        # Créer une table temporaire
        temp_table = f"{table_name}_temp_{int(time.time())}"

        with self.engine.begin() as conn:
            # Supprimer la table temporaire si elle existe
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

            # Créer la table temporaire avec la même structure
            conn.execute(
                text(
                    f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {table_name} WHERE 1=0"
                )
            )

            # Utiliser COPY pour insérer les données
            # Convertir le DataFrame en CSV en mémoire
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False, sep="\t", na_rep="\\N")
            csv_buffer.seek(0)

            # Utiliser l'API raw de psycopg2 pour COPY
            raw_conn = conn.connection
            with raw_conn.cursor() as cursor:
                cursor.copy_from(
                    csv_buffer,
                    temp_table,
                    columns=list(df.columns),
                    sep="\t",
                    null="\\N",
                )

            # Construire la requête UPSERT
            conflict_str = ", ".join(conflict_columns)
            columns_str = ", ".join(df.columns)

            if update_columns:
                updates = ", ".join(
                    [f"{col} = EXCLUDED.{col}" for col in update_columns]
                )
                upsert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {columns_str} FROM {temp_table}
                    ON CONFLICT ({conflict_str})
                    DO UPDATE SET {updates}, updated_at = CURRENT_TIMESTAMP
                """
            else:
                upsert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {columns_str} FROM {temp_table}
                    ON CONFLICT ({conflict_str})
                    DO NOTHING
                """

            result = conn.execute(text(upsert_sql))
            inserted = result.rowcount if result else count

            # Supprimer la table temporaire
            conn.execute(text(f"DROP TABLE {temp_table}"))

        return inserted

    def load_operators(self, df: pd.DataFrame) -> int:
        """
        Charge les opérateurs avec insertion en bulk.

        Args:
            df: DataFrame des opérateurs

        Returns:
            int: Nombre d'opérateurs chargés
        """
        self.logger.info("=" * 60)
        self.logger.info("Chargement des opérateurs (BULK UPSERT)...")
        self.logger.info("=" * 60)

        if df.empty:
            self.logger.warning("[WARN] Aucun opérateur à charger")
            return 0

        # Mapping et préparation
        column_mapping = {
            "agency_name": "name",
            "agency_url": "website",
            "source_name": "source_name",
        }
        df_load = df.rename(columns=column_mapping)

        if "country" not in df_load.columns:
            df_load["country"] = "EU"
        df_load["country"] = (
            df_load["country"].fillna("EU").astype(str).str.strip().replace("", "EU")
        )
        if "name" in df_load.columns:
            df_load = df_load[df_load["name"].notna()]
            df_load["name"] = df_load["name"].astype(str).str.strip()
            df_load = df_load[df_load["name"] != ""]

        # Certaines sources ne renseignent pas le pays; on applique une valeur
        # neutre pour respecter la contrainte NOT NULL en base.
        if "country" not in df_load.columns:
            df_load["country"] = "EU"
        df_load["country"] = (
            df_load["country"].fillna("EU").astype(str).str.strip().replace("", "EU")
        )
        if "name" in df_load.columns:
            df_load = df_load[df_load["name"].notna()]
            df_load["name"] = df_load["name"].astype(str).str.strip()
            df_load = df_load[df_load["name"] != ""]

        count = self._bulk_insert(
            df=df_load,
            table_name="operators",
            columns=["name", "country", "website", "source_name"],
            conflict_columns=["name", "country", "source_name"],
            update_columns=["website"],
        )

        self.stats["operators_loaded"] += count
        return count

    def load_stations(self, df: pd.DataFrame) -> int:
        """
        Charge les gares avec insertion en bulk.

        Args:
            df: DataFrame des gares

        Returns:
            int: Nombre de gares chargées
        """
        self.logger.info("=" * 60)
        self.logger.info("Chargement des gares (BULK UPSERT)...")
        self.logger.info("=" * 60)

        if df.empty:
            self.logger.warning("[WARN] Aucune gare à charger")
            return 0

        # Mapping et préparation
        column_mapping = {
            "stop_name": "name",
            "stop_lat": "latitude",
            "stop_lon": "longitude",
            "stop_id": "uic_code",
            "source_name": "source_name",
        }
        df_load = df.rename(columns=column_mapping)

        if "country" not in df_load.columns:
            df_load["country"] = "EU"
        df_load["country"] = (
            df_load["country"].fillna("EU").astype(str).str.strip().replace("", "EU")
        )
        if "name" in df_load.columns:
            df_load = df_load[df_load["name"].notna()]
            df_load["name"] = df_load["name"].astype(str).str.strip()
            df_load = df_load[df_load["name"] != ""]

        # CORRECTION: Deduplicate before insert to avoid ON CONFLICT error
        # Keep first occurrence of each unique (name, country, source_name) combination
        before_dedup = len(df_load)
        df_load = df_load.drop_duplicates(subset=["name", "country", "source_name"], keep="first")
        after_dedup = len(df_load)
        if before_dedup != after_dedup:
            self.logger.info(f"[DEDUP] Stations: removed {before_dedup - after_dedup} duplicates before insert")

        # Ensure optional columns exist (NULL when missing, no placeholder values)
        for col in ["city", "latitude", "longitude", "uic_code", "timezone", "source_name"]:
            if col not in df_load.columns:
                df_load[col] = None

        # Select only the required columns for insertion
        columns_to_insert = [
            "name",
            "city",
            "country",
            "latitude",
            "longitude",
            "uic_code",
            "timezone",
            "source_name",
        ]
        df_final = df_load[[c for c in columns_to_insert if c in df_load.columns]].copy()

        # Use to_sql instead of COPY for stations to avoid issues with missing columns
        count = self._insert_with_tosql(
            df_final,
            table_name="stations",
            conflict_columns=["name", "country", "source_name"],
            update_columns=["latitude", "longitude", "uic_code", "city"],
        )

        self.stats["stations_loaded"] += count
        return count

    def load_trains(self, df: pd.DataFrame) -> int:
        """
        Charge les trains avec insertion en bulk.

        Args:
            df: DataFrame des trains

        Returns:
            int: Nombre de trains chargés
        """
        self.logger.info("=" * 60)
        self.logger.info("Chargement des trains (BULK UPSERT)...")
        self.logger.info("=" * 60)

        if df.empty:
            self.logger.warning("[WARN] Aucun train à charger")
            return 0

        # Mapping et préparation
        column_mapping = {
            "trip_id": "train_number",
            "route_short_name": "category",
            "route_long_name": "route_name",
            "train_type": "train_type",
            "train_type_rule": "train_type_rule",
            "train_type_heuristic": "train_type_heuristic",
            "train_type_ml": "train_type_ml",
            "classification_method": "classification_method",
            "classification_reason": "classification_reason",
            "classification_confidence": "classification_confidence",
            "ml_night_probability": "ml_night_probability",
            "night_percentage": "night_percentage",
            "needs_manual_review": "needs_manual_review",
            "source_name": "source_name",
        }
        df_load = df.rename(columns=column_mapping)

        # S'assurer que operator_id est présent
        if "operator_id" not in df_load.columns:
            self.logger.error(
                "[ERROR] Colonne operator_id manquante dans les données trains"
            )
            return 0

        count = self._bulk_insert(
            df=df_load,
            table_name="trains",
            columns=[
                "train_number",
                "operator_id",
                "train_type",
                "category",
                "route_name",
                "train_type_rule",
                "train_type_heuristic",
                "train_type_ml",
                "classification_method",
                "classification_reason",
                "classification_confidence",
                "ml_night_probability",
                "night_percentage",
                "needs_manual_review",
                "source_name",
            ],
            conflict_columns=["train_number", "operator_id", "source_name"],
            update_columns=[
                "train_type",
                "category",
                "route_name",
                "train_type_rule",
                "train_type_heuristic",
                "train_type_ml",
                "classification_method",
                "classification_reason",
                "classification_confidence",
                "ml_night_probability",
                "night_percentage",
                "needs_manual_review",
            ],
        )

        self.stats["trains_loaded"] += count
        return count

    def load_airlines(self, df: pd.DataFrame) -> int:
        """Charge les compagnies aériennes."""
        self.logger.info("=" * 60)
        self.logger.info("Chargement des airlines (BULK UPSERT)...")
        self.logger.info("=" * 60)

        if df.empty:
            self.logger.warning("[WARN] Aucun airline à charger")
            return 0

        df_load = df.copy()
        # Map common columns
        column_mapping = {"name": "name", "iata": "iata", "icao": "icao", "country": "country", "alias": "alias"}
        df_load = df_load.rename(columns=column_mapping)

        for col in ["name", "iata", "icao", "country", "alias", "source_name"]:
            if col not in df_load.columns:
                df_load[col] = None

        count = self._bulk_insert(
            df=df_load,
            table_name="airlines",
            columns=["name", "alias", "iata", "icao", "country", "source_name"],
            conflict_columns=["name", "iata", "icao"],
            update_columns=["alias", "country"],
        )

        self.stats["airlines_loaded"] += count
        return count

    def load_airports(self, df: pd.DataFrame) -> int:
        """Charge les aéroports."""
        self.logger.info("=" * 60)
        self.logger.info("Chargement des airports (BULK UPSERT)...")
        self.logger.info("=" * 60)

        if df.empty:
            self.logger.warning("[WARN] Aucun airport à charger")
            return 0

        df_load = df.copy()
        column_mapping = {"name": "name", "city": "city", "country": "country", "iata": "iata", "icao": "icao", "latitude": "latitude", "longitude": "longitude", "timezone": "timezone"}
        df_load = df_load.rename(columns=column_mapping)

        for col in ["name", "city", "country", "iata", "icao", "latitude", "longitude", "timezone", "source_name"]:
            if col not in df_load.columns:
                df_load[col] = None

        # Ensure numeric
        if "latitude" in df_load.columns:
            df_load["latitude"] = pd.to_numeric(df_load["latitude"], errors="coerce")
        if "longitude" in df_load.columns:
            df_load["longitude"] = pd.to_numeric(df_load["longitude"], errors="coerce")

        count = self._insert_with_tosql(
            df_load[[c for c in ["name", "city", "country", "iata", "icao", "latitude", "longitude", "timezone", "source_name"] if c in df_load.columns]],
            table_name="airports",
            conflict_columns=["name", "iata", "icao"],
            update_columns=["city", "latitude", "longitude"],
        )

        self.stats["airports_loaded"] += count
        return count

    def load_flights(self, df: pd.DataFrame) -> int:
        """Charge les vols canoniques (routes)."""
        self.logger.info("=" * 60)
        self.logger.info("Chargement des flights (BULK UPSERT)...")
        self.logger.info("=" * 60)

        if df.empty:
            self.logger.warning("[WARN] Aucun flight à charger")
            return 0

        df_load = df.copy()
        # Expect columns: airline (iata or name), flight_number, source_airport / origin_iata, dest_airport / dest_iata, distance_km, equipment
        for col in ["airline", "flight_number", "source_airport", "origin_iata", "dest_airport", "dest_iata", "distance_km", "equipment", "source_name"]:
            if col not in df_load.columns:
                df_load[col] = None

        # Build lookup maps from DB
        with self.engine.connect() as conn:
            airlines_rows = conn.execute(text("SELECT airline_id, iata, icao, name FROM airlines")).fetchall()
            airports_rows = conn.execute(text("SELECT airport_id, iata, icao FROM airports")).fetchall()

        airline_map = {}
        airline_name_map = {}
        for row in airlines_rows:
            aid = row[0]
            iata = row[1]
            icao = row[2]
            name = row[3]
            if iata:
                airline_map[iata.upper()] = aid
            if icao:
                airline_map[icao.upper()] = aid
            if name:
                airline_name_map[name.lower()] = aid

        airport_map = {}
        for row in airports_rows:
            pid = row[0]
            iata = row[1]
            icao = row[2]
            if iata:
                airport_map[iata.upper()] = pid
            if icao:
                airport_map[icao.upper()] = pid

        # Resolve ids
        resolved = []
        dropped = 0
        for _, row in df_load.iterrows():
            airline_key = (str(row.get("airline", "")).upper() or None)
            airline_id = None
            if airline_key and airline_key in airline_map:
                airline_id = airline_map[airline_key]
            else:
                # try name match
                name_key = str(row.get("airline", "")).lower()
                airline_id = airline_name_map.get(name_key)

            origin_code = None
            if row.get("origin_iata"):
                origin_code = str(row.get("origin_iata")).upper()
            elif row.get("source_airport"):
                origin_code = str(row.get("source_airport")).upper()

            dest_code = None
            if row.get("dest_iata"):
                dest_code = str(row.get("dest_iata")).upper()
            elif row.get("dest_airport"):
                dest_code = str(row.get("dest_airport")).upper()

            origin_id = airport_map.get(origin_code)
            dest_id = airport_map.get(dest_code)

            if not airline_id or not origin_id or not dest_id:
                dropped += 1
                continue

            resolved.append(
                {
                    "airline_id": airline_id,
                    "flight_number": row.get("flight_number"),
                    "origin_airport_id": origin_id,
                    "destination_airport_id": dest_id,
                    "distance_km": row.get("distance_km"),
                    "aircraft_type": row.get("equipment"),
                    "source_name": row.get("source_name"),
                }
            )

        if dropped > 0:
            self.logger.warning(f"[WARN] Flights: dropped {dropped} rows due to unresolved airline/airport ids")

        if not resolved:
            return 0

        df_final = pd.DataFrame(resolved)

        count = self._bulk_insert(
            df=df_final,
            table_name="flights",
            columns=["airline_id", "flight_number", "origin_airport_id", "destination_airport_id", "distance_km", "aircraft_type", "source_name"],
            conflict_columns=["airline_id", "flight_number", "origin_airport_id", "destination_airport_id"],
            update_columns=["distance_km", "aircraft_type"],
        )

        self.stats["flights_loaded"] += count
        return count

    def generate_and_load_flight_instances(self, days: int = 7, instances_per_day: int = 1) -> int:
        """
        Generate synthetic flight instances for loaded flights and insert into `flight_instances`.

        Args:
            days: number of past days to generate (including today)
            instances_per_day: average instances per flight per day

        Returns:
            int: number of instances inserted
        """
        import random
        from datetime import datetime, timedelta, timezone

        self.logger.info("=" * 60)
        self.logger.info("Génération des flight_instances (synthétiques)...")
        self.logger.info("=" * 60)

        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT flight_id FROM flights")).fetchall()

        flight_ids = [r[0] for r in rows]
        if not flight_ids:
            self.logger.warning("[WARN] Aucune flight en base pour générer des instances")
            return 0

        instances = []
        now = datetime.now(tz=timezone.utc)
        for fid in flight_ids:
            for day in range(days):
                # schedule one or more instances per day
                for i in range(instances_per_day):
                    # pick a departure time between 04:00 and 22:00 UTC with some randomness
                    dep_hour = random.randint(4, 22)
                    dep_min = random.choice([0, 15, 30, 45])
                    flight_date = (now.date() - timedelta(days=day))
                    scheduled_departure = datetime.combine(flight_date, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(hours=dep_hour, minutes=dep_min)
                    # approximate duration: pick 1-3 hours randomly
                    duration_min = random.randint(45, 180)
                    scheduled_arrival = scheduled_departure + timedelta(minutes=duration_min)

                    instances.append(
                        {
                            "flight_id": fid,
                            "flight_date": flight_date,
                            "scheduled_departure": scheduled_departure,
                            "scheduled_arrival": scheduled_arrival,
                            "status": "scheduled",
                            "source_name": "openflights_synthetic",
                        }
                    )

        df_instances = pd.DataFrame(instances)
        if df_instances.empty:
            self.logger.warning("[WARN] Aucun flight_instance généré")
            return 0

        # Ensure datetime types
        df_instances["scheduled_departure"] = pd.to_datetime(df_instances["scheduled_departure"], utc=True)
        df_instances["scheduled_arrival"] = pd.to_datetime(df_instances["scheduled_arrival"], utc=True)

        count = self._bulk_insert(
            df=df_instances,
            table_name="flight_instances",
            columns=["flight_id", "flight_date", "scheduled_departure", "scheduled_arrival", "status", "source_name"],
            conflict_columns=["flight_id", "flight_date", "scheduled_departure"],
            update_columns=["scheduled_arrival", "status"],
        )

        self.stats["flight_instances_loaded"] += count
        return count

    def load_flight_instances(self, df: pd.DataFrame) -> int:
        """Load pre-built flight_instances DataFrame into `flight_instances` table."""
        self.logger.info("=" * 60)
        self.logger.info("Chargement des flight_instances (OpenSky / external)...")
        self.logger.info("=" * 60)

        if df is None or df.empty:
            self.logger.warning("[WARN] Aucun flight_instance à charger")
            return 0

        df_load = df.copy()
        # Ensure required columns exist
        for col in ["flight_id", "flight_date", "scheduled_departure", "scheduled_arrival", "actual_departure", "actual_arrival", "status", "source_name"]:
            if col not in df_load.columns:
                df_load[col] = None

        # Convert datetimes
        if "scheduled_departure" in df_load.columns:
            df_load["scheduled_departure"] = pd.to_datetime(df_load["scheduled_departure"], errors="coerce")
        if "scheduled_arrival" in df_load.columns:
            df_load["scheduled_arrival"] = pd.to_datetime(df_load["scheduled_arrival"], errors="coerce")

        selected_cols = [c for c in ["flight_id", "flight_date", "scheduled_departure", "scheduled_arrival", "actual_departure", "actual_arrival", "status", "source_name"] if c in df_load.columns]
        count = self._bulk_insert(
            df=df_load[selected_cols],
            table_name="flight_instances",
            columns=selected_cols,
            conflict_columns=["flight_id", "flight_date", "scheduled_departure"],
            update_columns=["scheduled_arrival", "actual_departure", "actual_arrival", "status"],
        )

        self.stats["flight_instances_loaded"] += count
        return count

    def load_schedules(self, df: pd.DataFrame) -> int:
        """
        Charge les dessertes avec insertion en bulk.

        Args:
            df: DataFrame des dessertes

        Returns:
            int: Nombre de dessertes chargées
        """
        self.logger.info("=" * 60)
        self.logger.info("Chargement des dessertes (BULK UPSERT)...")
        self.logger.info("=" * 60)

        if df.empty:
            self.logger.warning("[WARN] Aucune desserte à charger")
            return 0

        df_load = df.copy()

        # Conversion des types pour PostgreSQL
        if "departure_time" in df_load.columns:
            df_load["departure_time"] = pd.to_datetime(
                df_load["departure_time"], errors="coerce"
            )
        if "arrival_time" in df_load.columns:
            df_load["arrival_time"] = pd.to_datetime(
                df_load["arrival_time"], errors="coerce"
            )
        if "duration_min" in df_load.columns:
            df_load["duration_min"] = pd.to_numeric(
                df_load["duration_min"], errors="coerce"
            ).astype("Int64")
        if "distance_km" in df_load.columns:
            df_load["distance_km"] = pd.to_numeric(
                df_load["distance_km"], errors="coerce"
            )

        # Filtre défensif pour respecter les contraintes SQL de la table schedules.
        required_cols = [
            "train_id",
            "origin_id",
            "destination_id",
            "departure_time",
            "arrival_time",
            "duration_min",
        ]
        existing_required = [c for c in required_cols if c in df_load.columns]
        if existing_required:
            before_filter = len(df_load)
            df_load = df_load.dropna(subset=existing_required)
            if all(c in df_load.columns for c in ["origin_id", "destination_id"]):
                df_load = df_load[df_load["origin_id"] != df_load["destination_id"]]
            if all(c in df_load.columns for c in ["departure_time", "arrival_time"]):
                df_load = df_load[df_load["arrival_time"] > df_load["departure_time"]]
            if "duration_min" in df_load.columns:
                df_load = df_load[df_load["duration_min"] > 0]

            dropped = before_filter - len(df_load)
            if dropped > 0:
                self.logger.warning(
                    f"[WARN] Schedules: {dropped} lignes invalides ignorées avant chargement"
                )

        count = self._bulk_insert(
            df=df_load,
            table_name="schedules",
            columns=[
                "train_id",
                "origin_id",
                "destination_id",
                "departure_time",
                "arrival_time",
                "duration_min",
                "distance_km",
                "frequency",
                "source_name",
            ],
            conflict_columns=[
                "train_id",
                "origin_id",
                "destination_id",
                "departure_time",
            ],
            update_columns=["arrival_time", "duration_min", "distance_km", "frequency"],
        )

        self.stats["schedules_loaded"] += count
        return count

    def get_stats(self) -> Dict[str, int]:
        """
        Retourne les statistiques de chargement.

        Returns:
            Dict[str, int]: Statistiques
        """
        return self.stats.copy()

    def get_performance_report(self) -> str:
        """
        Retourne un rapport de performance détaillé.

        Returns:
            str: Rapport formaté
        """
        lines = ["\n" + "=" * 60, "RAPPORT DE PERFORMANCE", "=" * 60]

        total_rows = 0
        total_time = 0

        for table, stats in self._performance_stats.items():
            rows = stats["rows"]
            time_sec = stats["time"]
            rate = stats["rate"]
            total_rows += rows
            total_time += time_sec
            lines.append(
                f"{table:20s}: {rows:8d} lignes en {time_sec:6.2f}s ({rate:8.0f} lignes/sec)"
            )

        if total_time > 0:
            avg_rate = total_rows / total_time
            lines.append("-" * 60)
            lines.append(
                f"{'TOTAL':20s}: {total_rows:8d} lignes en {total_time:6.2f}s ({avg_rate:8.0f} lignes/sec)"
            )

        lines.append("=" * 60)
        return "\n".join(lines)

    def verify_counts(self) -> Dict[str, int]:
        """
        Vérifie les comptes dans la base de données.

        Returns:
            Dict[str, int]: Comptes par table
        """
        with self.engine.connect() as conn:
            counts = {}

            for table in ["operators", "stations", "trains", "schedules"]:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                counts[table] = result.scalar()

            return counts

    def calculate_distances(self, source_name: str | None = None) -> int:
        """
        Calcule les distances pour les schedules via Haversine (origin -> destination).

        Args:
            source_name: Nom de la source (optionnel). Si None, toutes les sources.

        Returns:
            int: Nombre de distances calculees
        """
        self.logger.info("=" * 60)
        self.logger.info("Calcul des distances (Haversine origin -> destination)...")
        self.logger.info("=" * 60)

        sql = """
            UPDATE schedules s
            SET distance_km = (
                SELECT ROUND(
                    6371.0 * acos(
                        cos(radians(o.latitude)) * cos(radians(d.latitude)) *
                        cos(radians(d.longitude) - radians(o.longitude)) +
                        sin(radians(o.latitude)) * sin(radians(d.latitude))
                    )::numeric,
                    1
                )::numeric(10,1)
                FROM stations o
                JOIN stations d ON d.station_id = s.destination_id
                WHERE o.station_id = s.origin_id
                  AND o.latitude IS NOT NULL AND o.longitude IS NOT NULL
                  AND d.latitude IS NOT NULL AND d.longitude IS NOT NULL
            )
            WHERE s.distance_km IS NULL
        """

        with self.engine.begin() as conn:
            result = conn.execute(text(sql))

            count_sql = "SELECT COUNT(*) FROM schedules WHERE distance_km IS NOT NULL"
            result_count = conn.execute(text(count_sql))
            count = result_count.scalar()

        self.logger.info(f"[OK] {count} distances calculees")
        return count
