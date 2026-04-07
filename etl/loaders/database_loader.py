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
