"""
Module de validation métier des données pour l'ETL ObRail Europe.

Applique les règles métier AVANT le chargement en base de données.
Chaque entité (operators, stations, trains, schedules) possède ses propres
règles de validation. Les lignes invalides sont séparées et journalisées.
"""

import re

import pandas as pd

from config.canonical_schema import validate_dataframe
from config.logging_config import setup_logging
from config.quality_report import quality_report


# Pattern ISO 3166-1 alpha-2 ou alpha-3 (codes pays)
_COUNTRY_CODE_RE = re.compile(r"^[A-Z]{2,3}$")


class BusinessValidator:
    """
    Valide les données contre les règles métier avant chargement.

    Chaque méthode ``validate_*`` retourne un tuple
    ``(valid_df, rejected_df)`` afin de séparer les lignes conformes
    des lignes rejetées.

    Responsabilités :
    - Validation structurelle via le schéma canonique
    - Application des règles métier spécifiques par entité
    - Enregistrement des métriques dans le rapport qualité
    - Journalisation des rejets avec raisons détaillées
    """

    def __init__(self):
        """Initialise le validateur métier."""
        self.logger = setup_logging("transformer.validator")

    # ------------------------------------------------------------------
    # Helpers internes
    # ------------------------------------------------------------------

    def _split_valid_rejected(
        self, df: pd.DataFrame, mask: pd.Series
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Sépare un DataFrame en lignes valides / rejetées selon *mask*."""
        valid_df = df.loc[mask].copy()
        rejected_df = df.loc[~mask].copy()
        return valid_df, rejected_df

    def _log_rejections(
        self, entity: str, rejected_df: pd.DataFrame, reasons: dict[str, int]
    ) -> None:
        """Journalise les rejets avec raisons."""
        total = len(rejected_df)
        if total == 0:
            return
        self.logger.warning(
            f"[VALIDATION] {entity} — {total} lignes rejetées"
        )
        for reason, count in reasons.items():
            if count > 0:
                self.logger.warning(f"  - {reason} : {count}")

    def _record_metrics(
        self,
        entity: str,
        rows_before: int,
        rows_after: int,
        reasons: dict[str, int],
        schema_report: dict,
    ) -> None:
        """Enregistre les métriques dans le rapport qualité."""
        source_name = "validation"
        rows_rejected = rows_before - rows_after

        # Métriques de validation (violations structurelles)
        quality_report.record_validation(
            source_name=source_name,
            entity=entity,
            null_violations=sum(schema_report.get("null_violations", {}).values()),
            type_mismatches=len(schema_report.get("type_mismatches", {})),
        )

        # Métriques de transformation (rejets métier)
        quality_report.record_transformation(
            source_name=source_name,
            entity=entity,
            rows_before=rows_before,
            rows_after=rows_after,
            nulls_filled=0,
            rows_rejected=rows_rejected,
            rejection_reasons=reasons,
        )

    # ------------------------------------------------------------------
    # Validation par entité
    # ------------------------------------------------------------------

    def validate_operators(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Valide les opérateurs ferroviaires.

        Règles métier :
        - ``name`` ne doit pas être vide ou constitué d'espaces
        - ``country`` (si présent) doit être un code ISO alpha-2

        Returns:
            Tuple (valid_df, rejected_df)
        """
        entity = "operators"
        self.logger.info(f"[VALIDATION] Validation des {entity} ({len(df)} lignes)...")

        if df.empty:
            return df.copy(), df.iloc[0:0].copy()

        # 1. Validation structurelle
        schema_report = validate_dataframe(df, entity)

        # 2. Règles métier
        reasons: dict[str, int] = {}
        mask = pd.Series(True, index=df.index)

        # name non vide
        if "name" in df.columns:
            empty_name = df["name"].isna() | (df["name"].astype(str).str.strip() == "")
            reasons["nom vide ou manquant"] = int(empty_name.sum())
            mask &= ~empty_name

        # country ISO alpha-2 (si présent et non null)
        if "country" in df.columns:
            has_country = df["country"].notna() & (
                df["country"].astype(str).str.strip() != ""
            )
            bad_country = has_country & ~df["country"].astype(str).str.strip().str.match(
                r"^[A-Z]{2}$"
            )
            reasons["code pays invalide"] = int(bad_country.sum())
            mask &= ~bad_country

        valid_df, rejected_df = self._split_valid_rejected(df, mask)

        self._log_rejections(entity, rejected_df, reasons)
        self._record_metrics(entity, len(df), len(valid_df), reasons, schema_report)

        self.logger.info(
            f"[VALIDATION] {entity} — {len(valid_df)}/{len(df)} lignes valides"
        )
        return valid_df, rejected_df

    def validate_stations(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Valide les gares et arrêts.

        Règles métier :
        - ``name`` ne doit pas être vide
        - ``latitude`` entre -90 et 90 (si renseignée)
        - ``longitude`` entre -180 et 180 (si renseignée)
        - ``country`` (si présent) doit être un code ISO alpha-2 ou alpha-3

        Returns:
            Tuple (valid_df, rejected_df)
        """
        entity = "stations"
        self.logger.info(f"[VALIDATION] Validation des {entity} ({len(df)} lignes)...")

        if df.empty:
            return df.copy(), df.iloc[0:0].copy()

        # 1. Validation structurelle
        schema_report = validate_dataframe(df, entity)

        # 2. Règles métier
        reasons: dict[str, int] = {}
        mask = pd.Series(True, index=df.index)

        # name non vide
        if "name" in df.columns:
            empty_name = df["name"].isna() | (df["name"].astype(str).str.strip() == "")
            reasons["nom vide ou manquant"] = int(empty_name.sum())
            mask &= ~empty_name

        # latitude entre -90 et 90
        if "latitude" in df.columns:
            lat = pd.to_numeric(df["latitude"], errors="coerce")
            has_lat = lat.notna()
            bad_lat = has_lat & ~lat.between(-90, 90)
            reasons["latitude hors limites"] = int(bad_lat.sum())
            mask &= ~bad_lat

        # longitude entre -180 et 180
        if "longitude" in df.columns:
            lon = pd.to_numeric(df["longitude"], errors="coerce")
            has_lon = lon.notna()
            bad_lon = has_lon & ~lon.between(-180, 180)
            reasons["longitude hors limites"] = int(bad_lon.sum())
            mask &= ~bad_lon

        # country ISO alpha-2 ou alpha-3
        if "country" in df.columns:
            has_country = df["country"].notna() & (
                df["country"].astype(str).str.strip() != ""
            )
            bad_country = has_country & ~df["country"].astype(str).str.strip().str.match(
                r"^[A-Z]{2,3}$"
            )
            reasons["code pays invalide"] = int(bad_country.sum())
            mask &= ~bad_country

        valid_df, rejected_df = self._split_valid_rejected(df, mask)

        self._log_rejections(entity, rejected_df, reasons)
        self._record_metrics(entity, len(df), len(valid_df), reasons, schema_report)

        self.logger.info(
            f"[VALIDATION] {entity} — {len(valid_df)}/{len(df)} lignes valides"
        )
        return valid_df, rejected_df

    def validate_trains(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Valide les trains et lignes.

        Règles métier :
        - ``train_number`` ne doit pas être vide
        - ``operator_id`` doit être un entier positif
        - ``train_type`` doit valoir ``'day'`` ou ``'night'``

        Returns:
            Tuple (valid_df, rejected_df)
        """
        entity = "trains"
        self.logger.info(f"[VALIDATION] Validation des {entity} ({len(df)} lignes)...")

        if df.empty:
            return df.copy(), df.iloc[0:0].copy()

        # 1. Validation structurelle
        schema_report = validate_dataframe(df, entity)

        # 2. Règles métier
        reasons: dict[str, int] = {}
        mask = pd.Series(True, index=df.index)

        # train_number non vide
        if "train_number" in df.columns:
            empty_tn = df["train_number"].isna() | (
                df["train_number"].astype(str).str.strip() == ""
            )
            reasons["numéro de train vide"] = int(empty_tn.sum())
            mask &= ~empty_tn

        # operator_id entier positif
        if "operator_id" in df.columns:
            op_id = pd.to_numeric(df["operator_id"], errors="coerce")
            bad_op = op_id.isna() | (op_id <= 0)
            reasons["operator_id invalide"] = int(bad_op.sum())
            mask &= ~bad_op

        # train_type doit être 'day' ou 'night'
        if "train_type" in df.columns:
            valid_types = {"day", "night"}
            bad_type = ~df["train_type"].astype(str).str.strip().str.lower().isin(
                valid_types
            )
            reasons["type de train invalide"] = int(bad_type.sum())
            mask &= ~bad_type

        valid_df, rejected_df = self._split_valid_rejected(df, mask)

        self._log_rejections(entity, rejected_df, reasons)
        self._record_metrics(entity, len(df), len(valid_df), reasons, schema_report)

        self.logger.info(
            f"[VALIDATION] {entity} — {len(valid_df)}/{len(df)} lignes valides"
        )
        return valid_df, rejected_df

    def validate_schedules(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Valide les dessertes / horaires.

        Règles métier :
        - ``train_id``, ``origin_id``, ``destination_id`` doivent être positifs
        - ``origin_id`` ≠ ``destination_id``
        - ``duration_min`` > 0
        - ``duration_min`` < 2880 (max 48 h pour les trains de nuit)

        Returns:
            Tuple (valid_df, rejected_df)
        """
        entity = "schedules"
        self.logger.info(f"[VALIDATION] Validation des {entity} ({len(df)} lignes)...")

        if df.empty:
            return df.copy(), df.iloc[0:0].copy()

        # 1. Validation structurelle
        schema_report = validate_dataframe(df, entity)

        # 2. Règles métier
        reasons: dict[str, int] = {}
        mask = pd.Series(True, index=df.index)

        # IDs positifs
        for id_col in ("train_id", "origin_id", "destination_id"):
            if id_col in df.columns:
                val = pd.to_numeric(df[id_col], errors="coerce")
                bad = val.isna() | (val <= 0)
                reasons[f"{id_col} invalide"] = int(bad.sum())
                mask &= ~bad

        # origin_id != destination_id
        if "origin_id" in df.columns and "destination_id" in df.columns:
            same_station = (
                pd.to_numeric(df["origin_id"], errors="coerce")
                == pd.to_numeric(df["destination_id"], errors="coerce")
            )
            reasons["origine = destination"] = int(same_station.sum())
            mask &= ~same_station

        # duration_min > 0 et < 2880
        if "duration_min" in df.columns:
            dur = pd.to_numeric(df["duration_min"], errors="coerce")
            bad_dur = dur.isna() | (dur <= 0) | (dur >= 2880)
            reasons["durée invalide"] = int(bad_dur.sum())
            mask &= ~bad_dur

        # departure_time et arrival_time ne doivent pas être null
        for time_col in ("departure_time", "arrival_time"):
            if time_col in df.columns:
                null_time = df[time_col].isna()
                reasons[f"{time_col} manquant"] = int(null_time.sum())
                mask &= ~null_time

        valid_df, rejected_df = self._split_valid_rejected(df, mask)

        self._log_rejections(entity, rejected_df, reasons)
        self._record_metrics(entity, len(df), len(valid_df), reasons, schema_report)

        self.logger.info(
            f"[VALIDATION] {entity} — {len(valid_df)}/{len(df)} lignes valides"
        )
        return valid_df, rejected_df

    # ------------------------------------------------------------------
    # Point d'entrée global
    # ------------------------------------------------------------------

    def validate_all(self, data: dict) -> dict:
        """
        Valide toutes les entités d'un dictionnaire de DataFrames.

        Args:
            data: Dictionnaire ``{entity_name: pd.DataFrame}``
                  Clés attendues : operators, stations, trains, schedules

        Returns:
            Dictionnaire avec les DataFrames validés (lignes conformes uniquement)
        """
        self.logger.info("=" * 60)
        self.logger.info("[VALIDATION] Début de la validation métier globale")
        self.logger.info("=" * 60)

        validators = {
            "operators": self.validate_operators,
            "stations": self.validate_stations,
            "trains": self.validate_trains,
            "schedules": self.validate_schedules,
        }

        validated: dict[str, pd.DataFrame] = {}

        for entity, validate_fn in validators.items():
            if entity in data and isinstance(data[entity], pd.DataFrame):
                valid_df, rejected_df = validate_fn(data[entity])
                validated[entity] = valid_df

                if len(rejected_df) > 0:
                    self.logger.warning(
                        f"[VALIDATION] {entity} — {len(rejected_df)} lignes "
                        f"rejetées sur {len(data[entity])}"
                    )
            else:
                self.logger.warning(
                    f"[VALIDATION] {entity} — absent ou invalide, ignoré"
                )

        self.logger.info("=" * 60)
        self.logger.info("[VALIDATION] Validation métier terminée")
        self.logger.info("=" * 60)

        return validated
