"""
Module de fusion des données pour l'ETL ObRail Europe.

Fusionne les données de différentes sources en un jeu de données unifié.
"""

import pandas as pd
from typing import Dict, List
from config.logging_config import setup_logging


class DataMerger:
    """
    Classe de fusion des données de multiples sources.

    Responsabilités:
    - Fusion des données de différents opérateurs
    - Harmonisation des schémas
    - Gestion des conflits d'identifiants
    - Création des relations entre entités
    """

    # Mapping source -> country basé sur les sources connues
    SOURCE_COUNTRY_MAP = {
        "sncf_intercites": "FR",
        "db_fernverkehr": "DE",
        "renfe": "ES",
        "trenitalia": "IT",
        "cff_sbb": "CH",
        "sncb": "BE",
    }

    def __init__(self):
        """Initialise le fusionneur de données."""
        self.logger = setup_logging("transformer.merger")
        self.sources_data = {}

    def add_source(self, source_name: str, data: Dict[str, pd.DataFrame]):
        """
        Ajoute une source de données.

        Args:
            source_name: Nom de la source
            data: Dictionnaire des DataFrames de la source
        """
        self.sources_data[source_name] = data
        self.logger.info(f"Source ajoutée: {source_name}")

    @staticmethod
    def _coalesce_columns(
        df: pd.DataFrame, target: str, candidates: List[str]
    ) -> pd.DataFrame:
        """Crée une colonne cible en prenant la première valeur non nulle."""
        available = [col for col in candidates if col in df.columns]
        if not available:
            return df

        series = df[available[0]]
        for col in available[1:]:
            series = series.combine_first(df[col])

        df[target] = series
        return df

    @staticmethod
    def _parse_duration_minutes(series: pd.Series) -> pd.Series:
        """Convertit des durées ISO ou HH:MM:SS en minutes."""
        dt_series = pd.to_datetime(series, errors="coerce", utc=True)
        parsed_from_dt = (
            dt_series.dt.hour * 60 + dt_series.dt.minute + (dt_series.dt.second / 60)
        )
        parsed_from_td = (
            pd.to_timedelta(series.astype(str), errors="coerce").dt.total_seconds() / 60
        )
        return parsed_from_dt.fillna(parsed_from_td)

    def merge_operators(self) -> pd.DataFrame:
        """
        Fusionne les opérateurs de toutes les sources.

        Returns:
            pd.DataFrame: Opérateurs fusionnés
        """
        self.logger.info("Fusion des opérateurs...")

        all_operators = []

        for source_name, data in self.sources_data.items():
            if "agency" in data or "agencies" in data:
                # GTFS: agency.txt, Back-on-Track: agencies.json
                agency_key = "agency" if "agency" in data else "agencies"
                df = data[agency_key].copy()

                # Normalisation des colonnes
                if "agency_id" in df.columns:
                    df["source_agency_id"] = df["agency_id"].astype(str)
                    df["agency_id"] = f"{source_name}_" + df["agency_id"].astype(str)

                df["source_name"] = source_name
                all_operators.append(df)

        if all_operators:
            merged = pd.concat(all_operators, ignore_index=True)

            # Suppression des doublons par nom
            merged = merged.drop_duplicates(subset=["agency_name"], keep="first")

            # Réindexation
            merged["operator_id"] = range(1, len(merged) + 1)

            self.logger.info(
                f"[OK] Operateurs fusionnes: {len(merged)} operateurs uniques"
            )
            return merged

        return pd.DataFrame()

    def merge_stations(self) -> pd.DataFrame:
        """
        Fusionne les gares de toutes les sources.

        Returns:
            pd.DataFrame: Gares fusionnées
        """
        self.logger.info("Fusion des gares...")

        all_stations = []

        for source_name, data in self.sources_data.items():
            if "stops" in data:
                df = data["stops"].copy()

                if "stop_id" in df.columns:
                    df["source_stop_id"] = df["stop_id"].astype(str)
                    df["stop_id"] = f"{source_name}_" + df["stop_id"].astype(str)

                # Gestion du pays
                if "country" not in df.columns:
                    df["country"] = self.SOURCE_COUNTRY_MAP.get(source_name, None)

                df["source_name"] = source_name
                all_stations.append(df)

        if all_stations:
            merged = pd.concat(all_stations, ignore_index=True)

            # Suppression des doublons par stop_id uniquement
            # CORRECTION: Suppression de la dedup stop_name qui cassait les références
            if "stop_id" in merged.columns:
                before = len(merged)
                merged = merged.drop_duplicates(subset=["stop_id"], keep="first")
                self.logger.info(
                    f"[DEDUP] Stations: {before} → {len(merged)} (supprimé {before - len(merged)} doublons stop_id)"
                )

            # Réindexation
            merged["station_id"] = range(1, len(merged) + 1)

            self.logger.info(f"[OK] Gares fusionnees: {len(merged)} gares uniques")
            return merged

        return pd.DataFrame()

    def merge_trains(self, operators: pd.DataFrame) -> pd.DataFrame:
        """
        Fusionne les trains de toutes les sources.

        Args:
            operators: DataFrame des opérateurs fusionnés

        Returns:
            pd.DataFrame: Trains fusionnés
        """
        self.logger.info("Fusion des trains...")

        all_trains = []

        for source_name, data in self.sources_data.items():
            if "routes" in data and "trips" in data:
                # Fusion routes + trips pour GTFS
                routes = data["routes"].copy()
                trips = data["trips"].copy()

                # Merge routes et trips
                df = trips.merge(
                    routes, on="route_id", how="left", suffixes=("_trip", "_route")
                )

                # Harmonisation des colonnes dupliquees, notamment pour Back-on-Track
                df = self._coalesce_columns(
                    df, "agency_id", ["agency_id_route", "agency_id_trip", "agency_id"]
                )
                df = self._coalesce_columns(
                    df,
                    "train_type",
                    ["train_type_trip", "train_type_route", "train_type"],
                )
                df = self._coalesce_columns(
                    df,
                    "train_type_rule",
                    [
                        "train_type_rule_trip",
                        "train_type_rule_route",
                        "train_type_rule",
                    ],
                )
                df = self._coalesce_columns(
                    df,
                    "train_type_heuristic",
                    [
                        "train_type_heuristic_trip",
                        "train_type_heuristic_route",
                        "train_type_heuristic",
                    ],
                )
                df = self._coalesce_columns(
                    df,
                    "train_type_ml",
                    ["train_type_ml_trip", "train_type_ml_route", "train_type_ml"],
                )
                df = self._coalesce_columns(
                    df,
                    "classification_method",
                    [
                        "classification_method_trip",
                        "classification_method_route",
                        "classification_method",
                    ],
                )
                df = self._coalesce_columns(
                    df,
                    "classification_reason",
                    [
                        "classification_reason_trip",
                        "classification_reason_route",
                        "classification_reason",
                    ],
                )
                df = self._coalesce_columns(
                    df,
                    "classification_confidence",
                    [
                        "classification_confidence_trip",
                        "classification_confidence_route",
                        "classification_confidence",
                    ],
                )
                df = self._coalesce_columns(
                    df,
                    "ml_night_probability",
                    [
                        "ml_night_probability_trip",
                        "ml_night_probability_route",
                        "ml_night_probability",
                    ],
                )
                df = self._coalesce_columns(
                    df,
                    "night_percentage",
                    [
                        "night_percentage_trip",
                        "night_percentage_route",
                        "night_percentage",
                    ],
                )
                df = self._coalesce_columns(
                    df,
                    "needs_manual_review",
                    [
                        "needs_manual_review_trip",
                        "needs_manual_review_route",
                        "needs_manual_review",
                    ],
                )

                # Normalisation
                if "trip_id" in df.columns:
                    df["source_trip_id"] = df["trip_id"].astype(str)
                    df["trip_id"] = f"{source_name}_" + df["trip_id"].astype(str)

                if "route_id" in df.columns:
                    df["source_route_id"] = df["route_id"].astype(str)
                    df["route_id"] = f"{source_name}_" + df["route_id"].astype(str)

                if "agency_id" in df.columns:
                    df["source_agency_id"] = df["agency_id"].astype(str)
                    df["agency_id"] = f"{source_name}_" + df["agency_id"].astype(str)

                df["source_name"] = source_name
                if "train_type" not in df.columns:
                    df["train_type"] = "day"
                all_trains.append(df)

            elif "trips" in data:
                # CORRECTION: Back-on-Track avec trips (nouveau processus)
                # ou GTFS-like structure avec trips déjà présents
                df = data["trips"].copy()

                # Ajouter les infos routes si disponibles
                if "routes" in data:
                    df = df.merge(data["routes"], on="route_id", how="left")

                if "trip_id" in df.columns:
                    df["source_trip_id"] = df["trip_id"].astype(str)
                    df["trip_id"] = f"{source_name}_" + df["trip_id"].astype(str)

                if "route_id" in df.columns:
                    df["source_route_id"] = df["route_id"].astype(str)
                    df["route_id"] = f"{source_name}_" + df["route_id"].astype(str)

                if "agency_id" in df.columns:
                    df["source_agency_id"] = df["agency_id"].astype(str)
                    df["agency_id"] = f"{source_name}_" + df["agency_id"].astype(str)

                df["source_name"] = source_name
                # Utiliser le train_type déjà défini dans process_back_on_track
                if "train_type" not in df.columns:
                    df["train_type"] = "night"
                all_trains.append(df)

            elif "routes" in data:
                # Fallback: routes sans trips (ancien comportement)
                df = data["routes"].copy()

                if "route_id" in df.columns:
                    df["source_route_id"] = df["route_id"].astype(str)
                    df["route_id"] = f"{source_name}_" + df["route_id"].astype(str)
                    # Fallback: mapper route_id sur trip_id si pas de trips data
                    if "trip_id" not in df.columns:
                        df["trip_id"] = df["route_id"]

                if "agency_id" in df.columns:
                    df["source_agency_id"] = df["agency_id"].astype(str)
                    df["agency_id"] = f"{source_name}_" + df["agency_id"].astype(str)

                df["source_name"] = source_name
                df["train_type"] = "night"
                all_trains.append(df)

        if all_trains:
            merged = pd.concat(all_trains, ignore_index=True)
            if "train_type" not in merged.columns:
                merged["train_type"] = "day"
            self.logger.info(
                f"Types de trains avant mapping: {merged['train_type'].value_counts().to_dict()}"
            )

            # Mapping des opérateurs
            if operators is not None and not operators.empty:
                op_map = operators.set_index("agency_id")["operator_id"].to_dict()
                merged["operator_id"] = merged["agency_id"].map(op_map)
                self.logger.info(
                    f"Types de trains après mapping: {merged.groupby('train_type')['operator_id'].count().to_dict()}"
                )

                # Gestion du train_type (obligatoire en BDD)
                if (
                    "train_type" not in merged.columns
                    or merged["train_type"].isna().any()
                ):
                    if "train_type" not in merged.columns:
                        merged["train_type"] = "day"
                    else:
                        merged["train_type"] = merged["train_type"].fillna("day")

                # Suppression des lignes sans opérateur (obligatoire en BDD)
                merged = merged.dropna(subset=["operator_id"])
                merged["operator_id"] = merged["operator_id"].astype(int)

            # Suppression des doublons
            subset = []
            if "trip_id" in merged.columns:
                subset.append("trip_id")
            elif "route_id" in merged.columns:
                subset.append("route_id")

            if subset:
                merged = merged.drop_duplicates(subset=subset, keep="first")

            # Réindexation
            merged["train_id"] = range(1, len(merged) + 1)

            self.logger.info(f"[OK] Trains fusionnes: {len(merged)} trains uniques")
            return merged

        return pd.DataFrame()

    def merge_schedules(
        self, trains: pd.DataFrame, stations: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Fusionne les horaires de toutes les sources.

        Corrige le bug de mapping IDs: préfixe trip_id et stop_ids avec le
        source_name avant la jointure, comme le fait merge_stations/trains.
        """
        self.logger.info("Fusion des horaires...")
        all_schedules = []

        for source_name, data in self.sources_data.items():
            if "stop_times" in data:
                df = data["stop_times"].copy()
                df["source_name"] = source_name
                all_schedules.append(df)
            elif "trip_stop" in data:
                df = data["trip_stop"].copy()
                df["source_name"] = source_name
                all_schedules.append(df)

        if not all_schedules:
            self.logger.warning("Aucune donnée d'horaire disponible")
            return pd.DataFrame()

        merged = pd.concat(all_schedules, ignore_index=True)

        # Agrégation par trajet (groupby source + trip_id pour éviter les collisions)
        if "stop_sequence" in merged.columns:
            merged = merged.sort_values(["source_name", "trip_id", "stop_sequence"])

            agg_df = merged.groupby(["source_name", "trip_id"], as_index=False).agg(
                origin_uic=("stop_id", "first"),
                destination_uic=("stop_id", "last"),
                departure_time=("departure_time", "first"),
                arrival_time=("arrival_time", "last"),
            )
            merged = agg_df
        elif "origin_uic" not in merged.columns:
            merged = merged.rename(
                columns={
                    "origin_id": "origin_uic",
                    "destination_id": "destination_uic",
                }
            )

        # Mapping train_id : préfixer trip_id avec source_name
        if trains is not None and not trains.empty:
            train_id_map = trains.set_index("trip_id")["train_id"].to_dict()
            merged["trip_id_prefixed"] = (
                merged["source_name"].astype(str) + "_" + merged["trip_id"].astype(str)
            )
            merged["train_id"] = merged["trip_id_prefixed"].map(train_id_map)

            train_enrichment_cols = ["trip_id"]
            for column in ["duration", "distance", "distance_km"]:
                if column in trains.columns:
                    train_enrichment_cols.append(column)

            train_enrichment = trains[train_enrichment_cols].drop_duplicates(
                subset=["trip_id"]
            )
            merged = merged.merge(
                train_enrichment,
                left_on="trip_id_prefixed",
                right_on="trip_id",
                how="left",
                suffixes=("", "_train"),
            )

        # Mapping station_id : préfixer stop_ids avec source_name
        if stations is not None and not stations.empty:
            station_id_map = stations.set_index("stop_id")["station_id"].to_dict()
            merged["origin_prefixed"] = (
                merged["source_name"].astype(str)
                + "_"
                + merged["origin_uic"].astype(str)
            )
            merged["destination_prefixed"] = (
                merged["source_name"].astype(str)
                + "_"
                + merged["destination_uic"].astype(str)
            )
            merged["origin_id"] = merged["origin_prefixed"].map(station_id_map)
            merged["destination_id"] = merged["destination_prefixed"].map(
                station_id_map
            )

        # Calcul de la durée
        if "duration_min" not in merged.columns:
            try:
                dep_dt = pd.to_datetime(
                    merged["departure_time"], errors="coerce", utc=True
                )
                arr_dt = pd.to_datetime(
                    merged["arrival_time"], errors="coerce", utc=True
                )
                diff = (arr_dt - dep_dt).dt.total_seconds() / 60

                fallback_dep = pd.to_timedelta(
                    merged["departure_time"].astype(str), errors="coerce"
                )
                fallback_arr = pd.to_timedelta(
                    merged["arrival_time"].astype(str), errors="coerce"
                )
                fallback_diff = (fallback_arr - fallback_dep).dt.total_seconds() / 60
                diff = diff.fillna(fallback_diff)

                if "duration" in merged.columns:
                    diff = diff.fillna(self._parse_duration_minutes(merged["duration"]))

                # Trains de nuit : durée négative → ajouter 24h
                diff = diff.where(diff >= 0, diff + 24 * 60)
                merged["duration_min"] = diff.round().astype("Int64")
            except Exception as e:
                self.logger.warning(f"Calcul durée échoué: {e}")
                merged["duration_min"] = None

        if "distance_km" not in merged.columns:
            if "distance" in merged.columns:
                merged["distance_km"] = pd.to_numeric(
                    merged["distance"], errors="coerce"
                )
            else:
                merged["distance_km"] = None
        if "frequency" not in merged.columns:
            merged["frequency"] = None

        # Nettoyage
        merged = merged.dropna(
            subset=["train_id", "origin_id", "destination_id", "duration_min"]
        )
        merged = merged[merged["duration_min"] > 0]
        merged = merged[merged["origin_id"] != merged["destination_id"]]
        merged[["train_id", "origin_id", "destination_id"]] = merged[
            ["train_id", "origin_id", "destination_id"]
        ].astype(int)

        self.logger.info(f"[OK] Horaires fusionnes: {len(merged)} dessertes")
        return merged

    def get_summary(self) -> Dict[str, int]:
        """
        Retourne un résumé des données fusionnées.

        Returns:
            Dict[str, int]: Statistiques
        """
        return {
            "sources": len(self.sources_data),
            "operators": sum(
                1
                for d in self.sources_data.values()
                if "agency" in d or "agencies" in d
            ),
            "stations": sum(1 for d in self.sources_data.values() if "stops" in d),
            "routes": sum(1 for d in self.sources_data.values() if "routes" in d),
        }
