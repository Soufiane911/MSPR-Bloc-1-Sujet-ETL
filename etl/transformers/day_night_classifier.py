"""Hybrid day/night classification for ObRail ETL.

The classification follows a strict priority order:
1. strong business rules;
2. explicit heuristic thresholds;
3. ML support for unresolved cases.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from config.logging_config import setup_logging

try:
    from sklearn.ensemble import RandomForestClassifier
except Exception:  # pragma: no cover - optional safety fallback
    RandomForestClassifier = None


class DayNightClassifier:
    """Hybrid classifier for day/night rail services."""

    NIGHT_START_HOUR = 21
    NIGHT_END_HOUR = 5
    MIN_DURATION_NIGHT = 4.0
    MIN_NIGHT_PCT = 50.0

    LABEL_DAY = "day"
    LABEL_NIGHT = "night"

    STRONG_NIGHT_KEYWORDS = (
        "nightjet",
        "intercites de nuit",
        "couchette",
        "couchettes",
        "sleeper",
        "sleeping car",
        "overnight",
        "train de nuit",
        "nuit",
        "nacht",
        "lit",
    )
    SLEEPER_ROUTE_TYPES = {105}

    ML_MIN_SAMPLES = 30
    ML_MIN_PER_CLASS = 10
    ML_NIGHT_THRESHOLD = 0.75
    ML_DAY_THRESHOLD = 0.25

    def __init__(self, enable_ml: bool = True):
        self.logger = setup_logging("transformer.classifier")
        self.enable_ml = enable_ml and RandomForestClassifier is not None
        self.stats = {
            "day": 0,
            "night": 0,
            "rule_based": 0,
            "heuristic_based": 0,
            "ml_supported": 0,
            "manual_review": 0,
        }

    @staticmethod
    def _to_timedelta(value: Any) -> Optional[pd.Timedelta]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, pd.Timedelta):
            return value
        try:
            td = pd.to_timedelta(value)
            if pd.isna(td):
                return None
            return td
        except Exception:
            return None

    @classmethod
    def _extract_schedule_metrics(
        cls, departure_time: Any, arrival_time: Any
    ) -> dict[str, Optional[float]]:
        dep_td = cls._to_timedelta(departure_time)
        arr_td = cls._to_timedelta(arrival_time)

        if dep_td is None or arr_td is None:
            return {
                "duration_hours": None,
                "night_percentage": None,
                "departure_hour": None,
                "arrival_hour": None,
                "overnight_flag": 0,
            }

        dep_minutes_total = dep_td.total_seconds() / 60
        arr_minutes_total = arr_td.total_seconds() / 60
        duration_minutes = arr_minutes_total - dep_minutes_total
        if duration_minutes <= 0:
            duration_minutes += 24 * 60

        departure_hour = int((dep_minutes_total // 60) % 24)
        arrival_hour = int((arr_minutes_total // 60) % 24)
        overnight_flag = int(
            arr_minutes_total >= 24 * 60 or arr_minutes_total <= dep_minutes_total
        )

        return {
            "duration_hours": round(duration_minutes / 60.0, 3),
            "night_percentage": cls.calc_night_percentage(departure_time, arrival_time),
            "departure_hour": departure_hour,
            "arrival_hour": arrival_hour,
            "overnight_flag": overnight_flag,
        }

    @staticmethod
    def _merge_text_fields(row: pd.Series) -> str:
        parts = [
            row.get("route_short_name"),
            row.get("route_long_name"),
            row.get("trip_headsign"),
            row.get("trip_short_name"),
            row.get("category"),
            row.get("service_name"),
        ]
        return " ".join(
            str(value).strip().lower() for value in parts if pd.notna(value)
        )

    @classmethod
    def calc_night_percentage(cls, departure_time: Any, arrival_time: Any) -> float:
        dep_td = cls._to_timedelta(departure_time)
        arr_td = cls._to_timedelta(arrival_time)

        if dep_td is None or arr_td is None:
            return 0.0

        dep_min_total = dep_td.total_seconds() / 60
        arr_min_total = arr_td.total_seconds() / 60
        duration = arr_min_total - dep_min_total
        if duration <= 0:
            duration += 24 * 60

        if duration <= 0:
            return 0.0

        current = int(dep_min_total % (24 * 60))
        night_minutes = 0
        night_start = cls.NIGHT_START_HOUR * 60
        night_end = cls.NIGHT_END_HOUR * 60

        for _ in range(int(duration)):
            if current >= night_start or current < night_end:
                night_minutes += 1
            current = (current + 1) % (24 * 60)

        return round((night_minutes / duration) * 100, 2)

    @classmethod
    def classify_simple(cls, hour: int) -> str:
        return cls.LABEL_DAY if 5 <= hour < 21 else cls.LABEL_NIGHT

    def _build_trip_metrics(self, stop_times: pd.DataFrame) -> pd.DataFrame:
        if stop_times.empty or "trip_id" not in stop_times.columns:
            return pd.DataFrame(columns=["trip_id"])

        work = stop_times.copy()
        for col in ["arrival_time", "departure_time"]:
            if col in work.columns:
                work[col] = work[col].apply(self._to_timedelta)

        sort_cols = ["trip_id"]
        if "stop_sequence" in work.columns:
            work["stop_sequence"] = pd.to_numeric(
                work["stop_sequence"], errors="coerce"
            )
            sort_cols.append("stop_sequence")
        work = work.sort_values(sort_cols)

        grouped = (
            work.groupby("trip_id", as_index=False)
            .agg(
                departure_time=("departure_time", "first"),
                arrival_time=("arrival_time", "last"),
            )
            .copy()
        )

        metrics = grouped.apply(
            lambda row: pd.Series(
                self._extract_schedule_metrics(
                    row["departure_time"], row["arrival_time"]
                )
            ),
            axis=1,
        )
        return pd.concat([grouped[["trip_id"]], metrics], axis=1)

    def _apply_rule_engine(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        out = df.copy()
        out["train_type_rule"] = pd.NA
        out["rule_reason"] = pd.NA
        out["rule_confidence"] = pd.NA

        out["metadata_text"] = out.apply(self._merge_text_fields, axis=1)
        out["route_type_numeric"] = pd.to_numeric(
            out.get("route_type"), errors="coerce"
        )
        out["is_sleeper_route"] = out["route_type_numeric"].isin(
            self.SLEEPER_ROUTE_TYPES
        )
        out["has_keyword_night"] = out["metadata_text"].apply(
            lambda value: any(
                keyword in value for keyword in self.STRONG_NIGHT_KEYWORDS
            )
        )
        out["has_couchette"] = out["metadata_text"].str.contains(
            "couchette|sleeper|sleeping car|lit", regex=True, na=False
        )
        out["has_sleeper"] = out["is_sleeper_route"] | out["has_couchette"]

        explicit_night = (
            (source_name == "back_on_track")
            | out["has_couchette"]
            | out["has_sleeper"]
            | out["has_keyword_night"]
        )
        explicit_day = (
            (~explicit_night)
            & out["duration_hours"].fillna(0).le(2.5)
            & out["night_percentage"].fillna(0).lt(15)
        )

        out.loc[explicit_night, "train_type_rule"] = self.LABEL_NIGHT
        out.loc[explicit_night, "rule_reason"] = "explicit_night_signal"
        out.loc[explicit_night, "rule_confidence"] = 1.0

        out.loc[explicit_day, "train_type_rule"] = self.LABEL_DAY
        out.loc[explicit_day, "rule_reason"] = "explicit_day_short_service"
        out.loc[explicit_day, "rule_confidence"] = 0.95
        return out

    def _apply_heuristics(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["train_type_heuristic"] = pd.NA
        out["heuristic_reason"] = pd.NA
        out["heuristic_confidence"] = pd.NA

        duration = out["duration_hours"].fillna(0)
        night_pct = out["night_percentage"].fillna(0)
        dep_hour = out["departure_hour"].fillna(-1)
        arr_hour = out["arrival_hour"].fillna(-1)
        overnight = out["overnight_flag"].fillna(0).astype(int)

        heuristic_night = (
            (duration >= self.MIN_DURATION_NIGHT) & (night_pct >= self.MIN_NIGHT_PCT)
        ) | (
            (duration >= 6.0) & (dep_hour >= 20) & ((arr_hour <= 9) | (overnight == 1))
        )
        heuristic_day = ((duration < 3.5) & (night_pct < 35) & (overnight == 0)) | (
            (duration < 6.0) & (night_pct < 20)
        )

        out.loc[heuristic_night, "train_type_heuristic"] = self.LABEL_NIGHT
        out.loc[heuristic_night, "heuristic_reason"] = "night_share_threshold"
        out.loc[heuristic_night, "heuristic_confidence"] = 0.85

        out.loc[heuristic_day, "train_type_heuristic"] = self.LABEL_DAY
        out.loc[heuristic_day, "heuristic_reason"] = "low_night_share"
        out.loc[heuristic_day, "heuristic_confidence"] = 0.75
        return out

    def _build_feature_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        features = pd.DataFrame(index=df.index)
        features["duration_hours"] = pd.to_numeric(
            df["duration_hours"], errors="coerce"
        ).fillna(0)
        features["night_percentage"] = pd.to_numeric(
            df["night_percentage"], errors="coerce"
        ).fillna(0)
        features["departure_hour"] = pd.to_numeric(
            df["departure_hour"], errors="coerce"
        ).fillna(0)
        features["arrival_hour"] = pd.to_numeric(
            df["arrival_hour"], errors="coerce"
        ).fillna(0)
        features["overnight_flag"] = pd.to_numeric(
            df["overnight_flag"], errors="coerce"
        ).fillna(0)
        features["has_couchette"] = df["has_couchette"].fillna(False).astype(int)
        features["has_sleeper"] = df["has_sleeper"].fillna(False).astype(int)
        features["is_sleeper_route"] = df["is_sleeper_route"].fillna(False).astype(int)
        features["has_keyword_night"] = (
            df["has_keyword_night"].fillna(False).astype(int)
        )
        return features

    def _fit_support_model(self, df: pd.DataFrame) -> Optional[RandomForestClassifier]:
        if not self.enable_ml:
            return None

        target_df = df.copy()
        target_df["ml_target"] = target_df["train_type_rule"]
        unresolved = (
            target_df["ml_target"].isna() & target_df["train_type_heuristic"].notna()
        )
        target_df.loc[unresolved, "ml_target"] = target_df.loc[
            unresolved, "train_type_heuristic"
        ]

        training = target_df[target_df["ml_target"].notna()].copy()
        if training.empty or training["ml_target"].nunique() < 2:
            return None

        class_counts = training["ml_target"].value_counts()
        if (
            len(training) < self.ML_MIN_SAMPLES
            or (class_counts < self.ML_MIN_PER_CLASS).any()
        ):
            return None

        model = RandomForestClassifier(
            n_estimators=200,
            random_state=42,
            min_samples_leaf=2,
            class_weight="balanced",
        )
        model.fit(self._build_feature_frame(training), training["ml_target"])
        return model

    def _apply_ml_support(
        self, df: pd.DataFrame, model: Optional[RandomForestClassifier]
    ) -> pd.DataFrame:
        out = df.copy()
        out["train_type_ml"] = pd.NA
        out["ml_night_probability"] = pd.NA

        if model is None:
            return out

        features = self._build_feature_frame(out)
        probabilities = model.predict_proba(features)
        class_labels = list(model.classes_)
        night_index = class_labels.index(self.LABEL_NIGHT)
        night_probability = probabilities[:, night_index]

        out["ml_night_probability"] = [
            round(float(value), 4) for value in night_probability
        ]
        out["train_type_ml"] = [
            self.LABEL_NIGHT if value >= 0.5 else self.LABEL_DAY
            for value in night_probability
        ]
        return out

    def _finalize_classification(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        final_type = []
        final_method = []
        final_reason = []
        final_confidence = []
        needs_review = []

        for _, row in out.iterrows():
            rule_label = row.get("train_type_rule")
            heuristic_label = row.get("train_type_heuristic")
            ml_label = row.get("train_type_ml")
            ml_probability = row.get("ml_night_probability")

            if pd.notna(rule_label):
                final_type.append(rule_label)
                final_method.append("rule")
                final_reason.append(row.get("rule_reason") or "rule_based")
                final_confidence.append(float(row.get("rule_confidence") or 1.0))
                needs_review.append(False)
                continue

            if pd.notna(heuristic_label):
                heuristic_confidence = float(row.get("heuristic_confidence") or 0.7)
                if pd.notna(ml_label) and ml_probability is not pd.NA:
                    ml_probability = float(ml_probability)
                    ml_confidence = (
                        ml_probability
                        if ml_label == self.LABEL_NIGHT
                        else 1 - ml_probability
                    )
                    if ml_label == heuristic_label:
                        final_type.append(heuristic_label)
                        final_method.append("heuristic_ml_consensus")
                        final_reason.append(
                            row.get("heuristic_reason") or "heuristic_consensus"
                        )
                        final_confidence.append(
                            round(max(heuristic_confidence, ml_confidence), 2)
                        )
                        needs_review.append(False)
                        continue
                    if heuristic_confidence >= 0.8:
                        final_type.append(heuristic_label)
                        final_method.append("heuristic_priority")
                        final_reason.append(
                            f"{row.get('heuristic_reason')}|ml_conflict"
                        )
                        final_confidence.append(
                            round(max(0.55, heuristic_confidence - 0.1), 2)
                        )
                        needs_review.append(True)
                        continue
                    if (
                        ml_probability >= self.ML_NIGHT_THRESHOLD
                        or ml_probability <= self.ML_DAY_THRESHOLD
                    ):
                        final_type.append(ml_label)
                        final_method.append("ml_support")
                        final_reason.append(f"{row.get('heuristic_reason')}|ml_support")
                        final_confidence.append(round(ml_confidence, 2))
                        needs_review.append(ml_confidence < 0.85)
                        continue

                final_type.append(heuristic_label)
                final_method.append("heuristic")
                final_reason.append(row.get("heuristic_reason") or "heuristic_based")
                final_confidence.append(round(heuristic_confidence, 2))
                needs_review.append(heuristic_confidence < 0.8)
                continue

            if pd.notna(ml_label) and ml_probability is not pd.NA:
                ml_probability = float(ml_probability)
                if (
                    ml_probability >= self.ML_NIGHT_THRESHOLD
                    or ml_probability <= self.ML_DAY_THRESHOLD
                ):
                    ml_confidence = (
                        ml_probability
                        if ml_label == self.LABEL_NIGHT
                        else 1 - ml_probability
                    )
                    final_type.append(ml_label)
                    final_method.append("ml_support")
                    final_reason.append("ml_resolved_ambiguous_case")
                    final_confidence.append(round(ml_confidence, 2))
                    needs_review.append(ml_confidence < 0.85)
                    continue

            final_type.append(self.LABEL_DAY)
            final_method.append("default_day")
            final_reason.append("insufficient_night_evidence")
            final_confidence.append(0.5)
            needs_review.append(True)

        out["train_type"] = final_type
        out["classification_method"] = final_method
        out["classification_reason"] = final_reason
        out["classification_confidence"] = final_confidence
        out["needs_manual_review"] = needs_review
        return out

    def classify_gtfs_trips(
        self,
        stop_times: pd.DataFrame,
        trips: pd.DataFrame,
        routes: Optional[pd.DataFrame] = None,
        source_name: str = "unknown",
    ) -> pd.DataFrame:
        """Classify GTFS trips with rules, heuristics and optional ML support."""

        if trips.empty or "trip_id" not in trips.columns:
            return pd.DataFrame(columns=["trip_id", "train_type"])

        trip_metrics = self._build_trip_metrics(stop_times)
        base = trips.copy()
        base["trip_id"] = base["trip_id"].astype(str)

        if routes is not None and not routes.empty and "route_id" in base.columns:
            route_cols = [
                col
                for col in [
                    "route_id",
                    "route_short_name",
                    "route_long_name",
                    "route_type",
                    "route_type_label",
                ]
                if col in routes.columns
            ]
            base = base.merge(routes[route_cols].copy(), on="route_id", how="left")

        classified = base.merge(trip_metrics, on="trip_id", how="left")
        classified = self._apply_rule_engine(classified, source_name)
        classified = self._apply_heuristics(classified)
        model = self._fit_support_model(classified)
        classified = self._apply_ml_support(classified, model)
        classified = self._finalize_classification(classified)

        counts = classified["train_type"].value_counts()
        self.stats["day"] = int(counts.get(self.LABEL_DAY, 0))
        self.stats["night"] = int(counts.get(self.LABEL_NIGHT, 0))
        self.stats["rule_based"] = int(
            (classified["classification_method"] == "rule").sum()
        )
        self.stats["heuristic_based"] = int(
            classified["classification_method"]
            .isin(["heuristic", "heuristic_priority", "heuristic_ml_consensus"])
            .sum()
        )
        self.stats["ml_supported"] = int(
            (classified["classification_method"] == "ml_support").sum()
        )
        self.stats["manual_review"] = int(classified["needs_manual_review"].sum())

        self.logger.info("=" * 60)
        self.logger.info(f"Classification hybride terminee pour {source_name}")
        self.logger.info(f"  Day: {self.stats['day']}")
        self.logger.info(f"  Night: {self.stats['night']}")
        self.logger.info(f"  Rule based: {self.stats['rule_based']}")
        self.logger.info(f"  Heuristic based: {self.stats['heuristic_based']}")
        self.logger.info(f"  ML supported: {self.stats['ml_supported']}")
        self.logger.info(f"  Review required: {self.stats['manual_review']}")
        self.logger.info("=" * 60)

        return classified[
            [
                "trip_id",
                "train_type",
                "train_type_rule",
                "train_type_heuristic",
                "train_type_ml",
                "classification_method",
                "classification_reason",
                "classification_confidence",
                "ml_night_probability",
                "night_percentage",
                "needs_manual_review",
            ]
        ].copy()

    def classify_from_timedelta(
        self,
        df: pd.DataFrame,
        timedelta_column: str = "departure_time",
        output_column: str = "train_type",
    ) -> pd.DataFrame:
        """Legacy helper based on departure hour only."""
        df = df.copy()

        def timedelta_to_hour(value: Any) -> Optional[int]:
            td = self._to_timedelta(value)
            if td is None:
                return None
            return int((td.total_seconds() // 3600) % 24)

        df["departure_hour"] = df[timedelta_column].apply(timedelta_to_hour)
        df[output_column] = df["departure_hour"].apply(
            lambda value: self.classify_simple(int(value))
            if pd.notna(value)
            else self.LABEL_DAY
        )
        df = df.drop(columns=["departure_hour"])
        return df

    def get_stats(self) -> dict[str, int]:
        return self.stats.copy()
