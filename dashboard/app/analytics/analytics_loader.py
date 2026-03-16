"""Analytics data loader for the Streamlit dashboard."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from database import engine
except ImportError:
    engine = None


DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "processed"
FINAL_CSV = DATA_DIR / "OBRAIL_COMPLETE_FINAL.csv"
FALLBACK_CSV = DATA_DIR / "all_sources_combined.csv"

_ANALYTICS_CACHE: Optional[pd.DataFrame] = None
_ANALYTICS_MODE = "unknown"


def _to_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def _normalize_train_type(value: object) -> str:
    return "night" if "night" in str(value).strip().lower() else "day"


def _prepare_csv_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    if "day_night_classification" in out.columns:
        out["train_type_normalized"] = out["day_night_classification"].apply(
            _normalize_train_type
        )
    elif "train_type" in out.columns:
        out["train_type_normalized"] = out["train_type"].apply(_normalize_train_type)
    else:
        out["train_type_normalized"] = "day"

    out = _to_numeric(
        out,
        [
            "duration_hours",
            "duration_min",
            "distance_km",
            "co2_vs_plane_kg",
            "co2_saving_kg",
        ],
    )

    if "duration_hours" in out.columns and "duration_min" not in out.columns:
        out["duration_min"] = out["duration_hours"] * 60

    if "co2_vs_plane_kg" in out.columns and "co2_saving_kg" not in out.columns:
        out["co2_saving_kg"] = out["co2_vs_plane_kg"]

    if "co2_saving_kg" not in out.columns and "distance_km" in out.columns:
        out["co2_saving_kg"] = (out["distance_km"] * 0.215) - (
            out["distance_km"] * 0.005
        )

    rename_map = {
        "operator_country": "operator_country",
        "operator": "operator",
        "website": "website",
        "departure_station": "departure_station",
        "arrival_station": "arrival_station",
        "train_id": "train_id",
        "train_number": "train_number",
    }
    for source_column, target_column in rename_map.items():
        if source_column not in out.columns:
            out[target_column] = pd.NA

    return out


def _try_load_from_db() -> Optional[pd.DataFrame]:
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            schedules_exists = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'schedules'
                    ) AS ok
                    """
                )
            ).scalar()

            if not schedules_exists:
                return None

            schedules_count = (
                conn.execute(text("SELECT COUNT(*) FROM schedules")).scalar() or 0
            )
            if schedules_count == 0:
                return None

        query = text(
            """
            SELECT
                t.train_id,
                t.train_number,
                o.name AS operator,
                o.country AS operator_country,
                o.website,
                t.train_type AS train_type_raw,
                t.train_type_rule,
                t.train_type_heuristic,
                t.train_type_ml,
                t.classification_method,
                t.classification_reason,
                t.classification_confidence,
                t.ml_night_probability,
                t.night_percentage,
                t.needs_manual_review,
                st_dep.name AS departure_station,
                st_arr.name AS arrival_station,
                s.departure_time,
                s.arrival_time,
                s.duration_min,
                s.distance_km,
                s.source_name AS data_source
            FROM trains t
            JOIN operators o ON o.operator_id = t.operator_id
            JOIN schedules s ON s.train_id = t.train_id
            LEFT JOIN stations st_dep ON st_dep.station_id = s.origin_id
            LEFT JOIN stations st_arr ON st_arr.station_id = s.destination_id
            """
        )

        df = pd.read_sql(query, engine)
        if df.empty:
            return None

        df = _to_numeric(df, ["duration_min", "distance_km"])
        df["duration_hours"] = df["duration_min"] / 60
        df["train_type_normalized"] = df["train_type_raw"].apply(_normalize_train_type)
        df["co2_emission_kg"] = df["distance_km"] * 0.005
        df["co2_saving_kg"] = (df["distance_km"] * 0.215) - df["co2_emission_kg"]
        return df
    except Exception:
        return None


def _try_load_from_csv() -> pd.DataFrame:
    for candidate in (FINAL_CSV, FALLBACK_CSV):
        if candidate.exists():
            return _prepare_csv_df(pd.read_csv(candidate, low_memory=False))
    return pd.DataFrame()


def load_analytics_data(force_refresh: bool = False) -> pd.DataFrame:
    global _ANALYTICS_CACHE, _ANALYTICS_MODE

    if _ANALYTICS_CACHE is not None and not force_refresh:
        return _ANALYTICS_CACHE.copy()

    db_df = _try_load_from_db()
    if db_df is not None and not db_df.empty:
        _ANALYTICS_CACHE = db_df
        _ANALYTICS_MODE = "postgresql"
        return _ANALYTICS_CACHE.copy()

    csv_df = _try_load_from_csv()
    _ANALYTICS_CACHE = csv_df
    _ANALYTICS_MODE = "csv_fallback" if not csv_df.empty else "empty"
    return _ANALYTICS_CACHE.copy()


def get_analytics_mode() -> str:
    if _ANALYTICS_MODE == "unknown":
        load_analytics_data(force_refresh=True)
    return _ANALYTICS_MODE


def get_data_source_label() -> str:
    mode = get_analytics_mode()
    if mode == "postgresql":
        return "PostgreSQL"
    if mode == "csv_fallback":
        return "CSV fallback"
    return "No data available"
