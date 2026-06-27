"""Pure feature engineering helpers for the ML dataset."""

import numpy as np
import pandas as pd

from ML.datasetML.config import (
    DEFAULT_CO2_KG_PER_KM,
    MAX_REALISTIC_SPEED_KMH,
    MIN_REALISTIC_SPEED_KMH,
)


def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with derived ML features added."""

    result = df.copy()

    distance = _numeric_column(result, "distance_km")
    if "duration_minutes" in result.columns:
        duration_minutes = pd.to_numeric(result["duration_minutes"], errors="coerce")
    else:
        duration_minutes = _numeric_column(result, "duration_min")

    result["duration_minutes"] = duration_minutes
    result["duration_min"] = duration_minutes
    duration_hours = duration_minutes / 60

    result["duration_hours"] = duration_hours
    result["avg_speed_kmh"] = distance / duration_hours.replace(0, np.nan)

    if {"origin_country", "destination_country"}.issubset(result.columns):
        result["is_international"] = (
            result["origin_country"].fillna("unknown").astype(str)
            != result["destination_country"].fillna("unknown").astype(str)
        )
    else:
        result["is_international"] = False

    if "estimated_co2_saving_kg" in result.columns:
        co2 = pd.to_numeric(result["estimated_co2_saving_kg"], errors="coerce")
    elif "co2_saving_kg" in result.columns:
        co2 = pd.to_numeric(result["co2_saving_kg"], errors="coerce")
    else:
        co2 = pd.Series(np.nan, index=result.index, dtype=float)

    result["estimated_co2_saving_kg"] = co2.fillna(distance * DEFAULT_CO2_KG_PER_KM).fillna(0)

    if "weekly_frequency" in result.columns:
        result["weekly_frequency"] = (
            pd.to_numeric(result["weekly_frequency"], errors="coerce").fillna(0)
        )
    else:
        result["weekly_frequency"] = 0

    return result


def _numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    """Return a numeric Series aligned to df.index, even when column is absent."""

    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype=float)


def assign_substitution_potential(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with transparent business target classes assigned."""

    result = df.copy()

    if "duration_hours" not in result.columns or "avg_speed_kmh" not in result.columns:
        result = add_derived_features(result)

    distance = pd.to_numeric(result.get("distance_km"), errors="coerce")
    if "duration_minutes" in result.columns:
        duration = pd.to_numeric(result["duration_minutes"], errors="coerce")
    else:
        duration = pd.to_numeric(result.get("duration_min"), errors="coerce")
    speed = pd.to_numeric(result.get("avg_speed_kmh"), errors="coerce")
    co2 = pd.to_numeric(result.get("estimated_co2_saving_kg"), errors="coerce").fillna(0)

    if "is_international" in result.columns:
        international = result["is_international"].fillna(False).astype(bool)
    elif {"origin_country", "destination_country"}.issubset(result.columns):
        international = (
            result["origin_country"].fillna("unknown").astype(str)
            != result["destination_country"].fillna("unknown").astype(str)
        )
    else:
        international = pd.Series(False, index=result.index)

    realistic_speed = speed.between(MIN_REALISTIC_SPEED_KMH, MAX_REALISTIC_SPEED_KMH)
    strong = (
        distance.between(300, 1200)
        & duration.le(480)
        & (international | co2.ge(80))
        & realistic_speed
    )
    medium = distance.between(200, 1500) & duration.le(720) & realistic_speed

    result["substitution_potential"] = np.select(
        [strong, medium],
        ["fort_potentiel", "potentiel_moyen"],
        default="faible_potentiel",
    )

    return result
