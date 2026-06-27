"""Cleaning helpers for the ML dataset."""

import pandas as pd

from ML.datasetML.config import MAX_REALISTIC_SPEED_KMH, MIN_REALISTIC_SPEED_KMH


def clean_ml_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Return a cleaned copy of an ML dataset DataFrame."""

    result = df.copy()

    result["distance_km"] = pd.to_numeric(result.get("distance_km"), errors="coerce")
    if "duration_minutes" in result.columns:
        duration = pd.to_numeric(result["duration_minutes"], errors="coerce")
    else:
        duration = pd.to_numeric(result.get("duration_min"), errors="coerce")
    result["duration_minutes"] = duration
    result["duration_min"] = duration

    valid_rows = result["distance_km"].gt(0) & result["duration_minutes"].gt(0)

    if "avg_speed_kmh" in result.columns:
        result["avg_speed_kmh"] = pd.to_numeric(result["avg_speed_kmh"], errors="coerce")
        valid_rows &= result["avg_speed_kmh"].between(
            MIN_REALISTIC_SPEED_KMH, MAX_REALISTIC_SPEED_KMH
        )

    result = result.loc[valid_rows].copy()

    for column in ["train_type", "origin_country", "destination_country"]:
        if column in result.columns:
            result[column] = result[column].fillna("unknown")
        else:
            result[column] = "unknown"

    if "weekly_frequency" in result.columns:
        result["weekly_frequency"] = (
            pd.to_numeric(result["weekly_frequency"], errors="coerce").fillna(0)
        )
    else:
        result["weekly_frequency"] = 0

    if "estimated_co2_saving_kg" in result.columns:
        result["estimated_co2_saving_kg"] = (
            pd.to_numeric(result["estimated_co2_saving_kg"], errors="coerce").fillna(0)
        )
    elif "co2_saving_kg" in result.columns:
        result["estimated_co2_saving_kg"] = (
            pd.to_numeric(result["co2_saving_kg"], errors="coerce").fillna(0)
        )
    else:
        result["estimated_co2_saving_kg"] = 0

    if "schedule_id" in result.columns:
        result = result.drop_duplicates(subset=["schedule_id"], keep="first")
    elif "route_id" in result.columns:
        result = result.drop_duplicates(subset=["route_id"], keep="first")

    return result.reset_index(drop=True)
