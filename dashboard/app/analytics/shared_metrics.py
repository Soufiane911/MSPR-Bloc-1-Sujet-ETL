"""Shared aggregation helpers for Streamlit dashboard pages."""

from __future__ import annotations

import pandas as pd


def _ensure_train_key(df: pd.DataFrame) -> pd.Series:
    if "train_id" in df.columns:
        return df["train_id"]
    if "train_number" in df.columns:
        return df["train_number"].astype(str)
    return pd.Series(range(len(df)), index=df.index)


def _unique_station_count(df: pd.DataFrame) -> int:
    stations: set[str] = set()
    for column in ("departure_station", "arrival_station"):
        if column in df.columns:
            stations.update(df[column].dropna().astype(str).tolist())
    return len(stations)


def get_overview_kpis(df: pd.DataFrame) -> dict[str, int]:
    if df.empty:
        return {
            "total_trains": 0,
            "total_stations": 0,
            "total_operators": 0,
            "total_schedules": 0,
        }

    train_key = _ensure_train_key(df)
    operators = df["operator"].dropna().nunique() if "operator" in df.columns else 0

    return {
        "total_trains": int(train_key.nunique()),
        "total_stations": _unique_station_count(df),
        "total_operators": int(operators),
        "total_schedules": int(len(df)),
    }


def get_train_type_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "train_type_normalized" not in df.columns:
        return pd.DataFrame(columns=["train_type", "count"])

    return (
        df.assign(train_key=_ensure_train_key(df))
        .groupby("train_type_normalized", observed=False)["train_key"]
        .nunique()
        .reset_index(name="count")
        .rename(columns={"train_type_normalized": "train_type"})
    )


def get_country_train_counts(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if df.empty or "operator_country" not in df.columns:
        return pd.DataFrame(columns=["country", "nb_trains"])

    return (
        df.assign(train_key=_ensure_train_key(df))
        .dropna(subset=["operator_country"])
        .groupby("operator_country", observed=False)["train_key"]
        .nunique()
        .reset_index(name="nb_trains")
        .rename(columns={"operator_country": "country"})
        .sort_values("nb_trains", ascending=False)
        .head(limit)
    )


def get_top_operators_summary(df: pd.DataFrame, limit: int = 15) -> pd.DataFrame:
    if df.empty or "operator" not in df.columns:
        return pd.DataFrame(
            columns=["Opérateur", "Pays", "Trains", "Jour", "Nuit", "Dessertes"]
        )

    work = df.assign(train_key=_ensure_train_key(df)).copy()
    day_counts = (
        work[work["train_type_normalized"] == "day"]
        .groupby(["operator", "operator_country"], observed=False)["train_key"]
        .nunique()
        .rename("Jour")
    )
    night_counts = (
        work[work["train_type_normalized"] == "night"]
        .groupby(["operator", "operator_country"], observed=False)["train_key"]
        .nunique()
        .rename("Nuit")
    )

    grouped = (
        work.groupby(["operator", "operator_country"], observed=False)
        .agg(Trains=("train_key", "nunique"), Dessertes=("train_key", "size"))
        .join(day_counts, how="left")
        .join(night_counts, how="left")
        .fillna({"Jour": 0, "Nuit": 0})
        .reset_index()
        .rename(columns={"operator": "Opérateur", "operator_country": "Pays"})
        .sort_values("Trains", ascending=False)
        .head(limit)
    )
    grouped["Jour"] = grouped["Jour"].astype(int)
    grouped["Nuit"] = grouped["Nuit"].astype(int)
    return grouped


def get_day_night_country_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "operator_country" not in df.columns:
        return pd.DataFrame(
            columns=[
                "train_type",
                "country",
                "nb_trains",
                "nb_schedules",
                "avg_duration",
                "avg_distance",
            ]
        )

    grouped = (
        df.assign(train_key=_ensure_train_key(df))
        .dropna(subset=["operator_country", "train_type_normalized"])
        .groupby(["train_type_normalized", "operator_country"], observed=False)
        .agg(
            nb_trains=("train_key", "nunique"),
            nb_schedules=("train_key", "size"),
            avg_duration=("duration_min", "mean"),
            avg_distance=("distance_km", "mean"),
        )
        .reset_index()
        .rename(
            columns={
                "train_type_normalized": "train_type",
                "operator_country": "country",
            }
        )
        .sort_values(["country", "train_type"])
    )
    grouped["avg_duration"] = grouped["avg_duration"].round(0)
    grouped["avg_distance"] = grouped["avg_distance"].round(0)
    return grouped


def get_operator_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "operator" not in df.columns:
        return pd.DataFrame(
            columns=[
                "operator_name",
                "country",
                "website",
                "nb_trains",
                "day_trains",
                "night_trains",
                "nb_schedules",
                "avg_duration",
                "avg_distance",
            ]
        )

    work = df.assign(train_key=_ensure_train_key(df)).copy()
    if "website" not in work.columns:
        work["website"] = pd.NA

    day_counts = (
        work[work["train_type_normalized"] == "day"]
        .groupby(["operator", "operator_country"], observed=False)["train_key"]
        .nunique()
        .rename("day_trains")
    )
    night_counts = (
        work[work["train_type_normalized"] == "night"]
        .groupby(["operator", "operator_country"], observed=False)["train_key"]
        .nunique()
        .rename("night_trains")
    )

    grouped = (
        work.groupby(["operator", "operator_country", "website"], observed=False)
        .agg(
            nb_trains=("train_key", "nunique"),
            nb_schedules=("train_key", "size"),
            avg_duration=("duration_min", "mean"),
            avg_distance=("distance_km", "mean"),
        )
        .join(day_counts, how="left")
        .join(night_counts, how="left")
        .fillna({"day_trains": 0, "night_trains": 0})
        .reset_index()
        .rename(columns={"operator": "operator_name", "operator_country": "country"})
        .sort_values("nb_trains", ascending=False)
    )
    grouped["day_trains"] = grouped["day_trains"].astype(int)
    grouped["night_trains"] = grouped["night_trains"].astype(int)
    grouped["avg_duration"] = grouped["avg_duration"].round(0)
    grouped["avg_distance"] = grouped["avg_distance"].round(0)
    return grouped
