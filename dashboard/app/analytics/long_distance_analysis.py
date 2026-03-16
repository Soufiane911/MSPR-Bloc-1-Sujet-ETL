"""Long-distance analytics for the Streamlit dashboard."""

from __future__ import annotations

import re

import pandas as pd


TIME_LIKE_RE = re.compile(r"^\d{1,2}:\d{2}(:\d{2})?$")


def _is_valid_station(value: object) -> bool:
    if pd.isna(value):
        return False
    return not bool(TIME_LIKE_RE.match(str(value).strip()))


def get_long_distance_base(
    df: pd.DataFrame, min_distance_km: int = 500
) -> pd.DataFrame:
    if df.empty or "distance_km" not in df.columns:
        return pd.DataFrame()
    out = df.dropna(subset=["distance_km", "train_type_normalized"]).copy()
    return out[out["distance_km"] >= min_distance_km]


def get_long_distance_kpis(df: pd.DataFrame) -> dict[str, float]:
    long_df = get_long_distance_base(df)
    if long_df.empty:
        return {
            "services": 0,
            "night_pct": 0.0,
            "day_pct": 0.0,
            "avg_co2_night": 0.0,
            "avg_co2_day": 0.0,
        }

    day_count = int((long_df["train_type_normalized"] == "day").sum())
    night_count = int((long_df["train_type_normalized"] == "night").sum())
    total = day_count + night_count

    avg_co2_night = long_df.loc[
        long_df["train_type_normalized"] == "night", "co2_saving_kg"
    ].mean()
    avg_co2_day = long_df.loc[
        long_df["train_type_normalized"] == "day", "co2_saving_kg"
    ].mean()

    return {
        "services": total,
        "night_pct": round((night_count / total) * 100, 1) if total else 0.0,
        "day_pct": round((day_count / total) * 100, 1) if total else 0.0,
        "avg_co2_night": 0.0
        if pd.isna(avg_co2_night)
        else round(float(avg_co2_night), 1),
        "avg_co2_day": 0.0 if pd.isna(avg_co2_day) else round(float(avg_co2_day), 1),
    }


def get_distance_segments(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "distance_km" not in df.columns:
        return pd.DataFrame()

    segmented = df.dropna(subset=["distance_km", "train_type_normalized"]).copy()
    segmented["segment"] = pd.cut(
        segmented["distance_km"],
        bins=[0, 200, 400, 600, 800, 1200, float("inf")],
        labels=["0-200", "200-400", "400-600", "600-800", "800-1200", "1200+"],
        right=False,
    )

    grouped = (
        segmented.groupby(["segment", "train_type_normalized"], observed=False)
        .size()
        .unstack(fill_value=0)
    )
    grouped["night_pct"] = (
        grouped.get("night", 0) / grouped.sum(axis=1).replace(0, pd.NA) * 100
    ).fillna(0)
    return grouped.reset_index()


def get_long_distance_co2(df: pd.DataFrame) -> pd.DataFrame:
    long_df = get_long_distance_base(df)
    if long_df.empty or "co2_saving_kg" not in long_df.columns:
        return pd.DataFrame()

    return (
        long_df.groupby("train_type_normalized", observed=False)
        .agg(avg_co2=("co2_saving_kg", "mean"), total_co2=("co2_saving_kg", "sum"))
        .reset_index()
        .rename(columns={"train_type_normalized": "train_type"})
    )


def get_night_corridors(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    night_df = df[
        (df["train_type_normalized"] == "night")
        & (df["distance_km"] >= 500)
        & (df["duration_min"] >= 360)
    ].copy()

    if night_df.empty:
        return pd.DataFrame()

    night_df = night_df[
        night_df["departure_station"].apply(_is_valid_station)
        & night_df["arrival_station"].apply(_is_valid_station)
    ]

    if night_df.empty:
        return pd.DataFrame()

    night_df["route"] = (
        night_df["departure_station"].astype(str).str.strip()
        + " -> "
        + night_df["arrival_station"].astype(str).str.strip()
    )

    return (
        night_df.groupby("route", observed=False)
        .agg(
            services=("train_id", "count"),
            avg_co2=("co2_saving_kg", "mean"),
            avg_distance=("distance_km", "mean"),
        )
        .reset_index()
        .sort_values("services", ascending=False)
        .head(10)
    )


def get_night_shift_candidates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    base = df[
        df["departure_station"].apply(_is_valid_station)
        & df["arrival_station"].apply(_is_valid_station)
    ].copy()

    if base.empty:
        return pd.DataFrame()

    base["route"] = (
        base["departure_station"].astype(str).str.strip()
        + " -> "
        + base["arrival_station"].astype(str).str.strip()
    )

    day_routes = base[
        (base["train_type_normalized"] == "day")
        & (base["distance_km"] >= 400)
        & (base["duration_min"] >= 240)
    ].copy()

    night_routes_set = set(
        base[(base["train_type_normalized"] == "night") & (base["distance_km"] >= 400)][
            "route"
        ]
        .dropna()
        .unique()
        .tolist()
    )

    if day_routes.empty:
        return pd.DataFrame()

    candidates = (
        day_routes.groupby("route", observed=False)
        .agg(
            services=("train_id", "count"),
            avg_dist=("distance_km", "mean"),
            avg_co2=("co2_saving_kg", "mean"),
        )
        .reset_index()
    )

    return (
        candidates[~candidates["route"].isin(night_routes_set)]
        .sort_values("services", ascending=False)
        .head(10)
    )
