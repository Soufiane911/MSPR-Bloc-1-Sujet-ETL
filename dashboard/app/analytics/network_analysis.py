"""Network complementarity analytics for the Streamlit dashboard."""

from __future__ import annotations

import re

import pandas as pd


TIME_LIKE_RE = re.compile(r"^\d{1,2}:\d{2}(:\d{2})?$")


def _is_valid_station(value: object) -> bool:
    if pd.isna(value):
        return False
    return not bool(TIME_LIKE_RE.match(str(value).strip()))


def get_routes_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df[
        df["departure_station"].apply(_is_valid_station)
        & df["arrival_station"].apply(_is_valid_station)
    ].copy()
    if out.empty:
        return pd.DataFrame()
    out["route"] = (
        out["departure_station"].astype(str).str.strip()
        + " -> "
        + out["arrival_station"].astype(str).str.strip()
    )
    return out


def get_network_kpis(df: pd.DataFrame) -> dict[str, float]:
    routes = get_routes_df(df)
    if routes.empty:
        return {
            "only_day": 0,
            "only_night": 0,
            "both": 0,
            "day_km": 0.0,
            "night_km": 0.0,
        }

    route_types = routes.groupby("route", observed=False)["train_type_normalized"].agg(
        lambda series: set(series.dropna().tolist())
    )
    only_day = int((route_types.apply(lambda values: values == {"day"})).sum())
    only_night = int((route_types.apply(lambda values: values == {"night"})).sum())
    both = int((route_types.apply(lambda values: values == {"day", "night"})).sum())

    day_km = routes.loc[routes["train_type_normalized"] == "day", "distance_km"].sum()
    night_km = routes.loc[
        routes["train_type_normalized"] == "night", "distance_km"
    ].sum()

    return {
        "only_day": only_day,
        "only_night": only_night,
        "both": both,
        "day_km": 0.0 if pd.isna(day_km) else float(day_km),
        "night_km": 0.0 if pd.isna(night_km) else float(night_km),
    }


def get_od_coverage(df: pd.DataFrame) -> pd.DataFrame:
    routes = get_routes_df(df)
    if routes.empty:
        return pd.DataFrame()

    coverage = (
        routes.groupby(["route", "train_type_normalized"], observed=False)
        .size()
        .reset_index(name="services")
        .groupby("train_type_normalized", observed=False)["route"]
        .nunique()
        .reset_index(name="nb_routes")
    )
    coverage["label"] = coverage["train_type_normalized"].map(
        {"day": "Day", "night": "Night"}
    )
    return coverage


def get_service_km(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "distance_km" not in df.columns:
        return pd.DataFrame()

    grouped = (
        df.groupby("train_type_normalized", observed=False)["distance_km"]
        .sum()
        .reset_index(name="total_km")
    )
    grouped["label"] = grouped["train_type_normalized"].map(
        {"day": "Day", "night": "Night"}
    )
    return grouped


def get_top_operators(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if df.empty or "operator" not in df.columns:
        return pd.DataFrame()

    grouped = (
        df.dropna(subset=["operator"])
        .groupby(["operator", "train_type_normalized"], observed=False)
        .size()
        .reset_index(name="count")
    )

    top_day = grouped[grouped["train_type_normalized"] == "day"].nlargest(
        top_n, "count"
    )
    top_night = grouped[grouped["train_type_normalized"] == "night"].nlargest(
        top_n, "count"
    )
    return pd.concat([top_day, top_night], ignore_index=True)


def get_country_breakdown(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    if df.empty or "operator_country" not in df.columns:
        return pd.DataFrame()

    grouped = (
        df.dropna(subset=["operator_country"])
        .groupby(["operator_country", "train_type_normalized"], observed=False)
        .size()
        .reset_index(name="count")
        .rename(
            columns={
                "operator_country": "country",
                "train_type_normalized": "train_type",
            }
        )
    )

    top_countries = (
        grouped.groupby("country", observed=False)["count"].sum().nlargest(top_n).index
    )
    return grouped[grouped["country"].isin(top_countries)]
