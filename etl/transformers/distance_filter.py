"""Filtre de distance minimale pour le perimetre grande ligne ObRail."""

from __future__ import annotations

import math
from typing import Dict, Optional, Set

import pandas as pd

from config.logging_config import setup_logging

MIN_TRIP_DISTANCE_KM = 100.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance geodesique en km entre deux points WGS84."""
    radius_km = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * radius_km * math.asin(math.sqrt(min(1.0, a)))


def compute_trip_distances_km(
    trips: pd.DataFrame,
    stop_times: pd.DataFrame,
    stops: pd.DataFrame,
    trip_id_col: str = "trip_id",
    stop_id_col: str = "stop_id",
) -> pd.Series:
    """
    Calcule la distance origine-destination (Haversine) pour chaque trip.

    Returns:
        Series indexee par trip_id avec distance en km.
    """
    if trips.empty or stop_times.empty or stops.empty:
        return pd.Series(dtype=float)

    st = stop_times.copy()
    st[trip_id_col] = st[trip_id_col].astype(str)
    st[stop_id_col] = st[stop_id_col].astype(str)
    st["stop_sequence"] = pd.to_numeric(st.get("stop_sequence"), errors="coerce")
    st = st.sort_values([trip_id_col, "stop_sequence"])

    station_coords = stops.copy()
    station_coords[stop_id_col] = station_coords[stop_id_col].astype(str)
    station_coords["stop_lat"] = pd.to_numeric(
        station_coords.get("stop_lat"), errors="coerce"
    )
    station_coords["stop_lon"] = pd.to_numeric(
        station_coords.get("stop_lon"), errors="coerce"
    )
    coords = station_coords.set_index(stop_id_col)[["stop_lat", "stop_lon"]]

    first = st.groupby(trip_id_col, as_index=False).first()
    last = st.groupby(trip_id_col, as_index=False).last()
    segment = first.merge(last, on=trip_id_col, suffixes=("_o", "_d"))

    distances = {}
    for row in segment.itertuples(index=False):
        trip_id = getattr(row, trip_id_col)
        origin_id = getattr(row, f"{stop_id_col}_o")
        dest_id = getattr(row, f"{stop_id_col}_d")
        if origin_id not in coords.index or dest_id not in coords.index:
            continue
        lat_o, lon_o = coords.loc[origin_id]
        lat_d, lon_d = coords.loc[dest_id]
        if pd.isna(lat_o) or pd.isna(lon_o) or pd.isna(lat_d) or pd.isna(lon_d):
            continue
        distances[trip_id] = haversine_km(lat_o, lon_o, lat_d, lon_d)

    return pd.Series(distances, dtype=float)


def _filter_stop_times(
    stop_times: pd.DataFrame, kept_trip_ids: Set[str], trip_id_col: str = "trip_id"
) -> pd.DataFrame:
    if stop_times.empty:
        return stop_times
    st = stop_times.copy()
    st[trip_id_col] = st[trip_id_col].astype(str)
    return st[st[trip_id_col].isin(kept_trip_ids)].copy()


def filter_trips_by_min_distance(
    source_name: str,
    trips: pd.DataFrame,
    stop_times: Optional[pd.DataFrame] = None,
    stops: Optional[pd.DataFrame] = None,
    declared_distances: Optional[pd.Series] = None,
    min_distance_km: float = MIN_TRIP_DISTANCE_KM,
    trip_id_col: str = "trip_id",
) -> tuple[pd.DataFrame, Optional[pd.DataFrame], pd.Series]:
    """
    Ne conserve que les trips dont la distance est >= min_distance_km.

    Returns:
        (trips_filtres, stop_times_filtres, series distances_km)
    """
    logger = setup_logging("transformer.filter.distance")

    if trips.empty:
        return trips, stop_times, pd.Series(dtype=float)

    trips = trips.copy()
    trips[trip_id_col] = trips[trip_id_col].astype(str)

    if declared_distances is not None and not declared_distances.empty:
        distances = declared_distances.copy()
        distances.index = distances.index.astype(str)
    elif stop_times is not None and stops is not None:
        distances = compute_trip_distances_km(trips, stop_times, stops, trip_id_col)
    else:
        logger.warning(
            f"[FILTER] {source_name}: impossible de calculer la distance, "
            f"filtre {min_distance_km} km ignore"
        )
        return trips, stop_times, pd.Series(dtype=float)

    if distances.empty:
        logger.warning(
            f"[FILTER] {source_name}: aucune distance calculee, "
            f"filtre {min_distance_km} km ignore"
        )
        return trips, stop_times, distances

    aligned = trips.set_index(trip_id_col).join(
        distances.rename("distance_km"), how="left"
    )
    mask = aligned["distance_km"] >= min_distance_km
    kept_ids = set(aligned.index[mask].astype(str))

    initial = len(trips)
    filtered_trips = trips[trips[trip_id_col].isin(kept_ids)].copy()
    filtered_trips["distance_km"] = filtered_trips[trip_id_col].map(distances)
    filtered_stop_times = (
        _filter_stop_times(stop_times, kept_ids, trip_id_col)
        if stop_times is not None
        else None
    )

    removed = initial - len(filtered_trips)
    if removed > 0:
        logger.info(
            f"[FILTER] {source_name}: {removed}/{initial} trips supprimes "
            f"(distance < {min_distance_km:g} km)"
        )

    return filtered_trips, filtered_stop_times, distances[distances.index.isin(kept_ids)]


def apply_min_distance_to_result(
    source_name: str,
    result: Dict[str, pd.DataFrame],
    min_distance_km: float = MIN_TRIP_DISTANCE_KM,
) -> Dict[str, pd.DataFrame]:
    """Applique le filtre distance sur un jeu transforme (GTFS ou Back-on-Track)."""
    if "trips" not in result or result["trips"].empty:
        return result

    declared = None
    if source_name == "back_on_track" and "distance" in result["trips"].columns:
        trip_ids = result["trips"]["trip_id"].astype(str)
        declared = pd.Series(
            pd.to_numeric(result["trips"]["distance"], errors="coerce").values,
            index=trip_ids,
        ).dropna()

    stops = result.get("stops")
    stop_times = result.get("stop_times")

    filtered_trips, filtered_stop_times, _ = filter_trips_by_min_distance(
        source_name=source_name,
        trips=result["trips"],
        stop_times=stop_times,
        stops=stops,
        declared_distances=declared,
        min_distance_km=min_distance_km,
    )

    result["trips"] = filtered_trips
    if filtered_stop_times is not None:
        result["stop_times"] = filtered_stop_times

    return result
