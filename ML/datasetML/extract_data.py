"""Data extraction helpers for the ObRail substitution ML dataset."""

from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from ML.datasetML.config import (
    RAIL_ROUTE_TYPES,
    RAW_DATA_DIR,
    SOURCE_COUNTRY_MAP,
    SQL_QUERY_PATH,
)

WEEKDAYS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]


def load_sql_query(query_path: Path = SQL_QUERY_PATH) -> str:
    """Load the SQL extraction query used for PostgreSQL exports."""

    return query_path.read_text(encoding="utf-8")


def extract_from_postgres(
    database_url: Optional[str] = None,
    query_path: Path = SQL_QUERY_PATH,
) -> pd.DataFrame:
    """Extract route-level ML source data from PostgreSQL."""

    from sqlalchemy import create_engine

    url = database_url or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is required for PostgreSQL extraction")

    engine = create_engine(url)
    return pd.read_sql(load_sql_query(query_path), engine)


def extract_from_local_raw(raw_dir: Path = RAW_DATA_DIR) -> pd.DataFrame:
    """Extract a first ML source dataset directly from local raw files."""

    frames: list[pd.DataFrame] = []

    back_on_track = _extract_back_on_track(raw_dir / "back_on_track")
    if not back_on_track.empty:
        frames.append(back_on_track)

    for source_name, country in SOURCE_COUNTRY_MAP.items():
        source_dir = raw_dir / source_name
        gtfs = _extract_gtfs_source(source_dir, source_name, country)
        if not gtfs.empty:
            frames.append(gtfs)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def estimate_weekly_frequency_from_calendar(calendar: pd.DataFrame) -> dict[str, int]:
    """Estimate weekly service frequency from GTFS weekday flags."""

    if calendar.empty or "service_id" not in calendar.columns:
        return {}

    result: dict[str, int] = {}
    for row in calendar.itertuples(index=False):
        service_id = str(getattr(row, "service_id"))
        count = 0
        for day in WEEKDAYS:
            value = getattr(row, day, 0) if day in calendar.columns else 0
            numeric = pd.to_numeric(value, errors="coerce")
            count += 0 if pd.isna(numeric) else int(numeric)
        result[service_id] = count
    return result


def estimate_weekly_frequency_from_calendar_dates(
    calendar_dates: pd.DataFrame,
) -> dict[str, int]:
    """Estimate weekly frequency from exception dates when calendar.txt is absent."""

    if calendar_dates.empty or "service_id" not in calendar_dates.columns:
        return {}

    dates = calendar_dates.copy()
    if "exception_type" in dates.columns:
        dates = dates[pd.to_numeric(dates["exception_type"], errors="coerce") == 1]

    dates["date"] = pd.to_datetime(
        dates.get("date").astype(str), format="%Y%m%d", errors="coerce"
    )
    dates = dates.dropna(subset=["date"])
    if dates.empty:
        return {}

    result: dict[str, int] = {}
    for service_id, group in dates.groupby("service_id"):
        span_days = max(1, int((group["date"].max() - group["date"].min()).days) + 1)
        weeks = max(1.0, span_days / 7)
        result[str(service_id)] = int(round(min(7, max(0, len(group) / weeks))))
    return result


def parse_back_on_track_duration_minutes(value: object) -> Optional[int]:
    """Parse Back-on-Track duration values into minutes."""

    if value is None or pd.isna(value) or str(value).strip() == "":
        return None

    text = str(value).strip()
    match = re.search(r"T(\d{1,2}):(\d{2})(?::(\d{2}))?", text)
    if not match:
        match = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", text)
    if not match:
        return None

    hours = int(match.group(1))
    minutes = int(match.group(2))
    return hours * 60 + minutes


def _extract_gtfs_source(
    source_dir: Path,
    source_name: str,
    country: str,
) -> pd.DataFrame:
    required = ["routes.txt", "trips.txt", "stop_times.txt", "stops.txt"]
    if not source_dir.exists() or any(not (source_dir / name).exists() for name in required):
        return pd.DataFrame()

    routes = pd.read_csv(source_dir / "routes.txt", dtype=str)
    trips = pd.read_csv(source_dir / "trips.txt", dtype=str)
    stop_times = pd.read_csv(
        source_dir / "stop_times.txt",
        dtype=str,
        usecols=lambda col: col
        in {"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"},
    )
    stops = pd.read_csv(
        source_dir / "stops.txt",
        dtype=str,
        usecols=lambda col: col in {"stop_id", "stop_name", "stop_lat", "stop_lon"},
    )

    if routes.empty or trips.empty or stop_times.empty or stops.empty:
        return pd.DataFrame()

    routes = _filter_rail_routes(routes)
    if routes.empty:
        return pd.DataFrame()

    trips = trips[trips["route_id"].isin(set(routes["route_id"]))].copy()
    if trips.empty:
        return pd.DataFrame()

    stop_times = stop_times[stop_times["trip_id"].isin(set(trips["trip_id"]))].copy()
    if stop_times.empty:
        return pd.DataFrame()

    schedules = _aggregate_stop_times(stop_times)
    if schedules.empty:
        return pd.DataFrame()

    schedules = schedules.merge(
        trips[[col for col in ["trip_id", "route_id", "service_id", "trip_headsign"] if col in trips.columns]],
        on="trip_id",
        how="left",
    )
    schedules = schedules.merge(
        routes[[col for col in ["route_id", "route_short_name", "route_long_name", "route_type"] if col in routes.columns]],
        on="route_id",
        how="left",
    )

    stop_lookup = stops.set_index("stop_id")
    schedules["origin"] = schedules["origin_stop_id"].map(stop_lookup["stop_name"])
    schedules["destination"] = schedules["destination_stop_id"].map(stop_lookup["stop_name"])
    schedules["origin_country"] = country
    schedules["destination_country"] = country
    schedules["distance_km"] = schedules.apply(
        lambda row: _distance_from_stop_lookup(row, stop_lookup), axis=1
    )
    schedules["weekly_frequency"] = schedules.get("service_id", pd.Series(index=schedules.index)).map(
        _load_frequency_map(source_dir)
    ).fillna(0)
    schedules["source_name"] = source_name
    schedules["schedule_id"] = source_name + "_" + schedules["trip_id"].astype(str)
    schedules["route_id"] = schedules["schedule_id"]
    schedules["train_type"] = schedules.apply(_classify_train_type, axis=1)

    return schedules[
        [
            "route_id",
            "schedule_id",
            "source_name",
            "origin",
            "destination",
            "origin_country",
            "destination_country",
            "distance_km",
            "duration_min",
            "train_type",
            "weekly_frequency",
        ]
    ].copy()


def _extract_back_on_track(source_dir: Path) -> pd.DataFrame:
    if not source_dir.exists() or not (source_dir / "trips.json").exists():
        return pd.DataFrame()

    trips = pd.DataFrame(_load_json_records(source_dir / "trips.json"))
    stops = pd.DataFrame(_load_json_records(source_dir / "stops.json"))
    trip_stop = pd.DataFrame(_load_json_records(source_dir / "trip_stop.json"))
    calendar = pd.DataFrame(_load_json_records(source_dir / "calendar.json"))

    if trips.empty:
        return pd.DataFrame()

    frame = trips.copy()
    frame["route_id"] = "back_on_track_" + frame.get("trip_id", frame.index).astype(str)
    frame["schedule_id"] = frame["route_id"]
    frame["source_name"] = "back_on_track"
    frame["origin"] = frame.get("trip_origin")
    frame["destination"] = frame.get("trip_headsign")
    frame["distance_km"] = pd.to_numeric(frame.get("distance"), errors="coerce")
    frame["duration_min"] = frame.get("duration").apply(parse_back_on_track_duration_minutes)
    frame["train_type"] = "night"

    origin_country, destination_country = _back_on_track_countries(frame, stops, trip_stop)
    frame["origin_country"] = origin_country
    frame["destination_country"] = destination_country

    frequency_map = estimate_weekly_frequency_from_calendar(calendar)
    frame["weekly_frequency"] = frame.get("service_id").apply(
        lambda value: _parse_back_on_track_frequency(value, frequency_map)
    )
    # Back-on-Track exposes train emissions, not observed flight-to-rail savings.
    # Keep this empty so feature engineering applies the shared saving estimate.
    frame["estimated_co2_saving_kg"] = pd.NA

    return frame[
        [
            "route_id",
            "schedule_id",
            "source_name",
            "origin",
            "destination",
            "origin_country",
            "destination_country",
            "distance_km",
            "duration_min",
            "train_type",
            "weekly_frequency",
            "estimated_co2_saving_kg",
        ]
    ].copy()


def _load_json_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return [value for value in data.values() if isinstance(value, dict)]
    if isinstance(data, list):
        return [value for value in data if isinstance(value, dict)]
    return []


def _filter_rail_routes(routes: pd.DataFrame) -> pd.DataFrame:
    routes = routes.copy()
    if "route_type" not in routes.columns:
        return routes
    route_type = pd.to_numeric(routes["route_type"], errors="coerce")
    return routes[route_type.isin(RAIL_ROUTE_TYPES)].copy()


def _aggregate_stop_times(stop_times: pd.DataFrame) -> pd.DataFrame:
    work = stop_times.copy()
    work["stop_sequence"] = pd.to_numeric(work.get("stop_sequence"), errors="coerce")
    work = work.dropna(subset=["trip_id", "stop_id", "stop_sequence"])
    work = work.sort_values(["trip_id", "stop_sequence"])

    first = work.groupby("trip_id", as_index=False).first()
    last = work.groupby("trip_id", as_index=False).last()
    merged = first.merge(last, on="trip_id", suffixes=("_origin", "_destination"))

    dep = merged["departure_time_origin"].apply(_gtfs_time_to_minutes)
    arr = merged["arrival_time_destination"].apply(_gtfs_time_to_minutes)
    duration = arr - dep
    duration = duration.where(duration > 0, duration + 24 * 60)

    return pd.DataFrame(
        {
            "trip_id": merged["trip_id"],
            "origin_stop_id": merged["stop_id_origin"],
            "destination_stop_id": merged["stop_id_destination"],
            "departure_minutes": dep,
            "arrival_minutes": arr,
            "duration_min": duration,
        }
    )


def _gtfs_time_to_minutes(value: object) -> float:
    if value is None or pd.isna(value):
        return math.nan
    parts = str(value).split(":")
    if len(parts) < 2:
        return math.nan
    try:
        return int(parts[0]) * 60 + int(parts[1])
    except ValueError:
        return math.nan


def _distance_from_stop_lookup(row: pd.Series, stop_lookup: pd.DataFrame) -> float:
    try:
        origin = stop_lookup.loc[row["origin_stop_id"]]
        destination = stop_lookup.loc[row["destination_stop_id"]]
        return _haversine_km(
            float(origin["stop_lat"]),
            float(origin["stop_lon"]),
            float(destination["stop_lat"]),
            float(destination["stop_lon"]),
        )
    except Exception:
        return math.nan


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return round(2 * radius_km * math.asin(math.sqrt(min(1.0, a))), 1)


def _load_frequency_map(source_dir: Path) -> dict[str, int]:
    frequency: dict[str, int] = {}
    calendar_path = source_dir / "calendar.txt"
    calendar_dates_path = source_dir / "calendar_dates.txt"
    if calendar_path.exists():
        frequency.update(estimate_weekly_frequency_from_calendar(pd.read_csv(calendar_path, dtype=str)))
    if calendar_dates_path.exists():
        from_dates = estimate_weekly_frequency_from_calendar_dates(
            pd.read_csv(calendar_dates_path, dtype=str)
        )
        for service_id, value in from_dates.items():
            frequency.setdefault(service_id, value)
    return frequency


def _classify_train_type(row: pd.Series) -> str:
    text = " ".join(
        str(row.get(column, "")).lower()
        for column in ["route_short_name", "route_long_name", "trip_headsign"]
    )
    if any(keyword in text for keyword in ["night", "nuit", "nightjet", "sleeper", "couchette"]):
        return "night"
    dep = row.get("departure_minutes")
    arr = row.get("arrival_minutes")
    duration = row.get("duration_min")
    if pd.notna(dep) and pd.notna(arr) and pd.notna(duration):
        night_minutes = _night_minutes(float(dep), float(duration))
        if duration >= 240 and night_minutes / duration >= 0.5:
            return "night"
    return "day"


def _night_minutes(departure_minutes: float, duration_minutes: float) -> float:
    night = 0
    current = int(departure_minutes % (24 * 60))
    for _ in range(int(duration_minutes)):
        if current >= 21 * 60 or current < 5 * 60:
            night += 1
        current = (current + 1) % (24 * 60)
    return float(night)


def _back_on_track_countries(
    trips: pd.DataFrame, stops: pd.DataFrame, trip_stop: pd.DataFrame
) -> tuple[pd.Series, pd.Series]:
    countries_from_text = trips.get("countries", pd.Series("", index=trips.index)).apply(
        _split_country_list
    )
    origin = countries_from_text.apply(lambda values: values[0] if values else "unknown")
    destination = countries_from_text.apply(lambda values: values[-1] if values else "unknown")

    if stops.empty or trip_stop.empty:
        return origin, destination

    if "stop_country" not in stops.columns or "stop_id" not in stops.columns:
        return origin, destination

    stop_country = stops.set_index("stop_id")["stop_country"].to_dict()
    ordered = trip_stop.copy()
    ordered["stop_sequence"] = pd.to_numeric(ordered.get("stop_sequence"), errors="coerce")
    ordered = ordered.dropna(subset=["trip_id", "stop_id", "stop_sequence"])
    ordered = ordered.sort_values(["trip_id", "stop_sequence"])
    first = ordered.groupby("trip_id").first()["stop_id"].map(stop_country)
    last = ordered.groupby("trip_id").last()["stop_id"].map(stop_country)

    trip_ids = trips.get("trip_id")
    if trip_ids is None:
        return origin, destination
    origin_from_stops = trip_ids.map(first)
    destination_from_stops = trip_ids.map(last)
    return origin_from_stops.fillna(origin), destination_from_stops.fillna(destination)


def _split_country_list(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _parse_back_on_track_frequency(value: object, frequency_map: dict[str, int]) -> int:
    if value is None or pd.isna(value):
        return 0
    text = str(value)
    candidates = [text, text.split(":", 1)[-1].strip()]
    for candidate in candidates:
        if candidate in frequency_map:
            return int(frequency_map[candidate])
        if candidate.lower() == "daily":
            return 7
    match = re.search(r"(\d+)\s*-\s*(\d+)\s*days/week", text, re.IGNORECASE)
    if match:
        return int(round((int(match.group(1)) + int(match.group(2))) / 2))
    match = re.search(r"(\d+)\s*days/week", text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 0
