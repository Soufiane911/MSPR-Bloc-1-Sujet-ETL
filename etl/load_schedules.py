#!/usr/bin/env python3
"""
Chargement des horaires (schedules) depuis les données Back-on-Track.

Télécharge trips + trip_stop depuis GitHub, mappe les IDs depuis la base
existante (trains + stations), puis insère dans la table schedules.

Usage:
    python load_schedules.py
    DATABASE_URL=postgresql://...@127.0.0.1:5433/obrail_db python load_schedules.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent))
from config.logging_config import setup_logging

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError(
        "La variable d'environnement DATABASE_URL est requise. "
        "Copiez .env.example vers .env et renseignez vos valeurs."
    )
BASE_URL = "https://raw.githubusercontent.com/Back-on-Track-eu/night-train-data/main/data/latest"
REF_DATE = datetime(2025, 1, 15, tzinfo=timezone.utc)


def fetch_endpoint(endpoint: str) -> pd.DataFrame:
    url = f"{BASE_URL}/{endpoint}.json"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame.from_dict(r.json(), orient="index").reset_index(names="id_from_key")
    return df


def parse_iso_to_ref_timestamp(time_str) -> datetime | None:
    """
    Convertit '1899-12-30T19:28:00.000Z' en '2025-01-15T19:28:00+00:00'.
    Seule la partie heure/minute/seconde est conservée.
    """
    if not time_str or pd.isna(time_str):
        return None
    try:
        s = str(time_str).replace(".000", "").replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return REF_DATE.replace(hour=dt.hour, minute=dt.minute, second=dt.second)
    except Exception:
        return None


def main() -> int:
    logger = setup_logging("load_schedules")
    logger.info("=== Chargement des horaires Back-on-Track ===")

    # 1. Téléchargement
    logger.info("Téléchargement trips + trip_stop...")
    trips_df = fetch_endpoint("trips")
    trip_stop_df = fetch_endpoint("trip_stop")
    logger.info(f"  trips: {len(trips_df)} lignes, trip_stop: {len(trip_stop_df)} lignes")

    # 2. Agrégation trip_stop → origine/destination par trajet
    trip_stop_df["stop_sequence"] = pd.to_numeric(trip_stop_df["stop_sequence"], errors="coerce")
    trip_stop_sorted = trip_stop_df.sort_values(["trip_id", "stop_sequence"])

    agg = (
        trip_stop_sorted
        .groupby("trip_id", as_index=False)
        .agg(
            origin_stop_id=("stop_id", "first"),
            destination_stop_id=("stop_id", "last"),
            departure_time_str=("departure_time", "first"),
            arrival_time_str=("arrival_time", "last"),
        )
    )

    # 3. Jointure avec trips pour récupérer route_id et distance
    trips_meta = trips_df[["trip_id", "route_id", "distance"]].copy()
    trips_meta["route_id"] = pd.to_numeric(trips_meta["route_id"], errors="coerce")
    trips_meta["distance"] = pd.to_numeric(trips_meta["distance"], errors="coerce")
    agg = agg.merge(trips_meta, on="trip_id", how="left")

    # 4. Connexion DB et récupération des maps train/station
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        trains_db = pd.read_sql(
            "SELECT train_id, train_number FROM trains WHERE source_name='back_on_track'",
            conn,
        )
        stations_db = pd.read_sql(
            "SELECT station_id, uic_code FROM stations "
            "WHERE source_name='back_on_track' AND uic_code IS NOT NULL",
            conn,
        )
    logger.info(f"  DB trains: {len(trains_db)}, DB stations: {len(stations_db)}")

    # 5. Construction des maps de correspondance
    # train_number = "back_on_track_{route_id}" (ex: "back_on_track_24")
    train_id_map = dict(zip(trains_db["train_number"], trains_db["train_id"]))
    # uic_code = "back_on_track_{stop_id}" (ex: "back_on_track_Arad")
    station_id_map = dict(zip(stations_db["uic_code"], stations_db["station_id"]))

    # 6. Mapping des IDs
    # route_id peut être float après to_numeric : "24.0" → "24"
    agg["route_id_str"] = agg["route_id"].apply(
        lambda x: str(int(x)) if pd.notna(x) else None
    )
    agg["train_lookup"] = "back_on_track_" + agg["route_id_str"].astype(str)
    agg["train_id"] = agg["train_lookup"].map(train_id_map)

    agg["origin_uic"] = "back_on_track_" + agg["origin_stop_id"].astype(str)
    agg["destination_uic"] = "back_on_track_" + agg["destination_stop_id"].astype(str)
    agg["origin_id"] = agg["origin_uic"].map(station_id_map)
    agg["destination_id"] = agg["destination_uic"].map(station_id_map)

    logger.info(
        f"  train_id null: {agg['train_id'].isna().sum()}, "
        f"origin_id null: {agg['origin_id'].isna().sum()}, "
        f"destination_id null: {agg['destination_id'].isna().sum()}"
    )

    # 7. Conversion des timestamps
    agg["departure_time"] = agg["departure_time_str"].apply(parse_iso_to_ref_timestamp)
    agg["arrival_time"] = agg["arrival_time_str"].apply(parse_iso_to_ref_timestamp)

    # Trains de nuit : arrivée < départ → arriver le lendemain
    valid = agg["departure_time"].notna() & agg["arrival_time"].notna()
    overnight = valid & (agg["arrival_time"] <= agg["departure_time"])
    agg.loc[overnight, "arrival_time"] = agg.loc[overnight, "arrival_time"] + pd.Timedelta(days=1)

    # 8. Calcul de la durée en minutes
    agg["duration_min"] = (agg["arrival_time"] - agg["departure_time"]).apply(
        lambda x: int(x.total_seconds() / 60) if pd.notna(x) and hasattr(x, "total_seconds") else None
    )

    # 9. Nettoyage
    required = ["train_id", "origin_id", "destination_id", "departure_time", "arrival_time", "duration_min"]
    schedules = agg.dropna(subset=required).copy()
    schedules = schedules[schedules["origin_id"] != schedules["destination_id"]]
    schedules = schedules[schedules["duration_min"] > 0]

    schedules["train_id"] = schedules["train_id"].astype(int)
    schedules["origin_id"] = schedules["origin_id"].astype(int)
    schedules["destination_id"] = schedules["destination_id"].astype(int)
    schedules["duration_min"] = schedules["duration_min"].astype(int)
    schedules["distance_km"] = agg.loc[schedules.index, "distance"]
    schedules["source_name"] = "back_on_track"
    schedules["frequency"] = None

    logger.info(f"  Dessertes valides: {len(schedules)}")

    # 10. Chargement en base (truncate + insert)
    cols = [
        "train_id", "origin_id", "destination_id",
        "departure_time", "arrival_time",
        "duration_min", "distance_km",
        "source_name", "frequency",
    ]
    schedules_to_load = schedules[cols].reset_index(drop=True)

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE schedules RESTART IDENTITY"))
        conn.commit()

    schedules_to_load.to_sql("schedules", engine, if_exists="append", index=False)

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM schedules")).scalar()

    logger.info(f"[OK] {count} dessertes chargees avec succes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
