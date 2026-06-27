"""Build cleaned European air route datasets for ObRail ML demos."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "raw"
PROCESSED_DIR = ROOT / "processed"

AIRPORT_TYPES = {"large_airport", "medium_airport"}

OPENFLIGHTS_AIRPORT_COLUMNS = [
    "openflights_airport_id",
    "name",
    "city",
    "country",
    "iata_code",
    "icao_code",
    "latitude_deg",
    "longitude_deg",
    "altitude_ft",
    "timezone_offset",
    "dst",
    "tz_database_time_zone",
    "type",
    "source",
]

OPENFLIGHTS_ROUTE_COLUMNS = [
    "airline_iata",
    "airline_id",
    "origin_iata",
    "origin_airport_id",
    "destination_iata",
    "destination_airport_id",
    "codeshare",
    "stops",
    "equipment",
]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(
        dlambda / 2
    ) ** 2
    return round(2 * radius_km * math.asin(math.sqrt(min(1.0, a))), 1)


def load_europe_airports() -> pd.DataFrame:
    airports = pd.read_csv(RAW_DIR / "ourairports_airports.csv")
    airports = airports[
        (airports["continent"] == "EU")
        & (airports["type"].isin(AIRPORT_TYPES))
        & (airports["scheduled_service"] == "yes")
        & (airports["iata_code"].notna())
    ].copy()

    airports = airports[
        [
            "id",
            "ident",
            "type",
            "name",
            "municipality",
            "iso_country",
            "iso_region",
            "iata_code",
            "icao_code",
            "latitude_deg",
            "longitude_deg",
        ]
    ].drop_duplicates(subset=["iata_code"])

    airports["iata_code"] = airports["iata_code"].astype(str).str.upper()
    airports = airports.sort_values(["iso_country", "municipality", "iata_code"])
    return airports.reset_index(drop=True)


def load_openflights_routes() -> pd.DataFrame:
    return pd.read_csv(
        RAW_DIR / "openflights_routes.dat",
        names=OPENFLIGHTS_ROUTE_COLUMNS,
        header=None,
        na_values="\\N",
    )


def load_openflights_airports() -> pd.DataFrame:
    airports = pd.read_csv(
        RAW_DIR / "openflights_airports.dat",
        names=OPENFLIGHTS_AIRPORT_COLUMNS,
        header=None,
        na_values="\\N",
    )
    airports["iata_code"] = airports["iata_code"].astype(str).str.upper()
    return airports


def build_route_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    europe_airports = load_europe_airports()
    airport_lookup = europe_airports.set_index("iata_code")
    europe_iata = set(airport_lookup.index)

    routes = load_openflights_routes()
    routes["origin_iata"] = routes["origin_iata"].astype(str).str.upper()
    routes["destination_iata"] = routes["destination_iata"].astype(str).str.upper()
    routes["stops"] = pd.to_numeric(routes["stops"], errors="coerce").fillna(0).astype(int)

    matched = routes[
        routes["origin_iata"].isin(europe_iata)
        & routes["destination_iata"].isin(europe_iata)
        & (routes["origin_iata"] != routes["destination_iata"])
        & (routes["stops"] == 0)
    ].copy()

    matched["origin_name"] = matched["origin_iata"].map(airport_lookup["name"])
    matched["origin_city"] = matched["origin_iata"].map(airport_lookup["municipality"])
    matched["origin_country"] = matched["origin_iata"].map(airport_lookup["iso_country"])
    matched["origin_latitude"] = matched["origin_iata"].map(airport_lookup["latitude_deg"])
    matched["origin_longitude"] = matched["origin_iata"].map(airport_lookup["longitude_deg"])

    matched["destination_name"] = matched["destination_iata"].map(airport_lookup["name"])
    matched["destination_city"] = matched["destination_iata"].map(
        airport_lookup["municipality"]
    )
    matched["destination_country"] = matched["destination_iata"].map(
        airport_lookup["iso_country"]
    )
    matched["destination_latitude"] = matched["destination_iata"].map(
        airport_lookup["latitude_deg"]
    )
    matched["destination_longitude"] = matched["destination_iata"].map(
        airport_lookup["longitude_deg"]
    )

    matched["air_distance_km"] = matched.apply(
        lambda row: haversine_km(
            row["origin_latitude"],
            row["origin_longitude"],
            row["destination_latitude"],
            row["destination_longitude"],
        ),
        axis=1,
    )
    matched["route_pair"] = matched["origin_iata"] + "-" + matched["destination_iata"]

    grouped = (
        matched.groupby(
            [
                "route_pair",
                "origin_iata",
                "destination_iata",
                "origin_city",
                "destination_city",
                "origin_country",
                "destination_country",
                "origin_latitude",
                "origin_longitude",
                "destination_latitude",
                "destination_longitude",
                "air_distance_km",
            ],
            dropna=False,
        )
        .agg(
            airline_count=("airline_iata", "nunique"),
            airlines=("airline_iata", lambda s: ",".join(sorted({str(v) for v in s.dropna()}))),
            equipment=("equipment", lambda s: ",".join(sorted({str(v) for v in s.dropna()}))[:250]),
        )
        .reset_index()
        .sort_values(["origin_country", "origin_city", "destination_city"])
    )

    demo = grouped[
        grouped["air_distance_km"].between(250, 1500)
        & (grouped["origin_country"] != grouped["destination_country"])
    ].copy()
    demo = demo.sort_values(["airline_count", "air_distance_km"], ascending=[False, True])

    return europe_airports, matched.reset_index(drop=True), grouped.reset_index(drop=True), demo.head(300)


def main() -> int:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    europe_airports, matched_routes, grouped_routes, demo_routes = build_route_tables()

    europe_airports.to_csv(PROCESSED_DIR / "europe_airports.csv", index=False)
    matched_routes.to_csv(PROCESSED_DIR / "europe_air_routes_airline_level.csv", index=False)
    grouped_routes.to_csv(PROCESSED_DIR / "europe_air_routes.csv", index=False)
    demo_routes.to_csv(PROCESSED_DIR / "europe_air_routes_demo.csv", index=False)

    print(f"European airports: {len(europe_airports)}")
    print(f"European airline-level routes: {len(matched_routes)}")
    print(f"European unique OD routes: {len(grouped_routes)}")
    print(f"Demo candidate routes: {len(demo_routes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
