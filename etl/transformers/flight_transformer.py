"""Transformers for OpenFlights data: normalize airports/airlines and compute distances."""
import pandas as pd
import math

# European country names to keep (OpenFlights uses country names)
EUROPEAN_COUNTRY_NAMES = set(
    [
        "France",
        "Germany",
        "Italy",
        "Spain",
        "Switzerland",
        "Austria",
        "Belgium",
        "Netherlands",
        "United Kingdom",
        "Sweden",
        "Norway",
        "Denmark",
        "Finland",
        "Ireland",
        "Portugal",
        "Poland",
        "Czech Republic",
        "Hungary",
        "Romania",
        "Bulgaria",
        "Croatia",
        "Slovenia",
        "Slovakia",
        "Lithuania",
        "Latvia",
        "Estonia",
        "Luxembourg",
    ]
)

EARTH_RADIUS_KM = 6371.0


def haversine(lat1, lon1, lat2, lon2):
    if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
        return None
    # convert to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(min(1, math.sqrt(a)))
    return EARTH_RADIUS_KM * c


class FlightTransformer:
    def __init__(self):
        pass

    def normalize_airports(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df2 = df.copy()
        # standardize columns
        if "tz_database_time_zone" in df2.columns:
            df2["timezone"] = df2["tz_database_time_zone"].fillna(df2.get("timezone"))
        df2.rename(columns={
            "name": "name",
            "city": "city",
            "country": "country",
            "iata": "iata",
            "icao": "icao",
            "latitude": "latitude",
            "longitude": "longitude",
        }, inplace=True)

        # coerce lat/lon
        df2["latitude"] = pd.to_numeric(df2["latitude"], errors="coerce")
        df2["longitude"] = pd.to_numeric(df2["longitude"], errors="coerce")

        # filter to Europe only (country names)
        if "country" in df2.columns:
            df2["country"] = df2["country"].astype(str).str.strip()
            df2 = df2[df2["country"].isin(EUROPEAN_COUNTRY_NAMES)]

        # keep only relevant columns
        keep = [c for c in ["name", "city", "country", "iata", "icao", "latitude", "longitude", "timezone"] if c in df2.columns]
        return df2[keep].drop_duplicates()

    def normalize_airlines(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df2 = df.copy()
        df2.rename(columns={"name": "name", "iata": "iata", "icao": "icao", "country": "country"}, inplace=True)
        keep = [c for c in ["name", "alias", "iata", "icao", "country"] if c in df2.columns]
        return df2[keep].drop_duplicates()

    def build_flights_from_routes(self, routes_df: pd.DataFrame, airports_df: pd.DataFrame, airlines_df: pd.DataFrame) -> pd.DataFrame:
        """Join routes with airport coordinates and airline ids to compute distance."""
        if routes_df.empty:
            return routes_df

        r = routes_df.copy()
        # ensure ids are numeric when present
        r["source_airport_id"] = pd.to_numeric(r.get("source_airport_id"), errors="coerce")
        r["dest_airport_id"] = pd.to_numeric(r.get("dest_airport_id"), errors="coerce")

        # Build mapping by airport iata/icao
        airports_map = airports_df.copy()
        airports_map["iata"] = airports_map.get("iata").fillna(airports_map.get("icao"))
        airports_map = airports_map.set_index("iata")

        def lookup_coords(code):
            try:
                row = airports_map.loc[code]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                return row.get("latitude"), row.get("longitude")
            except Exception:
                return None, None

        distances = []
        for _, row in r.iterrows():
            src = row.get("source_airport")
            dst = row.get("dest_airport")
            lat1, lon1 = lookup_coords(src)
            lat2, lon2 = lookup_coords(dst)
            d = haversine(lat1, lon1, lat2, lon2)
            distances.append(d)

        r["distance_km"] = distances
        # Build a deterministic pseudo flight number from route endpoints.
        r["flight_number"] = (
            r["airline"].fillna("").astype(str).str.upper()
            + "_"
            + r["source_airport"].fillna("").astype(str).str.upper()
            + "_"
            + r["dest_airport"].fillna("").astype(str).str.upper()
        )
        r["origin_iata"] = r["source_airport"]
        r["dest_iata"] = r["dest_airport"]
        keep = [
            c
            for c in [
                "airline",
                "airline_id",
                "flight_number",
                "origin_iata",
                "dest_iata",
                "distance_km",
                "equipment",
            ]
            if c in r.columns
        ]
        return r[keep]
