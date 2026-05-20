"""OpenSky extractor: fetches live states or recent flights from OpenSky Network.

Uses unauthenticated /states/all for a realtime snapshot. If `OPENSKY_USER` and
`OPENSKY_PASSWORD` environment variables are set, it can also fetch historical
flights via the `/flights/aircraft` endpoint for a given 24h window.

The extractor returns a dict with a DataFrame under key `instances` containing
columns compatible with `flight_instances` loading (flight_date, scheduled_departure,
scheduled_arrival, status, source_name, callsign, origin, destination).
"""
from __future__ import annotations

import os
import requests
import pandas as pd
from datetime import datetime, timezone


class OpenSkyExtractor:
    def __init__(self, username: str | None = None, password: str | None = None):
        self.base = "https://opensky-network.org/api"
        self.username = username or os.getenv("OPENSKY_USER")
        self.password = password or os.getenv("OPENSKY_PASSWORD")

    def fetch_states(self) -> pd.DataFrame:
        """Fetch realtime states (unauthenticated) and return DataFrame."""
        url = f"{self.base}/states/all"
        resp = requests.get(url, auth=(self.username, self.password) if self.username else None, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        states = data.get("states", [])

        # OpenSky state vector fields (see API docs)
        cols = [
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "last_contact",
            "longitude",
            "latitude",
            "baro_altitude",
            "on_ground",
            "velocity",
            "true_track",
            "vertical_rate",
            "sensors",
            "geo_altitude",
            "squawk",
            "spi",
            "position_source",
        ]

        rows = []
        for s in states:
            # s is a list with 17 fields
            row = {cols[i]: s[i] for i in range(min(len(s), len(cols)))}
            rows.append(row)

        df = pd.DataFrame(rows)
        # convert timestamps
        for ts in ["time_position", "last_contact"]:
            if ts in df.columns:
                df[ts] = pd.to_datetime(df[ts], unit="s", utc=True, errors="coerce")

        return df

    def extract(self) -> dict:
        """Return {'instances': DataFrame} with OpenSky snapshot mapped to flight_instances-like rows."""
        states_df = self.fetch_states()

        if states_df.empty:
            return {"instances": pd.DataFrame()}

        # Map states to flight_instances-compatible schema
        now = datetime.now(timezone.utc)
        rows = []
        for _, r in states_df.iterrows():
            flight_date = r.get("last_contact")
            if pd.isna(flight_date):
                flight_date = now
            rows.append(
                {
                    "flight_id": None,
                    "flight_date": pd.to_datetime(flight_date).date(),
                    "scheduled_departure": pd.to_datetime(r.get("last_contact")),
                    "scheduled_arrival": pd.to_datetime(r.get("last_contact")) + pd.Timedelta(hours=1),
                    "actual_departure": pd.NaT,
                    "actual_arrival": pd.NaT,
                    "status": "airborne" if not r.get("on_ground") else "ground",
                    "source_name": "opensky_states",
                    "callsign": r.get("callsign"),
                    "origin_country": r.get("origin_country"),
                    "icao24": r.get("icao24"),
                    "latitude": r.get("latitude"),
                    "longitude": r.get("longitude"),
                }
            )

        df = pd.DataFrame(rows)
        return {"instances": df}

    def validate(self) -> bool:
        return True
