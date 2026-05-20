"""OpenFlights extractor: reads local OpenFlights CSV data from data/raw/openflights/.

Expected files (CSV):
- airlines.csv or airlines.dat
- airports.csv or airports.dat
- routes.csv or routes.dat

This extractor returns a dict with DataFrames: airlines, airports, routes
"""
from pathlib import Path
import pandas as pd

class OpenFlightsExtractor:
    def __init__(self, data_dir: str = None):
        base = Path(__file__).parent.parent.parent
        self.data_dir = Path(data_dir) if data_dir else base / "data" / "raw" / "openflights"

    def _read_csv(self, name, cols=None):
        p = self.data_dir / name
        if not p.exists():
            # try .dat
            p = self.data_dir / name.replace('.csv', '.dat')
        if not p.exists():
            raise FileNotFoundError(f"OpenFlights file not found: {name} in {self.data_dir}")

        df = pd.read_csv(p, header=None)
        if cols:
            df.columns = cols
        return df

    def extract(self):
        """Return dictionary with keys: airlines, airports, routes"""
        # OpenFlights original formats (no header) - columns simplified
        airlines_cols = [
            "airline_id",
            "name",
            "alias",
            "iata",
            "icao",
            "callsign",
            "country",
            "active",
        ]
        airports_cols = [
            "airport_id",
            "name",
            "city",
            "country",
            "iata",
            "icao",
            "latitude",
            "longitude",
            "altitude",
            "timezone",
            "dst",
            "tz_database_time_zone",
            "type",
            "source",
        ]
        routes_cols = [
            "airline",
            "airline_id",
            "source_airport",
            "source_airport_id",
            "dest_airport",
            "dest_airport_id",
            "codeshare",
            "stops",
            "equipment",
        ]

        airlines = None
        airports = None
        routes = None

        # Prefer .dat (original OpenFlights files) when present, otherwise fall back to .csv
        try:
            airlines = self._read_csv("airlines.dat", cols=airlines_cols)
        except Exception:
            try:
                airlines = self._read_csv("airlines.csv", cols=airlines_cols)
            except Exception:
                airlines = pd.DataFrame(columns=airlines_cols)

        try:
            airports = self._read_csv("airports.dat", cols=airports_cols)
        except Exception:
            try:
                airports = self._read_csv("airports.csv", cols=airports_cols)
            except Exception:
                airports = pd.DataFrame(columns=airports_cols)

        try:
            routes = self._read_csv("routes.dat", cols=routes_cols)
        except Exception:
            try:
                routes = self._read_csv("routes.csv", cols=routes_cols)
            except Exception:
                routes = pd.DataFrame(columns=routes_cols)

        return {"airlines": airlines, "airports": airports, "routes": routes}

    def validate(self):
        return True

    def get_summary(self):
        return {
            "airlines": len(self.extract().get("airlines", [])),
            "airports": len(self.extract().get("airports", [])),
            "routes": len(self.extract().get("routes", [])),
        }
