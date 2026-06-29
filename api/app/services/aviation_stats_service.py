"""
Service for aviation statistics used by the demo dashboard.

The aviation data is stored as processed CSV files outside of the SQL database.
These endpoints intentionally read the CSV snapshots directly so the demo keeps
working even when the train database is unavailable or empty.
"""

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


class AviationStatsService:
    """Compute aviation statistics from processed CSV datasets."""

    def __init__(self, data_dir: Path | None = None):
        self.data_dir = data_dir or self._find_data_dir()

    @staticmethod
    def _find_data_dir() -> Path:
        api_root = Path(__file__).resolve().parents[3]
        candidates = [
            api_root / "data-avion" / "processed",
            api_root.parent / "data-avion" / "processed",
            Path("/data-avion/processed"),
            Path.cwd() / "data-avion" / "processed",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0]

    @staticmethod
    @lru_cache(maxsize=16)
    def _read_csv(path: str) -> pd.DataFrame:
        csv_path = Path(path)
        if not csv_path.exists():
            return pd.DataFrame()

        try:
            return pd.read_csv(csv_path)
        except Exception:
            return pd.DataFrame()

    def _dataset(self, filename: str) -> pd.DataFrame:
        return self._read_csv(str(self.data_dir / filename)).copy()

    def _routes(self) -> pd.DataFrame:
        return self._dataset("europe_air_routes.csv")

    def _demo_routes(self) -> pd.DataFrame:
        demo_routes = self._dataset("europe_air_routes_demo.csv")
        return demo_routes if not demo_routes.empty else self._routes()

    def _airline_routes(self) -> pd.DataFrame:
        return self._dataset("europe_air_routes_airline_level.csv")

    def _airports(self) -> pd.DataFrame:
        return self._dataset("europe_airports.csv")

    @staticmethod
    def _clean_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df.empty:
            return []

        clean_df = df.astype(object).where(pd.notna(df), None)
        return clean_df.to_dict(orient="records")

    @staticmethod
    def _number_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
        if column not in df.columns:
            return pd.Series([default] * len(df), index=df.index, dtype="float64")
        return pd.to_numeric(df[column], errors="coerce").fillna(default)

    @staticmethod
    def _text_series(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
        if column not in df.columns:
            return pd.Series([default] * len(df), index=df.index, dtype="object")
        return df[column].fillna(default).astype(str)

    @staticmethod
    def _estimated_flight_duration(distance_km: pd.Series) -> pd.Series:
        # Approximate block time: airborne cruise plus taxi/climb/approach buffer.
        return ((distance_km / 800) * 60 + 40).round(0).astype(int)

    def get_summary(self) -> Dict[str, int]:
        routes = self._routes()
        airports = self._airports()
        airline_routes = self._airline_routes()

        if "airline_iata" in airline_routes.columns:
            airlines = int(airline_routes["airline_iata"].dropna().nunique())
        elif "airlines" in routes.columns:
            airline_codes = set()
            for value in routes["airlines"].dropna():
                airline_codes.update(code.strip() for code in str(value).split(",") if code.strip())
            airlines = len(airline_codes)
        else:
            airlines = 0

        country_columns = [c for c in ["origin_country", "destination_country"] if c in routes.columns]
        countries = (
            int(pd.concat([routes[c].dropna() for c in country_columns]).nunique())
            if country_columns
            else 0
        )

        return {
            "airlines": airlines,
            "airports": int(len(airports)),
            "flights": int(len(routes)),
            "countries": countries,
        }

    def get_by_country(self) -> List[Dict[str, Any]]:
        routes = self._routes()
        required = {"origin_country", "destination_country", "air_distance_km", "airline_count"}
        if routes.empty or not required.issubset(routes.columns):
            return []

        work = routes.copy()
        work["air_distance_km"] = self._number_series(work, "air_distance_km")
        work["airline_count"] = self._number_series(work, "airline_count")

        grouped = (
            work.groupby("origin_country", dropna=True)
            .agg(
                routes=("origin_iata", "count"),
                destination_countries=("destination_country", "nunique"),
                avg_distance_km=("air_distance_km", "mean"),
                airline_links=("airline_count", "sum"),
            )
            .reset_index()
            .rename(columns={"origin_country": "country"})
            .sort_values(["routes", "country"], ascending=[False, True])
        )
        grouped["avg_distance_km"] = grouped["avg_distance_km"].round(1)
        grouped["airline_links"] = grouped["airline_links"].round(0).astype(int)
        return self._clean_records(grouped)

    def get_top_routes(self, limit: int = 20) -> List[Dict[str, Any]]:
        routes = self._demo_routes()
        if routes.empty:
            return []

        work = routes.copy()
        work["avg_distance_km"] = self._number_series(work, "air_distance_km")
        work["airline_count"] = self._number_series(work, "airline_count")
        work["estimated_duration_min"] = self._estimated_flight_duration(work["avg_distance_km"])
        if "route_pair" in work.columns:
            work["id"] = self._text_series(work, "route_pair")
        else:
            work["id"] = (
                self._text_series(work, "origin_iata")
                + "-"
                + self._text_series(work, "destination_iata")
            )
        work["origin_name"] = (
            self._text_series(work, "origin_city")
            if "origin_city" in work.columns
            else self._text_series(work, "origin_iata")
        )
        work["destination_name"] = (
            self._text_series(work, "destination_city")
            if "destination_city" in work.columns
            else self._text_series(work, "destination_iata")
        )

        columns = [
            "id",
            "route_pair",
            "origin_iata",
            "destination_iata",
            "origin_name",
            "destination_name",
            "origin_city",
            "destination_city",
            "origin_country",
            "destination_country",
            "avg_distance_km",
            "estimated_duration_min",
            "airline_count",
            "airlines",
            "equipment",
        ]
        available_columns = [column for column in columns if column in work.columns]
        result = (
            work.sort_values(["airline_count", "avg_distance_km"], ascending=[False, True])
            .head(limit)[available_columns]
            .copy()
        )
        result["avg_distance_km"] = result["avg_distance_km"].round(1)
        result["airline_count"] = result["airline_count"].round(0).astype(int)
        return self._clean_records(result)

    def get_top_airports(self, limit: int = 20) -> List[Dict[str, Any]]:
        routes = self._routes()
        if routes.empty:
            return []

        origin_columns = {
            "origin_iata": "iata_code",
            "origin_city": "city",
            "origin_country": "country",
            "air_distance_km": "air_distance_km",
            "airline_count": "airline_count",
        }
        destination_columns = {
            "destination_iata": "iata_code",
            "destination_city": "city",
            "destination_country": "country",
            "air_distance_km": "air_distance_km",
            "airline_count": "airline_count",
        }
        has_origin_columns = set(origin_columns).issubset(routes.columns)
        has_destination_columns = set(destination_columns).issubset(routes.columns)
        if not has_origin_columns or not has_destination_columns:
            return []

        origins = routes[list(origin_columns)].rename(columns=origin_columns)
        destinations = routes[list(destination_columns)].rename(columns=destination_columns)
        activity = pd.concat([origins, destinations], ignore_index=True)
        activity["air_distance_km"] = self._number_series(activity, "air_distance_km")
        activity["airline_count"] = self._number_series(activity, "airline_count")

        grouped = (
            activity.groupby(["iata_code", "city", "country"], dropna=True)
            .agg(
                routes=("iata_code", "count"),
                avg_distance_km=("air_distance_km", "mean"),
                airline_links=("airline_count", "sum"),
            )
            .reset_index()
        )

        airports = self._airports()
        airport_columns = ["iata_code", "name", "type"]
        if not airports.empty and set(airport_columns).issubset(airports.columns):
            grouped = grouped.merge(airports[airport_columns], on="iata_code", how="left")
        else:
            grouped["name"] = grouped["city"]
            grouped["type"] = None

        result = grouped.sort_values(["routes", "iata_code"], ascending=[False, True]).head(limit)
        result["avg_distance_km"] = result["avg_distance_km"].round(1)
        result["airline_links"] = result["airline_links"].round(0).astype(int)
        return self._clean_records(result)

    def get_airline_summary(self, limit: int = 50) -> List[Dict[str, Any]]:
        airline_routes = self._airline_routes()
        required = {"airline_iata", "route_pair", "origin_country", "destination_country", "air_distance_km"}
        if airline_routes.empty or not required.issubset(airline_routes.columns):
            return []

        work = airline_routes.copy()
        work["air_distance_km"] = self._number_series(work, "air_distance_km")
        grouped = (
            work.groupby("airline_iata", dropna=True)
            .agg(
                routes=("route_pair", "nunique"),
                route_rows=("route_pair", "count"),
                origin_countries=("origin_country", "nunique"),
                destination_countries=("destination_country", "nunique"),
                avg_distance_km=("air_distance_km", "mean"),
            )
            .reset_index()
            .sort_values(["routes", "airline_iata"], ascending=[False, True])
            .head(limit)
        )
        grouped["avg_distance_km"] = grouped["avg_distance_km"].round(1)
        return self._clean_records(grouped)

    def get_data_quality(self) -> List[Dict[str, Any]]:
        datasets = [
            ("europe_air_routes.csv", self._routes()),
            ("europe_air_routes_airline_level.csv", self._airline_routes()),
            ("europe_airports.csv", self._airports()),
            ("europe_air_routes_demo.csv", self._demo_routes()),
        ]

        quality = []
        for filename, df in datasets:
            missing_values = int(df.isna().sum().sum()) if not df.empty else 0
            total_cells = int(df.shape[0] * df.shape[1])
            missing_percent = round((missing_values / total_cells) * 100, 2) if total_cells else 0.0
            quality.append(
                {
                    "dataset": filename,
                    "available": bool(not df.empty),
                    "rows": int(df.shape[0]),
                    "columns": int(df.shape[1]),
                    "missing_values": missing_values,
                    "missing_percent": missing_percent,
                }
            )

        return quality
