"""Build the final ObRail ML dataset for flight-to-rail substitution."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from ML.datasetML.clean_dataset import clean_ml_dataset
from ML.datasetML.config import (
    CLASS_DISTRIBUTION_PATH,
    FEATURE_COLUMNS,
    ML_DATASET_PATH,
    PREPARED_DATASET_PATH,
    QUALITY_REPORT_PATH,
    RAW_EXTRACT_PATH,
    TARGET_COLUMN,
)
from ML.datasetML.extract_data import extract_from_local_raw, extract_from_postgres
from ML.datasetML.feature_engineering import (
    add_derived_features,
    assign_substitution_potential,
)
from ML.datasetML.validate_dataset import validate_ml_dataset


FINAL_COLUMN_ORDER = [
    "route_id",
    "schedule_id",
    "source_name",
    "origin",
    "destination",
    "origin_country",
    "destination_country",
    "distance_km",
    "duration_min",
    "duration_minutes",
    "duration_hours",
    "avg_speed_kmh",
    "is_international",
    "train_type",
    "weekly_frequency",
    "estimated_co2_saving_kg",
    TARGET_COLUMN,
]


def build_ml_dataset(source_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Build a cleaned, featured and labeled ML dataset from source rows."""

    harmonized = _harmonize_source_columns(source_df)
    featured = add_derived_features(harmonized)
    cleaned = clean_ml_dataset(featured)
    featured_clean = add_derived_features(cleaned)
    labeled = assign_substitution_potential(featured_clean)
    final = _select_final_columns(labeled)
    report = validate_ml_dataset(final)
    return final, report


def build_from_source(source: str = "auto") -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    """Extract source data, build the ML dataset and return all artifacts."""

    raw = pd.DataFrame()
    if source in {"auto", "postgres"}:
        try:
            raw = extract_from_postgres()
        except Exception:
            if source == "postgres":
                raise

    if raw.empty and source in {"auto", "local"}:
        raw = extract_from_local_raw()

    dataset, report = build_ml_dataset(raw)
    return dataset, report, raw


def write_dataset_outputs(
    dataset: pd.DataFrame,
    report: dict,
    dataset_path: Path = ML_DATASET_PATH,
    report_path: Path = QUALITY_REPORT_PATH,
    raw_extract: pd.DataFrame | None = None,
    raw_extract_path: Path = RAW_EXTRACT_PATH,
    prepared_dataset_path: Path = PREPARED_DATASET_PATH,
    class_distribution_path: Path = CLASS_DISTRIBUTION_PATH,
) -> None:
    """Write final and intermediate dataset artifacts."""

    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    prepared_dataset_path.parent.mkdir(parents=True, exist_ok=True)
    class_distribution_path.parent.mkdir(parents=True, exist_ok=True)

    dataset.to_csv(dataset_path, index=False)
    dataset.to_csv(prepared_dataset_path, index=False)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    class_distribution = (
        dataset[TARGET_COLUMN]
        .value_counts()
        .rename_axis(TARGET_COLUMN)
        .reset_index(name="count")
    )
    class_distribution.to_csv(class_distribution_path, index=False)

    if raw_extract is not None:
        raw_extract_path.parent.mkdir(parents=True, exist_ok=True)
        raw_extract.to_csv(raw_extract_path, index=False)


def _harmonize_source_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    if result.empty:
        return result

    if "duration_minutes" not in result.columns and "duration_min" in result.columns:
        result["duration_minutes"] = result["duration_min"]
    if "duration_min" not in result.columns and "duration_minutes" in result.columns:
        result["duration_min"] = result["duration_minutes"]

    for column in ["distance_km", "duration_min", "duration_minutes"]:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")

    if "train_type" in result.columns:
        result["train_type"] = result["train_type"].fillna("unknown").astype(str).str.lower()
        result["train_type"] = result["train_type"].replace(
            {
                "jour": "day",
                "nuit": "night",
                "tgv": "day",
                "ter": "day",
            }
        )

    return result


def _select_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in FINAL_COLUMN_ORDER:
        if column not in result.columns:
            result[column] = pd.NA

    numeric_columns = [
        "distance_km",
        "duration_min",
        "duration_minutes",
        "duration_hours",
        "avg_speed_kmh",
        "weekly_frequency",
        "estimated_co2_saving_kg",
    ]
    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    result["distance_km"] = result["distance_km"].round(1)
    result["duration_hours"] = result["duration_hours"].round(3)
    result["avg_speed_kmh"] = result["avg_speed_kmh"].round(2)
    result["estimated_co2_saving_kg"] = result["estimated_co2_saving_kg"].round(2)

    return result[FINAL_COLUMN_ORDER].reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the ObRail ML dataset")
    parser.add_argument(
        "--source",
        choices=["auto", "postgres", "local"],
        default="auto",
        help="Data source to use. auto tries PostgreSQL, then local raw files.",
    )
    parser.add_argument("--output", type=Path, default=ML_DATASET_PATH)
    parser.add_argument("--report", type=Path, default=QUALITY_REPORT_PATH)
    args = parser.parse_args()

    dataset, report, raw = build_from_source(args.source)
    write_dataset_outputs(dataset, report, args.output, args.report, raw_extract=raw)

    print(f"Rows extracted: {len(raw)}")
    print(f"Rows in final dataset: {len(dataset)}")
    print(f"Valid dataset: {report['is_valid']}")
    print(f"Final dataset: {args.output}")
    print(f"Quality report: {args.report}")
    return 0 if report["is_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
