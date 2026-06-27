import json

import pandas as pd

from ML.datasetML.build_dataset import build_ml_dataset, write_dataset_outputs
from ML.datasetML.extract_data import (
    estimate_weekly_frequency_from_calendar,
    parse_back_on_track_duration_minutes,
)


def test_build_ml_dataset_creates_clean_featured_targeted_dataset():
    source = pd.DataFrame(
        {
            "route_id": ["A", "B", "C", "bad"],
            "schedule_id": ["s1", "s2", "s3", "s4"],
            "origin_country": ["FR", "FR", "FR", "FR"],
            "destination_country": ["DE", "FR", "ES", "IT"],
            "distance_km": [650, 250, 80, -1],
            "duration_min": [300, 600, 70, 100],
            "train_type": ["night", "day", None, "day"],
            "weekly_frequency": [7, None, 3, 1],
        }
    )

    dataset, report = build_ml_dataset(source)

    assert dataset["route_id"].tolist() == ["A", "B", "C"]
    assert "duration_min" in dataset.columns
    assert "duration_minutes" in dataset.columns
    assert "duration_hours" in dataset.columns
    assert "avg_speed_kmh" in dataset.columns
    assert "is_international" in dataset.columns
    assert "estimated_co2_saving_kg" in dataset.columns
    assert "substitution_potential" in dataset.columns
    assert dataset.set_index("route_id")["substitution_potential"].to_dict() == {
        "A": "fort_potentiel",
        "B": "potentiel_moyen",
        "C": "faible_potentiel",
    }
    assert report["is_valid"] is True
    assert report["row_count"] == 3


def test_write_dataset_outputs_writes_csv_and_quality_report(tmp_path):
    dataset, report = build_ml_dataset(
        pd.DataFrame(
            {
                "route_id": ["A"],
                "origin_country": ["FR"],
                "destination_country": ["DE"],
                "distance_km": [650],
                "duration_min": [300],
                "train_type": ["night"],
            }
        )
    )

    csv_path = tmp_path / "obrail_ml_dataset.csv"
    report_path = tmp_path / "data_quality_report.json"
    raw_path = tmp_path / "raw_obrail_extract.csv"
    prepared_path = tmp_path / "prepared_obrail_dataset.csv"
    distribution_path = tmp_path / "class_distribution.csv"

    write_dataset_outputs(
        dataset,
        report,
        csv_path,
        report_path,
        raw_extract=dataset,
        raw_extract_path=raw_path,
        prepared_dataset_path=prepared_path,
        class_distribution_path=distribution_path,
    )

    written = pd.read_csv(csv_path)
    assert written["route_id"].tolist() == ["A"]
    assert json.loads(report_path.read_text())["row_count"] == 1
    assert pd.read_csv(prepared_path).shape[0] == 1
    assert pd.read_csv(raw_path).shape[0] == 1
    assert pd.read_csv(distribution_path)["count"].sum() == 1


def test_build_ml_dataset_handles_empty_source_without_crashing():
    dataset, report = build_ml_dataset(pd.DataFrame())

    assert dataset.empty
    assert report["row_count"] == 0
    assert report["is_valid"] is False


def test_cleaning_keeps_distinct_schedules_even_when_route_id_is_shared():
    source = pd.DataFrame(
        {
            "route_id": ["same-route", "same-route"],
            "schedule_id": ["schedule-1", "schedule-2"],
            "origin_country": ["FR", "FR"],
            "destination_country": ["DE", "DE"],
            "distance_km": [650, 650],
            "duration_min": [300, 320],
            "train_type": ["day", "day"],
        }
    )

    dataset, report = build_ml_dataset(source)

    assert dataset["schedule_id"].tolist() == ["schedule-1", "schedule-2"]
    assert report["row_count"] == 2


def test_parse_back_on_track_duration_minutes_handles_iso_like_time_values():
    assert parse_back_on_track_duration_minutes("1899-12-30T08:05:00.000Z") == 485
    assert parse_back_on_track_duration_minutes("08:05:00") == 485
    assert parse_back_on_track_duration_minutes("") is None


def test_estimate_weekly_frequency_from_calendar_uses_weekday_flags():
    calendar = pd.DataFrame(
        {
            "service_id": ["daily", "weekend"],
            "monday": [1, 0],
            "tuesday": [1, 0],
            "wednesday": [1, 0],
            "thursday": [1, 0],
            "friday": [1, 0],
            "saturday": [1, 1],
            "sunday": [1, 1],
        }
    )

    result = estimate_weekly_frequency_from_calendar(calendar)

    assert result == {"daily": 7, "weekend": 2}
