import pandas as pd

from ML.datasetML.clean_dataset import clean_ml_dataset
from ML.datasetML.feature_engineering import (
    add_derived_features,
    assign_substitution_potential,
)
from ML.datasetML.validate_dataset import validate_ml_dataset


def test_add_derived_features_computes_expected_columns():
    df = pd.DataFrame(
        {
            "route_id": ["FR-DE-001", "FR-FR-002"],
            "origin_country": ["FR", "FR"],
            "destination_country": ["DE", "FR"],
            "distance_km": [600, 120],
            "duration_minutes": [360, 90],
            "train_type": ["TGV", "TER"],
            "co2_saving_kg": [None, 12],
        }
    )

    result = add_derived_features(df)

    assert result["duration_hours"].tolist() == [6.0, 1.5]
    assert result["avg_speed_kmh"].round(2).tolist() == [100.0, 80.0]
    assert result["is_international"].tolist() == [True, False]
    assert result["estimated_co2_saving_kg"].tolist() == [90.0, 12.0]
    assert result["weekly_frequency"].tolist() == [0, 0]
    assert "duration_hours" not in df.columns


def test_assign_substitution_potential_covers_all_three_classes():
    df = pd.DataFrame(
        {
            "route_id": ["strong", "medium", "low"],
            "distance_km": [650, 250, 80],
            "duration_minutes": [300, 600, 70],
            "origin_country": ["FR", "FR", "FR"],
            "destination_country": ["DE", "FR", "FR"],
            "estimated_co2_saving_kg": [95, 35, 10],
            "avg_speed_kmh": [130, 25, 68],
        }
    )

    result = assign_substitution_potential(df)

    assert result.set_index("route_id")["substitution_potential"].to_dict() == {
        "strong": "fort_potentiel",
        "medium": "potentiel_moyen",
        "low": "faible_potentiel",
    }


def test_clean_ml_dataset_removes_impossible_rows_fills_missing_values_and_deduplicates():
    df = pd.DataFrame(
        {
            "route_id": ["A", "A", "B", "C", "D"],
            "schedule_id": ["s1", "s2", "s3", "s4", "s5"],
            "distance_km": [500, 500, 0, 120, 300],
            "duration_minutes": [300, 300, 100, -10, 60],
            "avg_speed_kmh": [100, 100, 0, 80, 600],
            "train_type": [None, "TGV", "TER", "TER", "ICE"],
            "origin_country": [None, "FR", "FR", "FR", "FR"],
            "destination_country": ["DE", "DE", None, "FR", "ES"],
            "weekly_frequency": [None, 7, 2, 1, None],
            "estimated_co2_saving_kg": [None, 75, 0, 5, None],
        }
    )

    result = clean_ml_dataset(df)

    assert len(result) == 2
    assert result["schedule_id"].tolist() == ["s1", "s2"]
    row = result.iloc[0]
    assert row["route_id"] == "A"
    assert row["train_type"] == "unknown"
    assert row["origin_country"] == "unknown"
    assert row["destination_country"] == "DE"
    assert row["weekly_frequency"] == 0
    assert row["estimated_co2_saving_kg"] == 0


def test_validate_ml_dataset_reports_schema_missing_values_and_classes():
    df = pd.DataFrame(
        {
            "route_id": ["A", "B"],
            "distance_km": [500, None],
            "duration_min": [300, 600],
            "duration_minutes": [300, 600],
            "duration_hours": [5.0, 10.0],
            "avg_speed_kmh": [100, 50],
            "is_international": [True, False],
            "train_type": ["night", "day"],
            "estimated_co2_saving_kg": [80, 20],
            "weekly_frequency": [7, 3],
            "origin_country": ["FR", "FR"],
            "destination_country": ["DE", "FR"],
            "substitution_potential": ["fort_potentiel", "potentiel_moyen"],
        }
    )

    report = validate_ml_dataset(df)

    assert report["row_count"] == 2
    assert report["missing_counts"]["distance_km"] == 1
    assert report["class_distribution"] == {
        "fort_potentiel": 1,
        "potentiel_moyen": 1,
        "faible_potentiel": 0,
    }
    assert report["required_columns_present"] is True
    assert report["is_valid"] is False
