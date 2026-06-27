import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression

from ML.modelTraining.common import TARGET_COLUMN, build_model_pipeline
from ML.modelTraining.predict import prepare_prediction_input, predict_payload


def _sample_training_frame(rows_per_class=20):
    records = []
    classes = ["faible_potentiel", "potentiel_moyen", "fort_potentiel"]
    for class_index, label in enumerate(classes):
        for row_index in range(rows_per_class):
            records.append(
                {
                    "distance_km": 250 + class_index * 250 + row_index,
                    "duration_min": 120 + class_index * 90 + row_index,
                    "avg_speed_kmh": 80 + class_index * 10,
                    "is_international": class_index > 0,
                    "train_type": "night" if class_index == 2 else "day",
                    "weekly_frequency": 3 + class_index,
                    "estimated_co2_saving_kg": 35 + class_index * 70 + row_index,
                    "origin_country": ["FR", "DE", "ES"][class_index],
                    "destination_country": ["BE", "IT", "PT"][class_index],
                    TARGET_COLUMN: label,
                }
            )
    return pd.DataFrame(records)


def test_prepare_prediction_input_derives_average_speed_when_missing():
    payload = {
        "distance_km": 750,
        "duration_min": 390,
        "is_international": True,
        "train_type": "night",
        "weekly_frequency": 7,
        "estimated_co2_saving_kg": 120,
        "origin_country": "FR",
        "destination_country": "DE",
    }

    frame = prepare_prediction_input(payload)

    assert frame.loc[0, "avg_speed_kmh"] == round(750 / (390 / 60), 3)


def test_predict_payload_returns_prediction_confidence_and_explanation(tmp_path):
    dataset = _sample_training_frame()
    X = dataset.drop(columns=[TARGET_COLUMN])
    y = dataset[TARGET_COLUMN]
    model = build_model_pipeline(LogisticRegression(max_iter=500))
    model.fit(X, y)
    model_path = tmp_path / "model.joblib"
    joblib.dump(model, model_path)

    payload = {
        "distance_km": 750,
        "duration_min": 390,
        "is_international": True,
        "train_type": "night",
        "weekly_frequency": 7,
        "estimated_co2_saving_kg": 120,
        "origin_country": "FR",
        "destination_country": "DE",
    }

    result = predict_payload(payload, model_path=model_path)

    assert set(result).issuperset(
        {"prediction", "confidence", "probabilities", "explanation"}
    )
    assert result["prediction"] in {
        "faible_potentiel",
        "potentiel_moyen",
        "fort_potentiel",
    }
    assert 0 <= result["confidence"] <= 1
    assert isinstance(result["explanation"], str)
    assert result["explanation"]
