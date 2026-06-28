from fastapi.testclient import TestClient
from unittest.mock import patch

from app.main import app
from app.routers import predict as predict_router


client = TestClient(app)


def test_predict_returns_model_prediction():
    response = client.post(
        "/predict/",
        json={
            "distance_km": 820,
            "duration_min": 510,
            "is_international": True,
            "train_type": "night",
            "weekly_frequency": 7,
            "estimated_co2_saving_kg": 140.5,
            "origin_country": "FR",
            "destination_country": "IT",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["prediction"] in {
        "faible_potentiel",
        "potentiel_moyen",
        "fort_potentiel",
    }
    assert 0 <= payload["confidence"] <= 1
    assert payload["model_name"]
    assert "probabilities" in payload
    assert "explanation" in payload
    assert "inference_ms" in payload
    assert "predicted_at" in payload


def test_predict_validation_error_on_invalid_train_type():
    response = client.post(
        "/predict/",
        json={
            "distance_km": 200,
            "duration_min": 180,
            "is_international": False,
            "train_type": "intercity",
            "weekly_frequency": 4,
            "estimated_co2_saving_kg": 52,
            "origin_country": "FR",
            "destination_country": "FR",
        },
    )

    assert response.status_code == 422


def test_predict_metrics_exposed():
    client.post(
        "/predict/",
        json={
            "distance_km": 700,
            "duration_min": 480,
            "is_international": True,
            "train_type": "night",
            "weekly_frequency": 6,
            "estimated_co2_saving_kg": 120,
            "origin_country": "DE",
            "destination_country": "AT",
        },
    )

    metrics_response = client.get("/metrics")
    assert metrics_response.status_code == 200
    body = metrics_response.text
    assert "model_predictions_total" in body
    assert "model_prediction_latency_seconds" in body


def test_predict_error_metric_incremented_on_inference_failure():
    failing_client = TestClient(app, raise_server_exceptions=False)

    with patch.object(predict_router.predictor, "predict", side_effect=RuntimeError("boom")):
        response = failing_client.post(
            "/predict/",
            json={
                "distance_km": 700,
                "duration_min": 480,
                "is_international": True,
                "train_type": "night",
                "weekly_frequency": 6,
                "estimated_co2_saving_kg": 120,
                "origin_country": "DE",
                "destination_country": "AT",
            },
        )

    assert response.status_code == 500

    metrics_response = client.get("/metrics")
    assert metrics_response.status_code == 200
    assert "model_prediction_errors_total" in metrics_response.text
