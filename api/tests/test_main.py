from fastapi.testclient import TestClient

from app.main import app
from app.routers import trains as trains_router


client = TestClient(app)


def test_root_endpoint_exposes_api_metadata():
    response = client.get("/")

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Bienvenue sur l'API ObRail Europe"
    assert payload["documentation"] == "/docs"
    assert "trains" in payload["endpoints"]


def test_health_endpoint_returns_basic_status():
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "healthy"
    assert payload["api"] == "running"
    assert payload["database"] == "connected"


def test_trains_endpoint_returns_service_payload(monkeypatch):
    monkeypatch.setattr(
        trains_router.train_service,
        "get_trains",
        lambda **_: [
            {
                "train_id": 1,
                "train_number": "T100",
                "operator_name": "SNCF",
                "train_type": "day",
                "category": "TGV",
                "route_name": "Paris-Lyon",
            }
        ],
    )

    response = client.get("/trains")

    assert response.status_code == 200
    assert response.json()[0]["train_number"] == "T100"
