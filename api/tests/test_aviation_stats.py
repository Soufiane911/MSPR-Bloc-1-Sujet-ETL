from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_aviation_stats_summary_returns_frontend_contract():
    response = client.get("/aviationStats/summary")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload).issuperset({"airlines", "airports", "flights", "countries"})
    assert payload["airports"] >= 0
    assert payload["flights"] >= 0


def test_aviation_stats_list_endpoints_return_json_without_database():
    endpoints = [
        "/aviationStats/byCountry",
        "/aviationStats/topRoutes?limit=5",
        "/aviationStats/topAirports?limit=5",
        "/aviationStats/airlineSummary",
        "/aviationStats/dataQuality",
    ]

    for endpoint in endpoints:
        response = client.get(endpoint)

        assert response.status_code == 200, endpoint
        assert isinstance(response.json(), list)


def test_top_routes_uses_demo_frontend_fields():
    response = client.get("/aviationStats/topRoutes?limit=3")

    assert response.status_code == 200
    routes = response.json()
    if routes:
        assert set(routes[0]).issuperset(
                {
                    "origin_iata",
                    "destination_iata",
                    "origin_country",
                    "destination_country",
                    "avg_distance_km",
                    "estimated_duration_min",
                }
            )
        assert routes[0]["estimated_duration_min"] > 0
