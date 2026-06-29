from app.services.trajet_service import filter_plausible_trajets


def test_filter_plausible_trajets_removes_impossible_speed():
    rows = [
        {
            "trajet_id": 1,
            "distance_km": 1036,
            "duration_min": 13,
        },
        {
            "trajet_id": 2,
            "distance_km": 1036,
            "duration_min": 676,
        },
    ]

    result = filter_plausible_trajets(rows)

    assert [row["trajet_id"] for row in result] == [2]
