"""Tests du filtre de distance minimale."""

import pandas as pd

from transformers.distance_filter import (
    MIN_TRIP_DISTANCE_KM,
    apply_min_distance_to_result,
    filter_trips_by_min_distance,
    haversine_km,
)


def test_haversine_paris_lyon_approx():
    # Paris -> Lyon ~390-400 km
    d = haversine_km(48.8566, 2.3522, 45.7640, 4.8357)
    assert 350 < d < 450


def test_filter_trips_removes_short_segments():
    trips = pd.DataFrame({"trip_id": ["t1", "t2"], "route_id": ["r1", "r1"]})
    stops = pd.DataFrame(
        {
            "stop_id": ["a", "b", "c"],
            "stop_lat": [48.0, 48.01, 52.0],
            "stop_lon": [2.0, 2.01, 3.0],
        }
    )
    stop_times = pd.DataFrame(
        {
            "trip_id": ["t1", "t1", "t2", "t2"],
            "stop_id": ["a", "b", "a", "c"],
            "stop_sequence": [1, 2, 1, 2],
        }
    )

    filtered, filtered_st, _ = filter_trips_by_min_distance(
        "test", trips, stop_times, stops, min_distance_km=100
    )

    assert set(filtered["trip_id"]) == {"t2"}
    assert len(filtered_st) == 2


def test_apply_min_distance_uses_declared_distance():
    result = {
        "trips": pd.DataFrame(
            {
                "trip_id": ["short", "long"],
                "distance": [50, 500],
            }
        ),
        "stop_times": pd.DataFrame({"trip_id": ["short", "long"], "stop_id": ["a", "b"]}),
        "stops": pd.DataFrame(
            {"stop_id": ["a"], "stop_lat": [0.0], "stop_lon": [0.0]}
        ),
    }

    out = apply_min_distance_to_result("back_on_track", result, min_distance_km=100)

    assert list(out["trips"]["trip_id"]) == ["long"]
    assert out["trips"]["distance_km"].iloc[0] == 500
