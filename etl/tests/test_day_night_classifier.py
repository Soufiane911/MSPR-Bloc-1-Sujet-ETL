import pandas as pd

from transformers.day_night_classifier import DayNightClassifier


def test_explicit_night_rule_has_priority():
    classifier = DayNightClassifier(enable_ml=False)

    stop_times = pd.DataFrame(
        [
            {
                "trip_id": "N1",
                "arrival_time": "22:00:00",
                "departure_time": "22:00:00",
                "stop_id": "PAR",
                "stop_sequence": 1,
            },
            {
                "trip_id": "N1",
                "arrival_time": "06:30:00",
                "departure_time": "06:30:00",
                "stop_id": "BER",
                "stop_sequence": 2,
            },
        ]
    )
    trips = pd.DataFrame([{"trip_id": "N1", "route_id": "R1"}])
    routes = pd.DataFrame(
        [{"route_id": "R1", "route_short_name": "Nightjet", "route_type": 105}]
    )

    result = classifier.classify_gtfs_trips(
        stop_times, trips, routes, source_name="db_fernverkehr"
    )
    row = result.iloc[0]

    assert row["train_type"] == "night"
    assert row["classification_method"] == "rule"
    assert row["classification_reason"] == "explicit_night_signal"


def test_heuristic_classification_marks_high_night_share_as_night():
    classifier = DayNightClassifier(enable_ml=False)

    stop_times = pd.DataFrame(
        [
            {
                "trip_id": "H1",
                "arrival_time": "21:30:00",
                "departure_time": "21:30:00",
                "stop_id": "PAR",
                "stop_sequence": 1,
            },
            {
                "trip_id": "H1",
                "arrival_time": "05:45:00",
                "departure_time": "05:45:00",
                "stop_id": "VEN",
                "stop_sequence": 2,
            },
        ]
    )
    trips = pd.DataFrame([{"trip_id": "H1", "route_id": "R2"}])
    routes = pd.DataFrame([{"route_id": "R2", "route_short_name": "IC"}])

    result = classifier.classify_gtfs_trips(
        stop_times, trips, routes, source_name="sncf_intercites"
    )
    row = result.iloc[0]

    assert row["train_type"] == "night"
    assert row["train_type_heuristic"] == "night"
    assert row["classification_method"] == "heuristic"
    assert float(row["night_percentage"]) >= 50.0


def test_ml_support_populates_probabilities_for_ambiguous_cases():
    classifier = DayNightClassifier(enable_ml=True)

    rows = []
    trip_rows = []
    route_rows = []

    for idx in range(15):
        trip_id = f"DAY{idx}"
        rows.extend(
            [
                {
                    "trip_id": trip_id,
                    "arrival_time": "08:00:00",
                    "departure_time": "08:00:00",
                    "stop_id": "A",
                    "stop_sequence": 1,
                },
                {
                    "trip_id": trip_id,
                    "arrival_time": "09:30:00",
                    "departure_time": "09:30:00",
                    "stop_id": "B",
                    "stop_sequence": 2,
                },
            ]
        )
        trip_rows.append({"trip_id": trip_id, "route_id": f"RD{idx}"})
        route_rows.append({"route_id": f"RD{idx}", "route_short_name": "IC"})

    for idx in range(15):
        trip_id = f"NIGHT{idx}"
        rows.extend(
            [
                {
                    "trip_id": trip_id,
                    "arrival_time": "22:00:00",
                    "departure_time": "22:00:00",
                    "stop_id": "A",
                    "stop_sequence": 1,
                },
                {
                    "trip_id": trip_id,
                    "arrival_time": "06:00:00",
                    "departure_time": "06:00:00",
                    "stop_id": "B",
                    "stop_sequence": 2,
                },
            ]
        )
        trip_rows.append({"trip_id": trip_id, "route_id": f"RN{idx}"})
        route_rows.append(
            {"route_id": f"RN{idx}", "route_short_name": "Nightjet", "route_type": 105}
        )

    rows.extend(
        [
            {
                "trip_id": "AMB1",
                "arrival_time": "18:30:00",
                "departure_time": "18:30:00",
                "stop_id": "A",
                "stop_sequence": 1,
            },
            {
                "trip_id": "AMB1",
                "arrival_time": "23:00:00",
                "departure_time": "23:00:00",
                "stop_id": "B",
                "stop_sequence": 2,
            },
        ]
    )
    trip_rows.append({"trip_id": "AMB1", "route_id": "RA1"})
    route_rows.append({"route_id": "RA1", "route_short_name": "IC"})

    stop_times = pd.DataFrame(rows)
    trips = pd.DataFrame(trip_rows)
    routes = pd.DataFrame(route_rows)

    result = classifier.classify_gtfs_trips(
        stop_times, trips, routes, source_name="db_fernverkehr"
    )
    ambiguous = result[result["trip_id"] == "AMB1"].iloc[0]

    assert pd.notna(ambiguous["train_type_ml"])
    assert pd.notna(ambiguous["ml_night_probability"])
    assert 0.0 <= float(ambiguous["ml_night_probability"]) <= 1.0
