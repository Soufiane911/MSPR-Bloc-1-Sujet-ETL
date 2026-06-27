import pandas as pd

from ML.airRailMatching.build_air_rail_candidates import (
    build_air_rail_matching_outputs,
    build_air_rail_candidates,
    build_air_rail_unmatched,
    filter_relevant_air_routes,
    normalize_city_name,
    write_air_rail_candidates,
    write_air_rail_outputs,
)


def test_normalize_city_name_removes_noise_and_accents():
    assert normalize_city_name("Frankfurt am Main") == "frankfurt am main"
    assert normalize_city_name("Caselle Torinese (TO)") == "caselle torinese"
    assert normalize_city_name("București Nord") == "bucuresti nord"


def test_filter_relevant_air_routes_keeps_distance_and_international_or_busy_routes():
    air = pd.DataFrame(
        {
            "route_pair": ["A-B", "C-D", "E-F", "G-H"],
            "origin_country": ["FR", "FR", "FR", "FR"],
            "destination_country": ["DE", "FR", "ES", "IT"],
            "air_distance_km": [500, 600, 1600, 200],
            "airline_count": [1, 2, 5, 5],
        }
    )

    result = filter_relevant_air_routes(air)

    assert result["route_pair"].tolist() == ["A-B", "C-D"]


def test_build_air_rail_candidates_prefers_country_city_and_distance_match():
    air = pd.DataFrame(
        {
            "route_pair": ["FRA-MAD", "CDG-FCO"],
            "origin_iata": ["FRA", "CDG"],
            "destination_iata": ["MAD", "FCO"],
            "origin_city": ["Frankfurt am Main", "Paris"],
            "destination_city": ["Madrid", "Rome"],
            "origin_country": ["DE", "FR"],
            "destination_country": ["ES", "IT"],
            "air_distance_km": [1420, 1100],
            "airline_count": [4, 3],
        }
    )
    rail = pd.DataFrame(
        {
            "route_id": ["bad-country", "best-fra-mad", "fr-it"],
            "origin": ["Frankfurt", "Frankfurt", "Paris Gare de Lyon"],
            "destination": ["Vienna", "Madrid", "Roma Termini"],
            "origin_country": ["DE", "DE", "FR"],
            "destination_country": ["AT", "ES", "IT"],
            "distance_km": [700, 1500, 1150],
            "duration_min": [400, 480, 660],
            "train_type": ["day", "day", "night"],
            "weekly_frequency": [7, 5, 3],
            "estimated_co2_saving_kg": [100, 213, 165],
            "substitution_potential": [
                "potentiel_moyen",
                "fort_potentiel",
                "potentiel_moyen",
            ],
        }
    )

    result = build_air_rail_candidates(air, rail)

    assert result["route_avion"].tolist() == ["FRA-MAD", "CDG-FCO"]
    assert result.set_index("route_avion").loc["FRA-MAD", "route_train_candidate"] == "best-fra-mad"
    assert result.set_index("route_avion").loc["FRA-MAD", "match_method"] == "country_city_distance"
    assert result.set_index("route_avion").loc["CDG-FCO", "route_train_candidate"] == "fr-it"


def test_build_air_rail_candidates_falls_back_to_country_match_when_city_names_differ():
    air = pd.DataFrame(
        {
            "route_pair": ["TIA-MXP"],
            "origin_iata": ["TIA"],
            "destination_iata": ["MXP"],
            "origin_city": ["Rinas"],
            "destination_city": ["Ferno"],
            "origin_country": ["AL"],
            "destination_country": ["IT"],
            "air_distance_km": [1000],
            "airline_count": [5],
        }
    )
    rail = pd.DataFrame(
        {
            "route_id": ["al-it-country-only"],
            "origin": ["Tirana"],
            "destination": ["Milano Centrale"],
            "origin_country": ["AL"],
            "destination_country": ["IT"],
            "distance_km": [1050],
            "duration_min": [700],
            "train_type": ["day"],
            "weekly_frequency": [2],
            "estimated_co2_saving_kg": [150],
            "substitution_potential": ["potentiel_moyen"],
        }
    )

    result = build_air_rail_candidates(air, rail)

    assert len(result) == 1
    assert result.iloc[0]["match_method"] == "country_distance"


def test_build_air_rail_candidates_rejects_country_match_with_incompatible_distance():
    air = pd.DataFrame(
        {
            "route_pair": ["GRZ-MUC"],
            "origin_iata": ["GRZ"],
            "destination_iata": ["MUC"],
            "origin_city": ["Graz"],
            "destination_city": ["Munich"],
            "origin_country": ["AT"],
            "destination_country": ["DE"],
            "air_distance_km": [313],
            "airline_count": [2],
        }
    )
    rail = pd.DataFrame(
        {
            "route_id": ["too-long"],
            "origin": ["Innsbruck Hbf"],
            "destination": ["Hamburg Altona"],
            "origin_country": ["AT"],
            "destination_country": ["DE"],
            "distance_km": [1112],
            "duration_min": [740],
            "train_type": ["night"],
            "weekly_frequency": [7],
            "estimated_co2_saving_kg": [166.8],
            "substitution_potential": ["faible_potentiel"],
        }
    )

    result = build_air_rail_candidates(air, rail)

    assert result.empty


def test_build_air_rail_unmatched_explains_missing_country_pair():
    air = pd.DataFrame(
        {
            "route_pair": ["FRA-MAD"],
            "origin_iata": ["FRA"],
            "destination_iata": ["MAD"],
            "origin_city": ["Frankfurt am Main"],
            "destination_city": ["Madrid"],
            "origin_country": ["DE"],
            "destination_country": ["ES"],
            "air_distance_km": [1420],
            "airline_count": [4],
        }
    )
    rail = pd.DataFrame(
        {
            "route_id": ["de-domestic"],
            "origin": ["Frankfurt"],
            "destination": ["Berlin"],
            "origin_country": ["DE"],
            "destination_country": ["DE"],
            "distance_km": [550],
            "duration_min": [300],
            "train_type": ["day"],
            "weekly_frequency": [7],
            "estimated_co2_saving_kg": [80],
            "substitution_potential": ["potentiel_moyen"],
        }
    )

    result = build_air_rail_unmatched(air, rail)

    assert len(result) == 1
    assert result.iloc[0]["route_avion"] == "FRA-MAD"
    assert result.iloc[0]["unmatched_reason"] == "no_country_pair_in_rail_dataset"


def test_build_air_rail_matching_outputs_returns_candidates_and_unmatched():
    air = pd.DataFrame(
        {
            "route_pair": ["CDG-FCO", "FRA-MAD"],
            "origin_iata": ["CDG", "FRA"],
            "destination_iata": ["FCO", "MAD"],
            "origin_city": ["Paris", "Frankfurt am Main"],
            "destination_city": ["Rome", "Madrid"],
            "origin_country": ["FR", "DE"],
            "destination_country": ["IT", "ES"],
            "air_distance_km": [1100, 1420],
            "airline_count": [3, 4],
        }
    )
    rail = pd.DataFrame(
        {
            "route_id": ["fr-it"],
            "origin": ["Paris Gare de Lyon"],
            "destination": ["Roma Termini"],
            "origin_country": ["FR"],
            "destination_country": ["IT"],
            "distance_km": [1150],
            "duration_min": [660],
            "train_type": ["night"],
            "weekly_frequency": [3],
            "estimated_co2_saving_kg": [165],
            "substitution_potential": ["potentiel_moyen"],
        }
    )

    candidates, unmatched = build_air_rail_matching_outputs(air, rail)

    assert candidates["route_avion"].tolist() == ["CDG-FCO"]
    assert unmatched["route_avion"].tolist() == ["FRA-MAD"]


def test_write_air_rail_candidates_writes_csv(tmp_path):
    candidates = pd.DataFrame(
        {
            "route_avion": ["FRA-MAD"],
            "route_train_candidate": ["train-1"],
            "match_score": [92.5],
        }
    )
    output = tmp_path / "air_rail_candidates.csv"

    write_air_rail_candidates(candidates, output)

    written = pd.read_csv(output)
    assert written["route_avion"].tolist() == ["FRA-MAD"]


def test_write_air_rail_outputs_writes_candidates_and_unmatched_csv(tmp_path):
    candidates = pd.DataFrame({"route_avion": ["CDG-FCO"]})
    unmatched = pd.DataFrame(
        {
            "route_avion": ["FRA-MAD"],
            "unmatched_reason": ["no_country_pair_in_rail_dataset"],
        }
    )
    candidates_output = tmp_path / "air_rail_candidates.csv"
    unmatched_output = tmp_path / "air_rail_unmatched.csv"

    write_air_rail_outputs(candidates, unmatched, candidates_output, unmatched_output)

    assert pd.read_csv(candidates_output)["route_avion"].tolist() == ["CDG-FCO"]
    assert pd.read_csv(unmatched_output)["route_avion"].tolist() == ["FRA-MAD"]
