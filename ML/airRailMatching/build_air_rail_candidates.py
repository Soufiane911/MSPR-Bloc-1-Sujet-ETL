"""Build candidate rail alternatives for European air routes."""

from __future__ import annotations

import argparse
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_AIR_ROUTES_PATH = PROJECT_ROOT / "data-avion" / "processed" / "europe_air_routes.csv"
DEFAULT_RAIL_DATASET_PATH = PROJECT_ROOT / "data" / "ml" / "obrail_ml_dataset.csv"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data-avion" / "processed" / "air_rail_candidates.csv"
DEFAULT_UNMATCHED_OUTPUT_PATH = PROJECT_ROOT / "data-avion" / "processed" / "air_rail_unmatched.csv"

MIN_AIR_DISTANCE_KM = 250
MAX_AIR_DISTANCE_KM = 1500
BUSY_ROUTE_MIN_AIRLINES = 2
MAX_DISTANCE_GAP_RATIO = 1.0

POTENTIAL_SCORE = {
    "fort_potentiel": 6.0,
    "potentiel_moyen": 3.0,
    "faible_potentiel": 0.0,
}
POTENTIAL_RANK = {
    "fort_potentiel": 3,
    "potentiel_moyen": 2,
    "faible_potentiel": 1,
}

OUTPUT_COLUMNS = [
    "route_avion",
    "origin_iata",
    "destination_iata",
    "origine_avion",
    "destination_avion",
    "origin_country",
    "destination_country",
    "distance_avion",
    "airline_count",
    "route_train_candidate",
    "origin_train",
    "destination_train",
    "distance_train",
    "duration_train",
    "train_type",
    "weekly_frequency",
    "estimated_co2_saving_kg",
    "substitution_potential",
    "match_score",
    "match_method",
]

UNMATCHED_COLUMNS = [
    "route_avion",
    "origin_iata",
    "destination_iata",
    "origine_avion",
    "destination_avion",
    "origin_country",
    "destination_country",
    "distance_avion",
    "airline_count",
    "unmatched_reason",
]


def normalize_city_name(value: object) -> str:
    """Normalize city/station names for lightweight text matching."""

    if value is None or pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^A-Za-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def filter_relevant_air_routes(
    air_routes: pd.DataFrame,
    min_distance_km: float = MIN_AIR_DISTANCE_KM,
    max_distance_km: float = MAX_AIR_DISTANCE_KM,
    busy_route_min_airlines: int = BUSY_ROUTE_MIN_AIRLINES,
) -> pd.DataFrame:
    """Keep air routes relevant for flight-to-rail substitution analysis."""

    required = {
        "route_pair",
        "origin_country",
        "destination_country",
        "air_distance_km",
        "airline_count",
    }
    missing = required - set(air_routes.columns)
    if missing:
        raise ValueError(f"Missing required air route columns: {sorted(missing)}")

    result = air_routes.copy()
    result["air_distance_km"] = pd.to_numeric(result["air_distance_km"], errors="coerce")
    result["airline_count"] = pd.to_numeric(result["airline_count"], errors="coerce").fillna(0)
    result["is_international_air"] = (
        result["origin_country"].astype(str) != result["destination_country"].astype(str)
    )

    mask = result["air_distance_km"].between(min_distance_km, max_distance_km)
    mask &= result["is_international_air"] | result["airline_count"].ge(
        busy_route_min_airlines
    )
    return result.loc[mask].reset_index(drop=True)


def build_air_rail_candidates(
    air_routes: pd.DataFrame,
    rail_routes: pd.DataFrame,
    top_n: int = 1,
) -> pd.DataFrame:
    """Return the best rail candidate(s) for each relevant air route."""

    candidates, _ = build_air_rail_matching_outputs(air_routes, rail_routes, top_n=top_n)
    return candidates


def build_air_rail_unmatched(
    air_routes: pd.DataFrame,
    rail_routes: pd.DataFrame,
) -> pd.DataFrame:
    """Return relevant air routes that could not be matched to a rail candidate."""

    _, unmatched = build_air_rail_matching_outputs(air_routes, rail_routes, top_n=1)
    return unmatched


def build_air_rail_matching_outputs(
    air_routes: pd.DataFrame,
    rail_routes: pd.DataFrame,
    top_n: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return candidate matches and unmatched relevant air routes."""

    relevant_air = filter_relevant_air_routes(air_routes)
    rail = _prepare_rail_routes(rail_routes)
    if relevant_air.empty or rail.empty:
        return (
            pd.DataFrame(columns=OUTPUT_COLUMNS),
            pd.DataFrame(columns=UNMATCHED_COLUMNS),
        )

    grouped = {
        key: group.reset_index(drop=True)
        for key, group in rail.groupby(["origin_country", "destination_country"])
    }
    reverse_grouped = {
        key: group.reset_index(drop=True)
        for key, group in rail.groupby(["destination_country", "origin_country"])
    }

    rows: list[dict] = []
    unmatched_rows: list[dict] = []
    for air_row in relevant_air.itertuples(index=False):
        candidates, method_base = _candidate_pool(air_row, grouped, reverse_grouped)
        if candidates.empty:
            unmatched_rows.append(_format_unmatched_row(air_row, "no_country_pair_in_rail_dataset"))
            continue

        candidates = _filter_distance_compatible(candidates, float(air_row.air_distance_km))
        if candidates.empty:
            unmatched_rows.append(_format_unmatched_row(air_row, "no_distance_compatible_rail_candidate"))
            continue

        scored = candidates.copy()
        scores = scored.apply(lambda rail_row: _score_candidate(air_row, rail_row), axis=1)
        scored["match_score"] = scores
        scored["potential_rank"] = (
            scored["substitution_potential"].map(POTENTIAL_RANK).fillna(0).astype(int)
        )
        scored = scored.sort_values(
            ["match_score", "potential_rank", "weekly_frequency", "duration_min"],
            ascending=[False, False, False, True],
        ).head(top_n)

        for rail_row in scored.itertuples(index=False):
            rows.append(_format_candidate_row(air_row, rail_row, method_base))

    candidates_output = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    unmatched_output = pd.DataFrame(unmatched_rows, columns=UNMATCHED_COLUMNS)
    return candidates_output, unmatched_output


def write_air_rail_candidates(candidates: pd.DataFrame, output_path: Path = DEFAULT_OUTPUT_PATH) -> None:
    """Write air-rail candidates to CSV."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(output_path, index=False)


def write_air_rail_unmatched(
    unmatched: pd.DataFrame,
    output_path: Path = DEFAULT_UNMATCHED_OUTPUT_PATH,
) -> None:
    """Write unmatched relevant air routes to CSV."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    unmatched.to_csv(output_path, index=False)


def write_air_rail_outputs(
    candidates: pd.DataFrame,
    unmatched: pd.DataFrame,
    candidates_output_path: Path = DEFAULT_OUTPUT_PATH,
    unmatched_output_path: Path = DEFAULT_UNMATCHED_OUTPUT_PATH,
) -> None:
    """Write both matching outputs."""

    write_air_rail_candidates(candidates, candidates_output_path)
    write_air_rail_unmatched(unmatched, unmatched_output_path)


def load_inputs(
    air_routes_path: Path = DEFAULT_AIR_ROUTES_PATH,
    rail_dataset_path: Path = DEFAULT_RAIL_DATASET_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load air and rail datasets."""

    return pd.read_csv(air_routes_path), pd.read_csv(rail_dataset_path)


def _prepare_rail_routes(rail_routes: pd.DataFrame) -> pd.DataFrame:
    required = {
        "route_id",
        "origin",
        "destination",
        "origin_country",
        "destination_country",
        "distance_km",
        "duration_min",
        "train_type",
        "weekly_frequency",
        "estimated_co2_saving_kg",
        "substitution_potential",
    }
    missing = required - set(rail_routes.columns)
    if missing:
        raise ValueError(f"Missing required rail route columns: {sorted(missing)}")

    result = rail_routes.copy()
    result["distance_km"] = pd.to_numeric(result["distance_km"], errors="coerce")
    result["duration_min"] = pd.to_numeric(result["duration_min"], errors="coerce")
    result["weekly_frequency"] = pd.to_numeric(result["weekly_frequency"], errors="coerce").fillna(0)
    result["estimated_co2_saving_kg"] = pd.to_numeric(
        result["estimated_co2_saving_kg"], errors="coerce"
    ).fillna(0)
    result["origin_norm"] = result["origin"].apply(normalize_city_name)
    result["destination_norm"] = result["destination"].apply(normalize_city_name)
    result = result.dropna(subset=["distance_km", "duration_min"])
    return result.reset_index(drop=True)


def _candidate_pool(air_row, grouped: dict, reverse_grouped: dict) -> tuple[pd.DataFrame, str]:
    key = (str(air_row.origin_country), str(air_row.destination_country))
    direct = grouped.get(key)
    if direct is not None and not direct.empty:
        return direct, "country"

    reverse = reverse_grouped.get(key)
    if reverse is not None and not reverse.empty:
        return reverse, "country_reverse"

    return pd.DataFrame(), "none"


def _score_candidate(air_row, rail_row: pd.Series) -> float:
    origin_similarity = _similarity(
        normalize_city_name(getattr(air_row, "origin_city", "")),
        rail_row.get("origin_norm", ""),
    )
    destination_similarity = _similarity(
        normalize_city_name(getattr(air_row, "destination_city", "")),
        rail_row.get("destination_norm", ""),
    )
    city_score = 30.0 * ((origin_similarity + destination_similarity) / 2)

    air_distance = float(getattr(air_row, "air_distance_km"))
    train_distance = float(rail_row["distance_km"])
    distance_gap_ratio = abs(train_distance - air_distance) / max(air_distance, 1)
    distance_score = max(0.0, 25.0 * (1.0 - distance_gap_ratio))

    frequency_score = min(float(rail_row.get("weekly_frequency", 0)), 7.0)
    potential_score = POTENTIAL_SCORE.get(str(rail_row.get("substitution_potential")), 0.0)

    return round(50.0 + city_score + distance_score + frequency_score + potential_score, 3)


def _filter_distance_compatible(
    candidates: pd.DataFrame,
    air_distance_km: float,
    max_gap_ratio: float = MAX_DISTANCE_GAP_RATIO,
) -> pd.DataFrame:
    """Keep rail candidates whose distance remains comparable to the air route."""

    if candidates.empty:
        return candidates
    gap_ratio = (candidates["distance_km"] - air_distance_km).abs() / max(air_distance_km, 1)
    return candidates.loc[gap_ratio <= max_gap_ratio].copy()


def _similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    if left in right or right in left:
        return 1.0
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    token_score = 0.0
    if left_tokens and right_tokens:
        token_score = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    sequence_score = SequenceMatcher(None, left, right).ratio()
    return max(token_score, sequence_score)


def _format_candidate_row(air_row, rail_row, method_base: str) -> dict:
    city_signal = _has_city_signal(air_row, rail_row)
    method = f"{method_base}_city_distance" if city_signal else f"{method_base}_distance"
    return {
        "route_avion": air_row.route_pair,
        "origin_iata": getattr(air_row, "origin_iata", ""),
        "destination_iata": getattr(air_row, "destination_iata", ""),
        "origine_avion": getattr(air_row, "origin_city", ""),
        "destination_avion": getattr(air_row, "destination_city", ""),
        "origin_country": air_row.origin_country,
        "destination_country": air_row.destination_country,
        "distance_avion": round(float(air_row.air_distance_km), 1),
        "airline_count": int(getattr(air_row, "airline_count", 0)),
        "route_train_candidate": rail_row.route_id,
        "origin_train": rail_row.origin,
        "destination_train": rail_row.destination,
        "distance_train": round(float(rail_row.distance_km), 1),
        "duration_train": round(float(rail_row.duration_min), 1),
        "train_type": rail_row.train_type,
        "weekly_frequency": int(float(rail_row.weekly_frequency)),
        "estimated_co2_saving_kg": round(float(rail_row.estimated_co2_saving_kg), 2),
        "substitution_potential": rail_row.substitution_potential,
        "match_score": round(float(rail_row.match_score), 3),
        "match_method": method,
    }


def _format_unmatched_row(air_row, reason: str) -> dict:
    return {
        "route_avion": air_row.route_pair,
        "origin_iata": getattr(air_row, "origin_iata", ""),
        "destination_iata": getattr(air_row, "destination_iata", ""),
        "origine_avion": getattr(air_row, "origin_city", ""),
        "destination_avion": getattr(air_row, "destination_city", ""),
        "origin_country": air_row.origin_country,
        "destination_country": air_row.destination_country,
        "distance_avion": round(float(air_row.air_distance_km), 1),
        "airline_count": int(getattr(air_row, "airline_count", 0)),
        "unmatched_reason": reason,
    }


def _has_city_signal(air_row, rail_row) -> bool:
    origin_similarity = _similarity(
        normalize_city_name(getattr(air_row, "origin_city", "")),
        getattr(rail_row, "origin_norm", ""),
    )
    destination_similarity = _similarity(
        normalize_city_name(getattr(air_row, "destination_city", "")),
        getattr(rail_row, "destination_norm", ""),
    )
    return origin_similarity >= 0.55 or destination_similarity >= 0.55


def main() -> int:
    parser = argparse.ArgumentParser(description="Build air-to-rail candidate routes")
    parser.add_argument("--air-routes", type=Path, default=DEFAULT_AIR_ROUTES_PATH)
    parser.add_argument("--rail-dataset", type=Path, default=DEFAULT_RAIL_DATASET_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--unmatched-output", type=Path, default=DEFAULT_UNMATCHED_OUTPUT_PATH)
    parser.add_argument("--top-n", type=int, default=1)
    args = parser.parse_args()

    air_routes, rail_routes = load_inputs(args.air_routes, args.rail_dataset)
    candidates, unmatched = build_air_rail_matching_outputs(
        air_routes, rail_routes, top_n=args.top_n
    )
    write_air_rail_outputs(candidates, unmatched, args.output, args.unmatched_output)

    print(f"Air routes loaded: {len(air_routes)}")
    print(f"Rail routes loaded: {len(rail_routes)}")
    print(f"Candidates written: {len(candidates)}")
    print(f"Unmatched relevant air routes written: {len(unmatched)}")
    print(f"Output: {args.output}")
    print(f"Unmatched output: {args.unmatched_output}")
    if candidates.empty:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
