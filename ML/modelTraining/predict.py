"""Run predictions with the saved ObRail substitution model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from ML.modelTraining.common import FEATURE_COLUMNS, MODELS_DIR


DEFAULT_MODEL_PATH = MODELS_DIR / "obrail_substitution_model.joblib"
REQUIRED_INPUT_FIELDS = [
    "distance_km",
    "duration_min",
    "is_international",
    "train_type",
    "weekly_frequency",
    "estimated_co2_saving_kg",
    "origin_country",
    "destination_country",
]


def load_model(model_path: Path = DEFAULT_MODEL_PATH) -> Any:
    """Load the persisted sklearn pipeline."""

    return joblib.load(model_path)


def prepare_prediction_input(payload: dict[str, Any]) -> pd.DataFrame:
    """Convert one API-style JSON payload to the shared feature schema."""

    missing = [field for field in REQUIRED_INPUT_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"Missing prediction fields: {missing}")

    row = {feature: payload.get(feature) for feature in FEATURE_COLUMNS}
    if row.get("avg_speed_kmh") is None:
        distance_km = float(payload["distance_km"])
        duration_min = float(payload["duration_min"])
        if duration_min <= 0:
            raise ValueError("duration_min must be greater than 0")
        row["avg_speed_kmh"] = round(distance_km / (duration_min / 60), 3)

    return pd.DataFrame([row], columns=FEATURE_COLUMNS)


def predict_payload(
    payload: dict[str, Any],
    model_path: Path = DEFAULT_MODEL_PATH,
) -> dict[str, Any]:
    """Predict substitution potential for one route payload."""

    model = load_model(model_path)
    features = prepare_prediction_input(payload)
    prediction = str(model.predict(features)[0])
    probabilities = _predict_probabilities(model, features)
    confidence = probabilities.get(prediction)
    if confidence is None:
        confidence = 1.0

    return {
        "prediction": prediction,
        "confidence": round(float(confidence), 4),
        "probabilities": probabilities,
        "explanation": build_explanation(payload, prediction),
    }


def build_explanation(payload: dict[str, Any], prediction: str) -> str:
    """Build a short business-readable explanation from input signals."""

    signals = []
    distance = float(payload["distance_km"])
    duration = float(payload["duration_min"])
    co2 = float(payload["estimated_co2_saving_kg"])
    weekly_frequency = float(payload["weekly_frequency"])

    if distance >= 500:
        signals.append("distance elevee")
    elif distance < 250:
        signals.append("distance courte")

    if bool(payload["is_international"]):
        signals.append("liaison internationale")

    if payload["train_type"] == "night":
        signals.append("train de nuit")

    if co2 >= 100:
        signals.append("economie CO2 importante")

    if weekly_frequency >= 7:
        signals.append("frequence hebdomadaire favorable")

    if duration > 480:
        signals.append("duree elevee")

    if not signals:
        signals.append("criteres de substitution moderes")

    return f"Prediction {prediction} basee sur : {', '.join(signals)}."


def _predict_probabilities(model: Any, features: pd.DataFrame) -> dict[str, float]:
    if not hasattr(model, "predict_proba"):
        return {}

    probabilities = model.predict_proba(features)[0]
    classes = getattr(model, "classes_", None)
    if classes is None and hasattr(model, "named_steps"):
        classes = getattr(model.named_steps.get("model"), "classes_", None)
    if classes is None:
        return {}

    return {
        str(label): round(float(probability), 4)
        for label, probability in zip(classes, probabilities, strict=False)
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Predict ObRail substitution potential")
    parser.add_argument("--input-json", type=Path, required=True)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL_PATH)
    args = parser.parse_args()

    payload = json.loads(args.input_json.read_text(encoding="utf-8"))
    result = predict_payload(payload, args.model)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
