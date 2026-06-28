"""Service de prediction branche au modele ML pousse dans `models/`."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import joblib

from app.models.prediction import PredictionRequest


def _candidate_model_paths() -> list[Path]:
    env_path = os.getenv("OBRAIL_MODEL_PATH", "").strip()
    candidates = []
    if env_path:
        candidates.append(Path(env_path))

    base_dir = Path(__file__).resolve().parents[3]
    candidates.extend(
        [
            base_dir / "models" / "obrail_substitution_model.joblib",
            base_dir.parent / "models" / "obrail_substitution_model.joblib",
            Path.cwd() / "models" / "obrail_substitution_model.joblib",
        ]
    )
    return candidates


def _find_model_path() -> Path:
    for path in _candidate_model_paths():
        if path.exists():
            return path
    looked_up = "\n".join(str(p) for p in _candidate_model_paths())
    raise FileNotFoundError(
        f"Model file 'obrail_substitution_model.joblib' introuvable. Chemins testes:\n{looked_up}"
    )


def _metadata_path_for(model_path: Path) -> Path:
    return model_path.parent / "model_metadata.json"


@lru_cache(maxsize=1)
def _load_model() -> Any:
    model_path = _find_model_path()
    return joblib.load(model_path)


@lru_cache(maxsize=1)
def _load_metadata() -> dict[str, Any]:
    model_path = _find_model_path()
    metadata_path = _metadata_path_for(model_path)
    if not metadata_path.exists():
        return {}
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def _prepare_prediction_input(payload: PredictionRequest):
    # Reuse the exact ML contract utility when available.
    try:
        from ML.modelTraining.predict import prepare_prediction_input  # type: ignore

        return prepare_prediction_input(payload.model_dump())
    except Exception:
        # Safe fallback if ML package import path is unavailable.
        import pandas as pd

        distance_km = float(payload.distance_km)
        duration_min = float(payload.duration_min)
        avg_speed_kmh = round(distance_km / (duration_min / 60), 3)
        return pd.DataFrame(
            [
                {
                    "distance_km": distance_km,
                    "duration_min": duration_min,
                    "avg_speed_kmh": avg_speed_kmh,
                    "weekly_frequency": float(payload.weekly_frequency),
                    "estimated_co2_saving_kg": float(payload.estimated_co2_saving_kg),
                    "is_international": bool(payload.is_international),
                    "train_type": payload.train_type,
                    "origin_country": payload.origin_country,
                    "destination_country": payload.destination_country,
                }
            ]
        )


def _build_explanation(payload: PredictionRequest, prediction: str) -> str:
    signals = []
    if payload.distance_km >= 500:
        signals.append("distance elevee")
    if payload.is_international:
        signals.append("liaison internationale")
    if payload.train_type == "night":
        signals.append("train de nuit")
    if payload.estimated_co2_saving_kg >= 100:
        signals.append("economie CO2 importante")
    if payload.weekly_frequency >= 7:
        signals.append("frequence hebdomadaire favorable")
    if payload.duration_min > 480:
        signals.append("duree elevee")
    if not signals:
        signals.append("criteres de substitution moderes")
    return f"Prediction {prediction} basee sur : {', '.join(signals)}."


@dataclass
class ModelPredictor:
    model_name: str = "gradient_boosting"
    model_version: str = "unknown"

    def predict(self, payload: PredictionRequest) -> dict:
        metadata = _load_metadata()
        if metadata.get("selected_model"):
            self.model_name = str(metadata["selected_model"])
        if metadata.get("trained_at_utc"):
            self.model_version = str(metadata["trained_at_utc"])

        model = _load_model()
        features = _prepare_prediction_input(payload)

        prediction = str(model.predict(features)[0])

        probabilities: dict[str, float] = {}
        confidence = 1.0
        if hasattr(model, "predict_proba"):
            probs = model.predict_proba(features)[0]
            classes = getattr(model, "classes_", None)
            if classes is None and hasattr(model, "named_steps"):
                classes = getattr(model.named_steps.get("model"), "classes_", None)
            if classes is not None:
                probabilities = {
                    str(label): round(float(probability), 4)
                    for label, probability in zip(classes, probs, strict=False)
                }
                confidence = probabilities.get(prediction, confidence)

        return {
            "prediction": prediction,
            "confidence": round(float(confidence), 4),
            "probabilities": probabilities,
            "explanation": _build_explanation(payload, prediction),
            "model_name": self.model_name,
            "model_version": self.model_version,
        }


predictor = ModelPredictor()
