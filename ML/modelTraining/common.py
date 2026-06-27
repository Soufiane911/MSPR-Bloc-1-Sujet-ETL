"""Shared training utilities for ObRail substitution potential models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATASET_PATH = PROJECT_ROOT / "data" / "ml" / "obrail_ml_dataset.csv"
OUTPUT_DIR = PROJECT_ROOT / "ML" / "modelTraining" / "outputs"
MODELS_DIR = PROJECT_ROOT / "models"
FIGURES_DIR = PROJECT_ROOT / "rendu" / "figures"

RANDOM_STATE = 42
TARGET_COLUMN = "substitution_potential"
LABEL_ORDER = ["faible_potentiel", "potentiel_moyen", "fort_potentiel"]

NUMERIC_FEATURES = [
    "distance_km",
    "duration_min",
    "avg_speed_kmh",
    "weekly_frequency",
    "estimated_co2_saving_kg",
]
CATEGORICAL_FEATURES = [
    "is_international",
    "train_type",
    "origin_country",
    "destination_country",
]
FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES


@dataclass(frozen=True)
class DatasetSplits:
    """Container for reproducible train/validation/test data."""

    X_train: pd.DataFrame
    X_validation: pd.DataFrame
    X_test: pd.DataFrame
    y_train: pd.Series
    y_validation: pd.Series
    y_test: pd.Series


def load_training_dataset(dataset_path: Path = DATASET_PATH) -> pd.DataFrame:
    """Load the ML dataset used for substitution-potential training."""

    return pd.read_csv(dataset_path)


def split_training_data(
    dataset: pd.DataFrame,
    random_state: int = RANDOM_STATE,
) -> DatasetSplits:
    """Split data into stratified 70/15/15 train/validation/test sets."""

    _validate_training_columns(dataset)
    clean_dataset = dataset.dropna(subset=[TARGET_COLUMN]).copy()
    X = clean_dataset[FEATURE_COLUMNS]
    y = clean_dataset[TARGET_COLUMN]

    X_train, X_temp, y_train, y_temp = train_test_split(
        X,
        y,
        train_size=0.70,
        random_state=random_state,
        stratify=y,
    )
    X_validation, X_test, y_validation, y_test = train_test_split(
        X_temp,
        y_temp,
        test_size=0.50,
        random_state=random_state,
        stratify=y_temp,
    )

    return DatasetSplits(
        X_train=X_train.reset_index(drop=True),
        X_validation=X_validation.reset_index(drop=True),
        X_test=X_test.reset_index(drop=True),
        y_train=y_train.reset_index(drop=True),
        y_validation=y_validation.reset_index(drop=True),
        y_test=y_test.reset_index(drop=True),
    )


def build_preprocessor() -> ColumnTransformer:
    """Build the shared preprocessing fitted only on the training split."""

    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipeline, NUMERIC_FEATURES),
            ("categorical", categorical_pipeline, CATEGORICAL_FEATURES),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )


def build_model_pipeline(estimator: Any) -> Pipeline:
    """Attach an estimator to the shared preprocessing pipeline."""

    return Pipeline(
        steps=[
            ("preprocessor", build_preprocessor()),
            ("model", estimator),
        ]
    )


def evaluate_classifier(model: Pipeline, X: pd.DataFrame, y: pd.Series) -> dict[str, Any]:
    """Evaluate a fitted classifier with metrics suited to imbalanced classes."""

    predictions = model.predict(X)
    return {
        "accuracy": round(float(accuracy_score(y, predictions)), 6),
        "balanced_accuracy": round(float(balanced_accuracy_score(y, predictions)), 6),
        "precision_macro": round(
            float(precision_score(y, predictions, average="macro", zero_division=0)), 6
        ),
        "recall_macro": round(
            float(recall_score(y, predictions, average="macro", zero_division=0)), 6
        ),
        "f1_macro": round(
            float(f1_score(y, predictions, average="macro", zero_division=0)), 6
        ),
        "classification_report": classification_report(
            y,
            predictions,
            labels=LABEL_ORDER,
            output_dict=True,
            zero_division=0,
        ),
        "confusion_matrix": confusion_matrix(
            y,
            predictions,
            labels=LABEL_ORDER,
        ).tolist(),
    }


def build_metrics_payload(
    model_name: str,
    estimator: Any,
    validation_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Create a consistent JSON payload for one model experiment."""

    return {
        "model_name": model_name,
        "primary_metric": "f1_macro",
        "secondary_metric": "balanced_accuracy",
        "hyperparameters": estimator.get_params(),
        "validation": validation_metrics,
        "test": test_metrics,
    }


def write_json(payload: dict[str, Any], output_path: Path) -> None:
    """Write a JSON artifact with stable formatting."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def ensure_output_dirs() -> None:
    """Create training artifact directories."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def _validate_training_columns(dataset: pd.DataFrame) -> None:
    required_columns = set(FEATURE_COLUMNS + [TARGET_COLUMN])
    missing = required_columns - set(dataset.columns)
    if missing:
        raise ValueError(f"Missing required training columns: {sorted(missing)}")
