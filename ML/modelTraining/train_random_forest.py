"""Train and evaluate the ObRail RandomForestClassifier model."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.ensemble import RandomForestClassifier

from ML.modelTraining.common import (
    OUTPUT_DIR,
    RANDOM_STATE,
    build_metrics_payload,
    build_model_pipeline,
    evaluate_classifier,
    load_training_dataset,
    split_training_data,
    write_json,
)


MODEL_NAME = "random_forest"
DEFAULT_OUTPUT_PATH = OUTPUT_DIR / "random_forest_metrics.json"


def build_estimator() -> RandomForestClassifier:
    """Build the Random Forest estimator used for ObRail classification."""

    return RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        class_weight="balanced",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )


def train_and_evaluate(
    dataset: pd.DataFrame | None = None,
    output_path: Path | None = None,
) -> dict:
    """Train the Random Forest model and persist its metrics payload."""

    training_dataset = load_training_dataset() if dataset is None else dataset
    metrics_output_path = DEFAULT_OUTPUT_PATH if output_path is None else output_path

    splits = split_training_data(training_dataset)
    estimator = build_estimator()
    model = build_model_pipeline(estimator)
    model.fit(splits.X_train, splits.y_train)

    validation_metrics = evaluate_classifier(
        model,
        splits.X_validation,
        splits.y_validation,
    )
    test_metrics = evaluate_classifier(
        model,
        splits.X_test,
        splits.y_test,
    )
    payload = build_metrics_payload(
        MODEL_NAME,
        estimator,
        validation_metrics,
        test_metrics,
    )
    write_json(payload, metrics_output_path)

    return payload


def main() -> None:
    """Run Random Forest training from the command line."""

    train_and_evaluate()


if __name__ == "__main__":
    main()
