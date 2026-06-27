"""Train and evaluate the ObRail GradientBoostingClassifier model."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier

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


MODEL_NAME = "gradient_boosting"
DEFAULT_OUTPUT_PATH = OUTPUT_DIR / "gradient_boosting_metrics.json"


def build_estimator() -> GradientBoostingClassifier:
    """Build the Gradient Boosting estimator used for ObRail classification."""

    return GradientBoostingClassifier(
        n_estimators=120,
        learning_rate=0.08,
        max_depth=3,
        random_state=RANDOM_STATE,
    )


def train_and_evaluate(
    dataset: pd.DataFrame | None = None,
    output_path: Path | None = None,
) -> dict:
    """Train Gradient Boosting and persist validation/test metrics."""

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
    """Run Gradient Boosting training from the command line."""

    train_and_evaluate()


if __name__ == "__main__":
    main()
