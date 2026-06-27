"""Train and evaluate the ObRail Logistic Regression classifier."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.linear_model import LogisticRegression

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


MODEL_NAME = "logistic_regression"
DEFAULT_OUTPUT_PATH = OUTPUT_DIR / "logistic_regression_metrics.json"


def build_estimator() -> LogisticRegression:
    """Build the Logistic Regression estimator for imbalanced classification."""

    return LogisticRegression(
        class_weight="balanced",
        max_iter=1_000,
        random_state=RANDOM_STATE,
    )


def train_and_evaluate(
    dataset: pd.DataFrame | None = None,
    output_path: Path | None = None,
) -> dict:
    """Train Logistic Regression and write validation/test metrics."""

    training_dataset = load_training_dataset() if dataset is None else dataset
    metrics_path = DEFAULT_OUTPUT_PATH if output_path is None else output_path

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

    write_json(payload, metrics_path)
    return payload


def main() -> None:
    """Run the Logistic Regression training job from the command line."""

    train_and_evaluate()


if __name__ == "__main__":
    main()
