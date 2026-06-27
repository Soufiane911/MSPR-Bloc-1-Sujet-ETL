"""Train and evaluate the ObRail MLPClassifier model."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.metrics import accuracy_score
from sklearn.neural_network import MLPClassifier

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


MODEL_NAME = "mlp"
DEFAULT_OUTPUT_PATH = OUTPUT_DIR / "mlp_metrics.json"


class ObRailMLPClassifier(MLPClassifier):
    """MLPClassifier variant whose early-stopping score supports string labels."""

    def _score(self, X, y, sample_weight=None):
        predictions = self.predict(X)
        return accuracy_score(y, predictions, sample_weight=sample_weight)


def build_estimator() -> MLPClassifier:
    """Build the MLP classifier used for ObRail classification."""

    return ObRailMLPClassifier(
        hidden_layer_sizes=(64, 32),
        activation="relu",
        solver="adam",
        alpha=0.0001,
        learning_rate_init=0.001,
        max_iter=400,
        early_stopping=True,
        n_iter_no_change=20,
        random_state=RANDOM_STATE,
    )


def train_and_evaluate(
    dataset: pd.DataFrame | None = None,
    output_path: Path | None = None,
) -> dict:
    """Train the MLP classifier and persist validation/test metrics."""

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
    """Run MLP training from the command line."""

    train_and_evaluate()


if __name__ == "__main__":
    main()
