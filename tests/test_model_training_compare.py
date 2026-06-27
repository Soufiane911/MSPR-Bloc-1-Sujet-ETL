import json

from ML.modelTraining.compare_models import build_model_comparison, select_best_model


def _write_metrics(path, model_name, validation_f1, validation_balanced_accuracy):
    payload = {
        "model_name": model_name,
        "primary_metric": "f1_macro",
        "secondary_metric": "balanced_accuracy",
        "hyperparameters": {"example": True},
        "validation": {
            "accuracy": 0.8,
            "balanced_accuracy": validation_balanced_accuracy,
            "precision_macro": 0.7,
            "recall_macro": 0.7,
            "f1_macro": validation_f1,
            "classification_report": {
                "fort_potentiel": {"f1-score": validation_f1 - 0.1}
            },
            "confusion_matrix": [[1, 0, 0], [0, 1, 0], [0, 0, 1]],
        },
        "test": {
            "accuracy": 0.75,
            "balanced_accuracy": validation_balanced_accuracy - 0.05,
            "precision_macro": 0.65,
            "recall_macro": 0.65,
            "f1_macro": validation_f1 - 0.05,
            "classification_report": {
                "fort_potentiel": {"f1-score": validation_f1 - 0.15}
            },
            "confusion_matrix": [[1, 0, 0], [0, 1, 0], [0, 0, 1]],
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_model_comparison_sorts_by_validation_f1_then_balanced_accuracy(tmp_path):
    _write_metrics(tmp_path / "logistic_regression_metrics.json", "logistic_regression", 0.71, 0.72)
    _write_metrics(tmp_path / "random_forest_metrics.json", "random_forest", 0.82, 0.8)
    _write_metrics(tmp_path / "gradient_boosting_metrics.json", "gradient_boosting", 0.82, 0.84)

    comparison = build_model_comparison(metrics_dir=tmp_path)

    assert comparison["model_name"].tolist() == [
        "gradient_boosting",
        "random_forest",
        "logistic_regression",
    ]
    assert comparison.iloc[0]["validation_f1_macro"] == 0.82
    assert comparison.iloc[0]["validation_balanced_accuracy"] == 0.84


def test_select_best_model_returns_first_ranked_model(tmp_path):
    _write_metrics(tmp_path / "logistic_regression_metrics.json", "logistic_regression", 0.71, 0.72)
    _write_metrics(tmp_path / "random_forest_metrics.json", "random_forest", 0.82, 0.8)

    comparison = build_model_comparison(metrics_dir=tmp_path)
    best_model = select_best_model(comparison)

    assert best_model["model_name"] == "random_forest"
