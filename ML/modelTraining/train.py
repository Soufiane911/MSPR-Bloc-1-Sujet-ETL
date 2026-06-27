"""Orchestrate ObRail model training, comparison, and artifact export."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any

import joblib
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from ML.modelTraining.common import (
    DATASET_PATH,
    FEATURE_COLUMNS,
    FIGURES_DIR,
    MODELS_DIR,
    OUTPUT_DIR,
    RANDOM_STATE,
    TARGET_COLUMN,
    build_model_pipeline,
    ensure_output_dirs,
    evaluate_classifier,
    load_training_dataset,
    split_training_data,
)
from ML.modelTraining.compare_models import (
    build_model_comparison,
    select_best_model,
    write_model_comparison,
)


MODEL_MODULES = {
    "logistic_regression": "ML.modelTraining.train_logistic_regression",
    "random_forest": "ML.modelTraining.train_random_forest",
    "gradient_boosting": "ML.modelTraining.train_gradient_boosting",
    "mlp": "ML.modelTraining.train_mlp",
}

DEFAULT_MODEL_PATH = MODELS_DIR / "obrail_substitution_model.joblib"
DEFAULT_METADATA_PATH = MODELS_DIR / "model_metadata.json"
DEFAULT_REPORT_PATH = OUTPUT_DIR / "classification_report.json"
DEFAULT_COMPARISON_PATH = OUTPUT_DIR / "model_comparison.csv"


@dataclass
class FinalModelResult:
    """Final model and evaluation details."""

    model_name: str
    model: Any
    metrics: dict[str, Any]
    training_rows: int
    validation_rows: int
    test_rows: int
    final_training_rows: int


def run_model_experiments(dataset: pd.DataFrame | None = None) -> list[dict[str, Any]]:
    """Train and evaluate all candidate models using the shared split."""

    ensure_output_dirs()
    payloads = []
    for model_name in MODEL_MODULES:
        module = _load_model_module(model_name)
        payloads.append(module.train_and_evaluate(dataset=dataset))
    return payloads


def train_final_model(
    model_name: str,
    dataset: pd.DataFrame | None = None,
) -> FinalModelResult:
    """Train the selected model on train+validation and evaluate on test."""

    training_dataset = load_training_dataset() if dataset is None else dataset
    splits = split_training_data(training_dataset)
    module = _load_model_module(model_name)
    estimator = module.build_estimator()
    model = build_model_pipeline(estimator)

    X_final_train = pd.concat(
        [splits.X_train, splits.X_validation],
        ignore_index=True,
    )
    y_final_train = pd.concat(
        [splits.y_train, splits.y_validation],
        ignore_index=True,
    )
    model.fit(X_final_train, y_final_train)
    metrics = evaluate_classifier(model, splits.X_test, splits.y_test)

    return FinalModelResult(
        model_name=model_name,
        model=model,
        metrics=metrics,
        training_rows=len(splits.X_train),
        validation_rows=len(splits.X_validation),
        test_rows=len(splits.X_test),
        final_training_rows=len(X_final_train),
    )


def write_final_outputs(
    result: FinalModelResult,
    comparison: pd.DataFrame,
    model_path: Path = DEFAULT_MODEL_PATH,
    report_path: Path = DEFAULT_REPORT_PATH,
    metadata_path: Path = DEFAULT_METADATA_PATH,
    figures_dir: Path = FIGURES_DIR,
) -> dict[str, Any]:
    """Write the final model, metadata, report, and figures."""

    model_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(result.model, model_path)

    report_payload = {
        "model_name": result.model_name,
        "test_metrics": result.metrics,
        "classification_report": result.metrics["classification_report"],
        "confusion_matrix": result.metrics["confusion_matrix"],
    }
    report_path.write_text(
        json.dumps(report_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    metadata = _build_metadata(result, model_path, report_path, figures_dir)
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    plot_model_comparison(comparison, figures_dir / "model_comparison.png")
    plot_confusion_matrix(
        result.metrics["confusion_matrix"],
        figures_dir / "confusion_matrix.png",
    )
    plot_feature_importance(result.model, figures_dir / "feature_importance.png")

    return metadata


def run_training_pipeline(dataset: pd.DataFrame | None = None) -> dict[str, Any]:
    """Run the complete training workflow and return final metadata."""

    ensure_output_dirs()
    training_dataset = load_training_dataset() if dataset is None else dataset
    run_model_experiments(training_dataset)
    comparison = build_model_comparison(OUTPUT_DIR)
    write_model_comparison(comparison, DEFAULT_COMPARISON_PATH)
    best_model = select_best_model(comparison)
    final_result = train_final_model(best_model["model_name"], training_dataset)
    metadata = write_final_outputs(final_result, comparison)
    metadata["selection_metrics"] = best_model
    DEFAULT_METADATA_PATH.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return metadata


def plot_model_comparison(comparison: pd.DataFrame, output_path: Path) -> None:
    """Plot validation f1_macro and balanced_accuracy for each model."""

    fig, ax = plt.subplots(figsize=(9, 5))
    if comparison.empty:
        ax.text(0.5, 0.5, "Aucune metrique disponible", ha="center", va="center")
        ax.axis("off")
    else:
        ordered = comparison.sort_values("validation_f1_macro", ascending=True)
        y = np.arange(len(ordered))
        ax.barh(y - 0.18, ordered["validation_f1_macro"], height=0.35, label="F1 macro")
        ax.barh(
            y + 0.18,
            ordered["validation_balanced_accuracy"],
            height=0.35,
            label="Balanced accuracy",
        )
        ax.set_yticks(y, ordered["model_name"])
        ax.set_xlim(0, 1.05)
        ax.set_xlabel("Score validation")
        ax.set_title("Comparaison des modeles")
        ax.legend(loc="lower right")
        ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def plot_confusion_matrix(confusion_values: list[list[int]], output_path: Path) -> None:
    """Plot the final model confusion matrix."""

    labels = ["faible", "moyen", "fort"]
    matrix = np.array(confusion_values)
    fig, ax = plt.subplots(figsize=(6, 5))
    image = ax.imshow(matrix, cmap="Blues")
    ax.set_xticks(np.arange(len(labels)), labels)
    ax.set_yticks(np.arange(len(labels)), labels)
    ax.set_xlabel("Prediction")
    ax.set_ylabel("Classe reelle")
    ax.set_title("Matrice de confusion")
    for row in range(matrix.shape[0]):
        for col in range(matrix.shape[1]):
            ax.text(col, row, int(matrix[row, col]), ha="center", va="center")
    fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def plot_feature_importance(model: Any, output_path: Path, top_n: int = 20) -> None:
    """Plot feature importance when available on the final estimator."""

    feature_names = _get_feature_names(model)
    importances = _get_feature_importances(model)

    fig, ax = plt.subplots(figsize=(9, 6))
    if importances is None or len(importances) != len(feature_names):
        ax.text(0.5, 0.5, "Importance des variables non disponible", ha="center")
        ax.axis("off")
    else:
        order = np.argsort(importances)[-top_n:]
        ax.barh(np.array(feature_names)[order], importances[order])
        ax.set_xlabel("Importance")
        ax.set_title("Variables les plus importantes")
        ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def _load_model_module(model_name: str):
    if model_name not in MODEL_MODULES:
        raise ValueError(f"Unknown model name: {model_name}")
    return import_module(MODEL_MODULES[model_name])


def _build_metadata(
    result: FinalModelResult,
    model_path: Path,
    report_path: Path,
    figures_dir: Path,
) -> dict[str, Any]:
    return {
        "selected_model": result.model_name,
        "selection_rule": "highest validation f1_macro, then balanced_accuracy",
        "primary_metric": "f1_macro",
        "secondary_metric": "balanced_accuracy",
        "target_column": TARGET_COLUMN,
        "feature_columns": FEATURE_COLUMNS,
        "dataset_path": str(DATASET_PATH),
        "random_state": RANDOM_STATE,
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "training_rows_70_percent": result.training_rows,
        "validation_rows_15_percent": result.validation_rows,
        "test_rows_15_percent": result.test_rows,
        "final_training_rows_85_percent": result.final_training_rows,
        "test_metrics": {
            key: value
            for key, value in result.metrics.items()
            if key not in {"classification_report", "confusion_matrix"}
        },
        "model_path": str(model_path),
        "classification_report_path": str(report_path),
        "figures": {
            "model_comparison": str(figures_dir / "model_comparison.png"),
            "confusion_matrix": str(figures_dir / "confusion_matrix.png"),
            "feature_importance": str(figures_dir / "feature_importance.png"),
        },
    }


def _get_feature_names(model: Any) -> list[str]:
    preprocessor = model.named_steps["preprocessor"]
    try:
        return [str(name) for name in preprocessor.get_feature_names_out()]
    except Exception:
        return FEATURE_COLUMNS


def _get_feature_importances(model: Any) -> np.ndarray | None:
    estimator = model.named_steps["model"]
    if hasattr(estimator, "feature_importances_"):
        return np.asarray(estimator.feature_importances_)
    if hasattr(estimator, "coef_"):
        coefficients = np.asarray(estimator.coef_)
        if coefficients.ndim == 1:
            return np.abs(coefficients)
        return np.mean(np.abs(coefficients), axis=0)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Train ObRail substitution model")
    parser.add_argument("--dataset", type=Path, default=DATASET_PATH)
    args = parser.parse_args()

    dataset = load_training_dataset(args.dataset)
    metadata = run_training_pipeline(dataset)
    print(f"Selected model: {metadata['selected_model']}")
    print(f"Model: {metadata['model_path']}")
    print(f"Report: {metadata['classification_report_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
