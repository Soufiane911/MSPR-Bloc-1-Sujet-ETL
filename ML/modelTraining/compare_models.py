"""Aggregate model metrics and select the final ObRail classifier."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from ML.modelTraining.common import OUTPUT_DIR


METRIC_FILES = [
    "logistic_regression_metrics.json",
    "random_forest_metrics.json",
    "gradient_boosting_metrics.json",
    "mlp_metrics.json",
]
DEFAULT_COMPARISON_PATH = OUTPUT_DIR / "model_comparison.csv"


def load_metrics_file(metrics_path: Path) -> dict[str, Any]:
    """Load one model metrics JSON file."""

    return json.loads(metrics_path.read_text(encoding="utf-8"))


def build_model_comparison(metrics_dir: Path = OUTPUT_DIR) -> pd.DataFrame:
    """Build a sorted comparison table from per-model metrics JSON files."""

    rows = []
    for metrics_file in METRIC_FILES:
        metrics_path = metrics_dir / metrics_file
        if not metrics_path.exists():
            continue
        payload = load_metrics_file(metrics_path)
        validation = payload["validation"]
        test = payload["test"]
        rows.append(
            {
                "model_name": payload["model_name"],
                "validation_accuracy": validation["accuracy"],
                "validation_balanced_accuracy": validation["balanced_accuracy"],
                "validation_precision_macro": validation["precision_macro"],
                "validation_recall_macro": validation["recall_macro"],
                "validation_f1_macro": validation["f1_macro"],
                "validation_fort_potentiel_f1": _class_f1(
                    validation["classification_report"], "fort_potentiel"
                ),
                "test_accuracy": test["accuracy"],
                "test_balanced_accuracy": test["balanced_accuracy"],
                "test_precision_macro": test["precision_macro"],
                "test_recall_macro": test["recall_macro"],
                "test_f1_macro": test["f1_macro"],
                "test_fort_potentiel_f1": _class_f1(
                    test["classification_report"], "fort_potentiel"
                ),
                "primary_metric": payload.get("primary_metric", "f1_macro"),
                "secondary_metric": payload.get("secondary_metric", "balanced_accuracy"),
            }
        )

    if not rows:
        return pd.DataFrame()

    comparison = pd.DataFrame(rows)
    return comparison.sort_values(
        [
            "validation_f1_macro",
            "validation_balanced_accuracy",
            "validation_fort_potentiel_f1",
            "test_f1_macro",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def select_best_model(comparison: pd.DataFrame) -> dict[str, Any]:
    """Select the best model according to the sorted comparison table."""

    if comparison.empty:
        raise ValueError("Cannot select a model from an empty comparison table")
    return comparison.iloc[0].to_dict()


def write_model_comparison(
    comparison: pd.DataFrame,
    output_path: Path = DEFAULT_COMPARISON_PATH,
) -> None:
    """Write the comparison table to CSV."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(output_path, index=False)


def _class_f1(classification_report_payload: dict[str, Any], class_name: str) -> float:
    class_metrics = classification_report_payload.get(class_name, {})
    return float(class_metrics.get("f1-score", 0.0))


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare ObRail model metrics")
    parser.add_argument("--metrics-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_COMPARISON_PATH)
    args = parser.parse_args()

    comparison = build_model_comparison(args.metrics_dir)
    write_model_comparison(comparison, args.output)
    if comparison.empty:
        print("No model metrics found")
        return 1

    best = select_best_model(comparison)
    print(f"Best model: {best['model_name']}")
    print(f"Validation f1_macro: {best['validation_f1_macro']}")
    print(f"Output: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
