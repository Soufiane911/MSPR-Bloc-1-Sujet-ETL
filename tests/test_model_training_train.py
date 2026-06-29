import json

import pandas as pd

from ML.modelTraining.common import TARGET_COLUMN
from ML.modelTraining.train import (
    run_hyperparameter_search,
    train_final_model,
    write_final_outputs,
)


def _sample_training_frame(rows_per_class=30):
    records = []
    classes = ["faible_potentiel", "potentiel_moyen", "fort_potentiel"]
    for class_index, label in enumerate(classes):
        for row_index in range(rows_per_class):
            records.append(
                {
                    "distance_km": 250 + class_index * 240 + row_index,
                    "duration_min": 100 + class_index * 110 + row_index,
                    "avg_speed_kmh": 85 + class_index * 8,
                    "is_international": class_index > 0,
                    "train_type": "night" if class_index == 2 else "day",
                    "weekly_frequency": 2 + class_index,
                    "estimated_co2_saving_kg": 30 + class_index * 65 + row_index,
                    "origin_country": ["FR", "DE", "ES"][class_index],
                    "destination_country": ["BE", "IT", "PT"][class_index],
                    TARGET_COLUMN: label,
                }
            )
    return pd.DataFrame(records)


def test_train_final_model_and_write_outputs_create_required_artifacts(tmp_path):
    dataset = _sample_training_frame()
    comparison = pd.DataFrame(
        [
            {
                "model_name": "logistic_regression",
                "validation_f1_macro": 0.9,
                "validation_balanced_accuracy": 0.91,
                "test_f1_macro": 0.88,
            }
        ]
    )
    result = train_final_model("logistic_regression", dataset=dataset)

    model_path = tmp_path / "models" / "obrail_substitution_model.joblib"
    report_path = tmp_path / "outputs" / "classification_report.json"
    metadata_path = tmp_path / "models" / "model_metadata.json"
    figures_dir = tmp_path / "figures"

    metadata = write_final_outputs(
        result=result,
        comparison=comparison,
        model_path=model_path,
        report_path=report_path,
        metadata_path=metadata_path,
        figures_dir=figures_dir,
    )

    assert model_path.exists()
    assert report_path.exists()
    assert metadata_path.exists()
    assert (figures_dir / "model_comparison.png").exists()
    assert (figures_dir / "confusion_matrix.png").exists()
    assert (figures_dir / "feature_importance.png").exists()
    assert metadata["selected_model"] == "logistic_regression"
    assert json.loads(report_path.read_text(encoding="utf-8"))["model_name"] == (
        "logistic_regression"
    )


def test_run_hyperparameter_search_uses_stratified_grid_search_and_writes_artifact(
    tmp_path,
):
    dataset = _sample_training_frame(rows_per_class=12)
    output_path = tmp_path / "hyperparameter_search.json"
    param_grid = {
        "model__n_estimators": [5, 8],
        "model__learning_rate": [0.05],
        "model__max_depth": [1, 2],
    }

    payload = run_hyperparameter_search(
        "gradient_boosting",
        dataset=dataset,
        output_path=output_path,
        param_grid=param_grid,
        cv_splits=3,
    )

    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert output_path.exists()
    assert saved["model_name"] == "gradient_boosting"
    assert saved["method"] == "GridSearchCV"
    assert saved["scoring"] == "f1_macro"
    assert saved["cv"] == {
        "strategy": "StratifiedKFold",
        "n_splits": 3,
        "shuffle": True,
        "random_state": 42,
    }
    assert len(saved["candidates"]) == 4
    assert 0 <= saved["best_score"] <= 1
    assert saved["best_params"] == payload["best_params"]
    assert payload["estimator_params"]["n_estimators"] in {5, 8}
