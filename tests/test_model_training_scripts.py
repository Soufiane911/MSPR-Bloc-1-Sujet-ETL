import importlib

import pandas as pd

from ML.modelTraining.common import TARGET_COLUMN


MODEL_MODULES = [
    "ML.modelTraining.train_logistic_regression",
    "ML.modelTraining.train_random_forest",
    "ML.modelTraining.train_gradient_boosting",
    "ML.modelTraining.train_mlp",
]


def _sample_training_frame(rows_per_class=30):
    records = []
    classes = ["faible_potentiel", "potentiel_moyen", "fort_potentiel"]
    for class_index, label in enumerate(classes):
        for row_index in range(rows_per_class):
            records.append(
                {
                    "distance_km": 250 + class_index * 220 + row_index,
                    "duration_min": 90 + class_index * 120 + row_index,
                    "avg_speed_kmh": 75 + class_index * 15,
                    "is_international": class_index > 0,
                    "train_type": "night" if class_index == 2 else "day",
                    "weekly_frequency": 2 + class_index,
                    "estimated_co2_saving_kg": 25 + class_index * 60 + row_index,
                    "origin_country": ["FR", "DE", "ES"][class_index],
                    "destination_country": ["BE", "IT", "PT"][class_index],
                    TARGET_COLUMN: label,
                }
            )
    return pd.DataFrame(records)


def test_model_training_scripts_share_same_public_contract(tmp_path):
    dataset = _sample_training_frame()

    for module_name in MODEL_MODULES:
        module = importlib.import_module(module_name)
        output_path = tmp_path / f"{module.MODEL_NAME}_metrics.json"

        payload = module.train_and_evaluate(dataset=dataset, output_path=output_path)

        assert output_path.exists()
        assert payload["model_name"] == module.MODEL_NAME
        assert payload["primary_metric"] == "f1_macro"
        assert isinstance(payload["hyperparameters"], dict)
        for split_name in ["validation", "test"]:
            split_metrics = payload[split_name]
            assert set(split_metrics).issuperset(
                {
                    "accuracy",
                    "balanced_accuracy",
                    "precision_macro",
                    "recall_macro",
                    "f1_macro",
                    "classification_report",
                    "confusion_matrix",
                }
            )
            assert 0 <= split_metrics["f1_macro"] <= 1
