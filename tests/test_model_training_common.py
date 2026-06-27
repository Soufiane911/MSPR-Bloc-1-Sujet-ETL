import pandas as pd
from sklearn.dummy import DummyClassifier

from ML.modelTraining.common import (
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    TARGET_COLUMN,
    build_model_pipeline,
    evaluate_classifier,
    split_training_data,
)


def _sample_training_frame(rows_per_class=20):
    records = []
    classes = ["faible_potentiel", "potentiel_moyen", "fort_potentiel"]
    for class_index, label in enumerate(classes):
        for row_index in range(rows_per_class):
            records.append(
                {
                    "distance_km": 300 + class_index * 150 + row_index,
                    "duration_min": 120 + class_index * 60 + row_index,
                    "avg_speed_kmh": 70 + class_index * 10,
                    "is_international": class_index % 2 == 0,
                    "train_type": "night" if class_index == 2 else "day",
                    "weekly_frequency": 3 + class_index,
                    "estimated_co2_saving_kg": 40 + class_index * 30 + row_index,
                    "origin_country": ["FR", "DE", "ES"][class_index],
                    "destination_country": ["DE", "ES", "FR"][class_index],
                    TARGET_COLUMN: label,
                }
            )
    return pd.DataFrame(records)


def test_split_training_data_uses_fixed_70_15_15_stratified_split():
    df = _sample_training_frame(rows_per_class=40)

    splits = split_training_data(df)

    assert len(splits.X_train) == 84
    assert len(splits.X_validation) == 18
    assert len(splits.X_test) == 18
    assert set(splits.X_train.columns) == set(NUMERIC_FEATURES + CATEGORICAL_FEATURES)
    assert splits.y_train.value_counts().to_dict() == {
        "faible_potentiel": 28,
        "potentiel_moyen": 28,
        "fort_potentiel": 28,
    }
    assert splits.y_validation.value_counts().to_dict() == {
        "faible_potentiel": 6,
        "potentiel_moyen": 6,
        "fort_potentiel": 6,
    }
    assert splits.y_test.value_counts().to_dict() == {
        "faible_potentiel": 6,
        "potentiel_moyen": 6,
        "fort_potentiel": 6,
    }


def test_build_model_pipeline_fits_shared_preprocessing_and_classifier():
    df = _sample_training_frame(rows_per_class=20)
    splits = split_training_data(df)
    pipeline = build_model_pipeline(DummyClassifier(strategy="most_frequent"))

    pipeline.fit(splits.X_train, splits.y_train)
    predictions = pipeline.predict(splits.X_validation)

    assert len(predictions) == len(splits.y_validation)


def test_evaluate_classifier_returns_macro_metrics_report_and_confusion_matrix():
    df = _sample_training_frame(rows_per_class=20)
    splits = split_training_data(df)
    pipeline = build_model_pipeline(DummyClassifier(strategy="most_frequent"))
    pipeline.fit(splits.X_train, splits.y_train)

    metrics = evaluate_classifier(pipeline, splits.X_validation, splits.y_validation)

    assert set(metrics).issuperset(
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
    assert 0 <= metrics["f1_macro"] <= 1
    assert len(metrics["confusion_matrix"]) == 3
