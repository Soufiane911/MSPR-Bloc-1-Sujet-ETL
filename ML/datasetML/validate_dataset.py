"""Validation helpers for the ML dataset."""

import pandas as pd

from ML.datasetML.config import REQUIRED_COLUMNS, SUBSTITUTION_CLASSES, TARGET_COLUMN


def validate_ml_dataset(df: pd.DataFrame) -> dict:
    """Return a serializable validation report for an ML dataset."""

    missing_required = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    required_columns_present = not missing_required

    missing_counts = {
        column: int(count) for column, count in df.isna().sum().to_dict().items()
    }

    if TARGET_COLUMN in df.columns:
        observed_counts = df[TARGET_COLUMN].value_counts(dropna=False).to_dict()
    else:
        observed_counts = {}

    class_distribution = {
        class_name: int(observed_counts.get(class_name, 0))
        for class_name in SUBSTITUTION_CLASSES
    }

    required_missing_count = 0
    if required_columns_present:
        required_missing_count = int(df[REQUIRED_COLUMNS].isna().sum().sum())

    is_valid = bool(
        required_columns_present
        and len(df) > 0
        and required_missing_count == 0
        and set(df[TARGET_COLUMN].dropna()).issubset(set(SUBSTITUTION_CLASSES))
    )

    return {
        "row_count": int(len(df)),
        "missing_counts": missing_counts,
        "class_distribution": class_distribution,
        "required_columns_present": required_columns_present,
        "is_valid": is_valid,
    }
