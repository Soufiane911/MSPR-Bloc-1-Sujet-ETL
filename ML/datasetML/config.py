"""Configuration constants for ML dataset preparation."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
ML_DATA_DIR = DATA_DIR / "ml"
ML_DATASET_PATH = ML_DATA_DIR / "obrail_ml_dataset.csv"

DATASETML_DIR = PROJECT_ROOT / "ML" / "datasetML"
OUTPUTS_DIR = DATASETML_DIR / "outputs"
LOGS_DIR = DATASETML_DIR / "logs"
SQL_DIR = DATASETML_DIR / "sql"
SQL_QUERY_PATH = SQL_DIR / "extract_obrail_routes.sql"
RAW_EXTRACT_PATH = OUTPUTS_DIR / "raw_obrail_extract.csv"
PREPARED_DATASET_PATH = OUTPUTS_DIR / "prepared_obrail_dataset.csv"
QUALITY_REPORT_PATH = OUTPUTS_DIR / "data_quality_report.json"
CLASS_DISTRIBUTION_PATH = OUTPUTS_DIR / "class_distribution.csv"

TARGET_COLUMN = "substitution_potential"

FEATURE_COLUMNS = [
    "distance_km",
    "duration_min",
    "duration_minutes",
    "duration_hours",
    "avg_speed_kmh",
    "is_international",
    "train_type",
    "estimated_co2_saving_kg",
    "weekly_frequency",
    "origin_country",
    "destination_country",
]

IDENTIFIER_COLUMNS = [
    "route_id",
    "schedule_id",
]

REQUIRED_COLUMNS = [
    "distance_km",
    "duration_min",
    "duration_minutes",
    "duration_hours",
    "avg_speed_kmh",
    "is_international",
    "train_type",
    "estimated_co2_saving_kg",
    "weekly_frequency",
    "origin_country",
    "destination_country",
    TARGET_COLUMN,
]

SUBSTITUTION_CLASSES = [
    "faible_potentiel",
    "potentiel_moyen",
    "fort_potentiel",
]

MIN_REALISTIC_SPEED_KMH = 5
MAX_REALISTIC_SPEED_KMH = 350
DEFAULT_CO2_KG_PER_KM = 0.15

SOURCE_COUNTRY_MAP = {
    "sncf_intercites": "FR",
    "sncf_tgv": "FR",
    "db_fernverkehr": "DE",
    "renfe": "ES",
    "trenitalia": "IT",
    "cff_sbb": "CH",
    "obb": "AT",
    "sncb": "BE",
}

RAIL_ROUTE_TYPES = {
    2,
    100,
    101,
    102,
    103,
    104,
    105,
    106,
    107,
    108,
    109,
    110,
    111,
    112,
    113,
    114,
    115,
    116,
    117,
}
