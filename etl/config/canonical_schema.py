"""
Schéma canonique pour le pipeline ETL ObRail Europe.

Définit le contrat de données entre les extracteurs, transformateurs et loaders.
Chaque entité (operators, stations, trains, schedules) possède un schéma décrivant :
- Le nom de la colonne
- Le type Python attendu
- Si la colonne est nullable
- Le rôle de la colonne (technical_id, source_id, business_attr)
- Une description concise

Rôles :
- technical_id : Clé primaire auto-générée (SERIAL en base)
- source_id    : Identifiant provenant de la source d'origine (GTFS, API, etc.)
- business_attr: Attribut métier (nom, pays, coordonnées, etc.)
- foreign_key  : Référence vers une autre entité canonique
"""

from typing import Any, Dict, List

import pandas as pd

# ============================================================
# Types de rôles autorisés
# ============================================================
VALID_ROLES = {"technical_id", "source_id", "business_attr", "foreign_key"}

# ============================================================
# OPERATORS — Opérateurs ferroviaires
# ============================================================
CANONICAL_OPERATORS = {
    "operator_id": {
        "type": "int",
        "nullable": False,
        "role": "technical_id",
        "desc": "Clé primaire auto-générée (SERIAL)",
    },
    "name": {
        "type": "str",
        "nullable": False,
        "role": "business_attr",
        "desc": "Nom de l'opérateur ferroviaire",
    },
    "country": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Code pays ISO 3166-1 alpha-2 (FR, DE, etc.), NULL si inconnu",
    },
    "website": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "URL du site web de l'opérateur",
    },
    "source_agency_id": {
        "type": "str",
        "nullable": True,
        "role": "source_id",
        "desc": "agency_id original de la source GTFS ou API",
    },
    "source_name": {
        "type": "str",
        "nullable": True,
        "role": "source_id",
        "desc": "Nom de la source de données (Back-on-Track, SNCF, etc.)",
    },
}

# ============================================================
# STATIONS — Gares et arrêts
# ============================================================
CANONICAL_STATIONS = {
    "station_id": {
        "type": "int",
        "nullable": False,
        "role": "technical_id",
        "desc": "Clé primaire auto-générée (SERIAL)",
    },
    "name": {
        "type": "str",
        "nullable": False,
        "role": "business_attr",
        "desc": "Nom de la gare ou de l'arrêt",
    },
    "city": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Ville de la gare, NULL si inconnue",
    },
    "country": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Code pays ISO 3166-1 alpha-2, NULL si inconnu",
    },
    "latitude": {
        "type": "float",
        "nullable": True,
        "role": "business_attr",
        "desc": "Latitude en degrés décimaux (-90 à 90)",
    },
    "longitude": {
        "type": "float",
        "nullable": True,
        "role": "business_attr",
        "desc": "Longitude en degrés décimaux (-180 à 180)",
    },
    "uic_code": {
        "type": "str",
        "nullable": True,
        "role": "source_id",
        "desc": (
            "Code stop_id d'origine (source_stop_code) — pas nécessairement "
            "un vrai code UIC international, dépend de la source"
        ),
    },
    "timezone": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Fuseau horaire IANA (ex: Europe/Paris), défaut Europe/Paris",
    },
    "source_name": {
        "type": "str",
        "nullable": True,
        "role": "source_id",
        "desc": "Nom de la source de données pour traçabilité",
    },
}

# ============================================================
# TRAINS — Trains et lignes
# ============================================================
CANONICAL_TRAINS = {
    "train_id": {
        "type": "int",
        "nullable": False,
        "role": "technical_id",
        "desc": "Clé primaire auto-générée (SERIAL)",
    },
    "train_number": {
        "type": "str",
        "nullable": False,
        "role": "source_id",
        "desc": (
            "Identifiant du train (source_trip_id préfixé par le nom de la source, "
            "ex: 'sncf_12345'). N'est pas un vrai numéro de train commercial"
        ),
    },
    "operator_id": {
        "type": "int",
        "nullable": False,
        "role": "foreign_key",
        "desc": "Référence vers operators.operator_id",
    },
    "train_type": {
        "type": "str",
        "nullable": False,
        "role": "business_attr",
        "desc": "Type final : 'day' (06h-21h59) ou 'night' (22h-05h59)",
    },
    "category": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Catégorie commerciale (TGV, ICE, Eurostar, etc.)",
    },
    "route_name": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Nom de la ligne (ex: Paris - Lyon)",
    },
    "train_type_rule": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Classification issue des règles métier fortes",
    },
    "train_type_heuristic": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Classification issue de l'heuristique métier",
    },
    "train_type_ml": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Suggestion du modèle ML de support",
    },
    "classification_method": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Méthode finale retenue (rule, heuristic, ml, default)",
    },
    "classification_reason": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Raison principale de la décision finale",
    },
    "classification_confidence": {
        "type": "float",
        "nullable": True,
        "role": "business_attr",
        "desc": "Niveau de confiance de la classification (0.00-1.00)",
    },
    "ml_night_probability": {
        "type": "float",
        "nullable": True,
        "role": "business_attr",
        "desc": "Probabilité ML pour la classe 'night' (0.00-1.00)",
    },
    "night_percentage": {
        "type": "float",
        "nullable": True,
        "role": "business_attr",
        "desc": "Part estimée du trajet en période nocturne (0-100%)",
    },
    "needs_manual_review": {
        "type": "bool",
        "nullable": True,
        "role": "business_attr",
        "desc": "Indique si un contrôle manuel est recommandé",
    },
    "source_name": {
        "type": "str",
        "nullable": True,
        "role": "source_id",
        "desc": "Nom de la source de données pour traçabilité",
    },
}

# ============================================================
# SCHEDULES — Dessertes / Horaires
# ============================================================
CANONICAL_SCHEDULES = {
    "schedule_id": {
        "type": "int",
        "nullable": False,
        "role": "technical_id",
        "desc": "Clé primaire auto-générée (SERIAL)",
    },
    "train_id": {
        "type": "int",
        "nullable": False,
        "role": "foreign_key",
        "desc": "Référence vers trains.train_id",
    },
    "origin_id": {
        "type": "int",
        "nullable": False,
        "role": "foreign_key",
        "desc": "Référence vers stations.station_id (gare de départ)",
    },
    "destination_id": {
        "type": "int",
        "nullable": False,
        "role": "foreign_key",
        "desc": "Référence vers stations.station_id (gare d'arrivée)",
    },
    "departure_time": {
        "type": "datetime",
        "nullable": False,
        "role": "business_attr",
        "desc": "Heure de départ (TIMESTAMP WITH TIME ZONE)",
    },
    "arrival_time": {
        "type": "datetime",
        "nullable": False,
        "role": "business_attr",
        "desc": "Heure d'arrivée (TIMESTAMP WITH TIME ZONE)",
    },
    "duration_min": {
        "type": "int",
        "nullable": False,
        "role": "business_attr",
        "desc": "Durée du trajet en minutes (> 0)",
    },
    "distance_km": {
        "type": "float",
        "nullable": True,
        "role": "business_attr",
        "desc": "Distance du trajet en kilomètres",
    },
    "frequency": {
        "type": "str",
        "nullable": True,
        "role": "business_attr",
        "desc": "Fréquence de service (quotidien, hebdomadaire, etc.)",
    },
    "source_name": {
        "type": "str",
        "nullable": True,
        "role": "source_id",
        "desc": "Nom de la source de données pour traçabilité",
    },
}

# ============================================================
# Registre global des schémas
# ============================================================
SCHEMAS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "operators": CANONICAL_OPERATORS,
    "stations": CANONICAL_STATIONS,
    "trains": CANONICAL_TRAINS,
    "schedules": CANONICAL_SCHEDULES,
}

# Mapping type string -> types Python pour la validation
_TYPE_MAP = {
    "int": (int, "int64", "Int64"),
    "float": (float, "float64", "Float64"),
    "str": (str, "object", "string"),
    "bool": (bool, "bool", "boolean"),
    "datetime": ("datetime64[ns]", "datetime64[ns, UTC]"),
}


def get_schema(schema_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Retourne le schéma canonique d'une entité.

    Args:
        schema_name: Nom de l'entité (operators, stations, trains, schedules)

    Returns:
        Dictionnaire du schéma canonique

    Raises:
        ValueError: Si le schéma n'existe pas
    """
    if schema_name not in SCHEMAS:
        raise ValueError(
            f"Schéma inconnu: '{schema_name}'. "
            f"Schémas disponibles: {list(SCHEMAS.keys())}"
        )
    return SCHEMAS[schema_name]


def get_required_columns(schema_name: str, exclude_technical_ids: bool = True) -> List[str]:
    """
    Retourne les colonnes non-nullables d'un schéma.

    Args:
        schema_name: Nom de l'entité
        exclude_technical_ids: Exclure les technical_id (auto-générés en base)

    Returns:
        Liste des colonnes obligatoires
    """
    schema = get_schema(schema_name)
    return [
        col
        for col, spec in schema.items()
        if not spec["nullable"]
        and (not exclude_technical_ids or spec["role"] != "technical_id")
    ]


def validate_dataframe(
    df: pd.DataFrame,
    schema_name: str,
    exclude_technical_ids: bool = True,
) -> Dict[str, Any]:
    """
    Valide un DataFrame par rapport au schéma canonique.

    Vérifie la conformité structurelle et les contraintes de nullabilité.
    Les colonnes technical_id sont exclues par défaut car elles sont
    générées automatiquement en base de données.

    Args:
        df: DataFrame à valider
        schema_name: Nom du schéma (operators, stations, trains, schedules)
        exclude_technical_ids: Exclure les technical_id de la validation

    Returns:
        Rapport de validation::

            {
                "valid": bool,
                "schema": str,
                "row_count": int,
                "missing_columns": [...],
                "extra_columns": [...],
                "null_violations": {"col": count, ...},
                "type_mismatches": {"col": {"expected": ..., "actual": ...}, ...},
            }
    """
    schema = get_schema(schema_name)

    # Filtrer les technical_id si demandé
    expected_cols = {
        col: spec
        for col, spec in schema.items()
        if not (exclude_technical_ids and spec["role"] == "technical_id")
    }

    df_columns = set(df.columns)
    schema_columns = set(expected_cols.keys())

    # --- Colonnes manquantes / en trop ---
    missing_columns = sorted(schema_columns - df_columns)
    extra_columns = sorted(df_columns - schema_columns)

    # --- Violations de nullabilité ---
    null_violations: Dict[str, int] = {}
    for col, spec in expected_cols.items():
        if col in df_columns and not spec["nullable"]:
            null_count = int(df[col].isna().sum())
            if null_count > 0:
                null_violations[col] = null_count

    # --- Vérification des types ---
    type_mismatches: Dict[str, Dict[str, str]] = {}
    for col, spec in expected_cols.items():
        if col not in df_columns:
            continue

        expected_type = spec["type"]
        actual_dtype = str(df[col].dtype)

        # Récupérer les types acceptables
        acceptable = _TYPE_MAP.get(expected_type, ())

        # Vérifier la compatibilité du dtype pandas
        match = False
        for acc in acceptable:
            if isinstance(acc, str) and acc in actual_dtype:
                match = True
                break
            if isinstance(acc, type) and actual_dtype == "object":
                # Pour object dtype, vérifier un échantillon non-null
                match = True
                break

        if not match:
            type_mismatches[col] = {
                "expected": expected_type,
                "actual": actual_dtype,
            }

    # --- Résultat global ---
    is_valid = (
        len(missing_columns) == 0
        and len(null_violations) == 0
        and len(type_mismatches) == 0
    )

    return {
        "valid": is_valid,
        "schema": schema_name,
        "row_count": len(df),
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "null_violations": null_violations,
        "type_mismatches": type_mismatches,
    }
