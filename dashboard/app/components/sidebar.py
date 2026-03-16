import streamlit as st
import pandas as pd
from typing import Optional


def render_sidebar(df: pd.DataFrame) -> dict:
    st.sidebar.title("ObRail Europe")
    st.sidebar.markdown("---")

    st.sidebar.markdown("### Filtres")

    countries = ["Tous"]
    if not df.empty and "operator_country" in df.columns:
        countries += sorted(df["operator_country"].dropna().unique().tolist())

    selected_country = st.sidebar.selectbox(
        "Pays", countries, help="Filtrer par pays d'opérateur"
    )

    train_types = ["Tous", "Jour", "Nuit"]
    selected_type = st.sidebar.selectbox(
        "Type de train", train_types, help="Filtrer par type de train"
    )

    min_distance = 0
    max_distance = 2000
    if not df.empty and "distance_km" in df.columns:
        max_dist_val = df["distance_km"].max()
        if pd.notna(max_dist_val):
            max_distance = int(max_dist_val)

    distance_range = st.sidebar.slider(
        "Distance (km)",
        min_value=0,
        max_value=max_distance,
        value=(0, max_distance),
        help="Filtrer par distance du trajet",
    )

    st.sidebar.markdown("---")

    st.sidebar.info("""
    **Tableau de bord analytique**
    
    Données ferroviaires européennes.
    
    **Sources:**
    - Back-on-Track Night Train Database
    - SNCF, DB, Trenitalia, CFF/SBB, SNCB
    
    © 2025 ObRail Europe
    """)

    return {
        "country": selected_country,
        "train_type": selected_type,
        "distance_min": distance_range[0],
        "distance_max": distance_range[1],
    }


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()

    if filters["country"] != "Tous" and "operator_country" in result.columns:
        result = result[result["operator_country"] == filters["country"]]

    if filters["train_type"] != "Tous" and "train_type_normalized" in result.columns:
        type_map = {"Jour": "day", "Nuit": "night"}
        result = result[
            result["train_type_normalized"] == type_map[filters["train_type"]]
        ]

    if "distance_km" in result.columns:
        # Conserver les lignes avec distance_km null OU dans la plage
        result = result[
            (result["distance_km"].isna()) | 
            ((result["distance_km"] >= filters["distance_min"]) & (result["distance_km"] <= filters["distance_max"]))
        ]

    return result
