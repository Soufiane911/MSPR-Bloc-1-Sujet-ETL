import streamlit as st
import pandas as pd
from analytics.shared_metrics import get_overview_kpis


def render_kpi_bar(df: pd.DataFrame) -> None:
    if df.empty:
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Trains", "0")
        with col2:
            st.metric("Gares", "0")
        with col3:
            st.metric("Opérateurs", "0")
        with col4:
            st.metric("Dessertes", "0")
        with col5:
            st.metric("CO2 économisé", "0 kg")
        return

    metrics = get_overview_kpis(df)

    total_co2 = 0
    if "co2_saving_kg" in df.columns:
        total_co2 = df["co2_saving_kg"].sum()

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Trains", f"{metrics['total_trains']:,}")
    with col2:
        st.metric("Gares", f"{metrics['total_stations']:,}")
    with col3:
        st.metric("Opérateurs", f"{metrics['total_operators']:,}")
    with col4:
        st.metric("Dessertes", f"{metrics['total_schedules']:,}")
    with col5:
        if total_co2 >= 1000000:
            st.metric("CO2 économisé", f"{total_co2 / 1000000:.1f} t")
        elif total_co2 >= 1000:
            st.metric("CO2 économisé", f"{total_co2 / 1000:.0f} kg")
        else:
            st.metric("CO2 économisé", f"{total_co2:.0f} kg")

    st.markdown("---")
