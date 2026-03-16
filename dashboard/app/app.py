"""Tableau de bord Streamlit OBRail Europe - Version consolidée.

Usage:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from analytics.analytics_loader import load_analytics_data, get_data_source_label
from components.sidebar import render_sidebar, apply_filters
from components.kpi_bar import render_kpi_bar
from components.tab_overview import render_tab_overview
from components.tab_day_night import render_tab_day_night
from components.tab_network import render_tab_network
from components.tab_map import render_tab_map
from components.footer import render_footer

st.set_page_config(
    page_title="ObRail Europe - Dashboard",
    page_icon="🚂",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://back-on-track.eu",
        "Report a bug": "https://github.com/obrail/issues",
        "About": "Tableau de bord analytique OBRail Europe",
    },
)


@st.cache_data(ttl=300)
def get_data():
    return load_analytics_data()


def main():
    st.title("ObRail Europe")
    st.markdown("Tableau de bord analytique des données ferroviaires européennes")

    df_full = get_data()

    filters = render_sidebar(df_full)

    df = apply_filters(df_full, filters)

    render_kpi_bar(df)

    tab1, tab2, tab3, tab4 = st.tabs(
        ["Vue d'ensemble", "Comparaison Jour/Nuit", "Réseau & Distance", "Carte"]
    )

    with tab1:
        render_tab_overview(df)

    with tab2:
        render_tab_day_night(df)

    with tab3:
        render_tab_network(df)

    with tab4:
        render_tab_map(df)

    render_footer(df)


if __name__ == "__main__":
    main()
