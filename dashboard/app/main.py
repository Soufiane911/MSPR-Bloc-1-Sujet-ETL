"""
Dashboard Streamlit pour ObRail Europe.

Ce dashboard permet de visualiser les données ferroviaires européennes
et de contrôler la qualité des données.

Usage:
    streamlit run app/main.py
"""

import streamlit as st
from pages import overview, day_night, operators, map_page, data_quality, fair_comparison

# Configuration de la page
st.set_page_config(
    page_title="ObRail Europe - Dashboard",
    page_icon="🚂",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://back-on-track.eu',
        'Report a bug': 'https://github.com/obrail/issues',
        'About': 'Dashboard de contrôle qualité pour ObRail Europe'
    }
)

# Sidebar
st.sidebar.title("🚂 ObRail Europe")
st.sidebar.markdown("---")

# Navigation
page = st.sidebar.radio(
    "Navigation",
    ["Vue d'ensemble", "📊 Comparaison Équitable", "Jour vs Nuit", "Opérateurs", "Carte", "Qualité des données"]
)

# Affichage de la page sélectionnée
if page == "Vue d'ensemble":
    overview.show()
elif page == "📊 Comparaison Équitable":
    fair_comparison.show()
elif page == "Jour vs Nuit":
    day_night.show()
elif page == "Opérateurs":
    operators.show()
elif page == "Carte":
    map_page.show()
elif page == "Qualité des données":
    data_quality.show()

# Footer
st.sidebar.markdown("---")
st.sidebar.info("""
© 2025 ObRail Europe

Dashboard de contrôle qualité des données ferroviaires européennes.

**Sources:**
- Back-on-Track Night Train Database
- SNCF, Deutsche Bahn, ÖBB, Renfe, Trenitalia
- Mobility Database Catalogs
""")
