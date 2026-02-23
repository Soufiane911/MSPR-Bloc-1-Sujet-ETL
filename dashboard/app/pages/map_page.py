"""
Page de carte du dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from database import get_connection


def show():
    """Affiche la page de carte."""
    
    st.title("🗺️ Carte des Gares")
    st.markdown("Visualisation géographique des gares ferroviaires européennes")
    
    conn = get_connection()
    
    # Filtres
    st.subheader("🔍 Filtres")
    
    col1, col2 = st.columns(2)
    
    with col1:
        countries = pd.read_sql(
            "SELECT DISTINCT country FROM stations WHERE latitude IS NOT NULL ORDER BY country",
            conn
        )
        selected_country = st.selectbox(
            "Filtrer par pays",
            ["Tous"] + countries['country'].tolist()
        )
    
    with col2:
        show_density = st.checkbox(
            "Afficher la densité",
            value=False,
            help="Affiche une carte de densité au lieu des points"
        )
    
    st.markdown("---")
    
    # Requête
    country_filter = f"AND country = '{selected_country}'" if selected_country != "Tous" else ""
    
    df = pd.read_sql(f"""
        SELECT 
            station_id,
            name,
            city,
            country,
            latitude,
            longitude,
            uic_code
        FROM stations
        WHERE latitude IS NOT NULL 
        AND longitude IS NOT NULL
        {country_filter}
        ORDER BY country, city
    """, conn)
    
    if df.empty:
        st.warning("⚠️ Aucune donnée géographique disponible.")
        return
    
    # Carte
    st.subheader("📍 Carte interactive")
    
    if show_density:
        fig = px.density_mapbox(
            df,
            lat='latitude',
            lon='longitude',
            radius=10,
            center=dict(lat=50, lon=10),
            zoom=3,
            mapbox_style="carto-positron",
            title="Densité des gares"
        )
    else:
        fig = px.scatter_mapbox(
            df,
            lat='latitude',
            lon='longitude',
            hover_name='name',
            hover_data={
                'city': True,
                'country': True,
                'uic_code': True,
                'latitude': False,
                'longitude': False
            },
            color='country',
            zoom=3,
            height=600,
            center=dict(lat=50, lon=10),
            mapbox_style="carto-positron"
        )
    
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        showlegend=True,
        legend=dict(
            title="Pays",
            orientation="h",
            yanchor="bottom",
            y=-0.1,
            xanchor="center",
            x=0.5
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Liste des gares
    st.subheader("📋 Liste des gares")
    
    st.dataframe(
        df[['name', 'city', 'country', 'uic_code', 'latitude', 'longitude']],
        use_container_width=True,
        hide_index=True,
        column_config={
            "name": st.column_config.TextColumn("Gare", width="large"),
            "city": st.column_config.TextColumn("Ville", width="medium"),
            "country": st.column_config.TextColumn("Pays", width="small"),
            "uic_code": st.column_config.TextColumn("Code UIC", width="small"),
            "latitude": st.column_config.NumberColumn("Latitude", width="small"),
            "longitude": st.column_config.NumberColumn("Longitude", width="small")
        }
    )
    
    # Export
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(
        label="📥 Exporter les coordonnées (CSV)",
        data=csv,
        file_name="stations_coordinates.csv",
        mime="text/csv"
    )
