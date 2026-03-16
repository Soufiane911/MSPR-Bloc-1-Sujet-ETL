import streamlit as st
import pandas as pd
import plotly.express as px

try:
    from database import get_connection
except ImportError:
    get_connection = None


def render_tab_map(df: pd.DataFrame) -> None:
    st.subheader("Carte des gares")
    st.markdown("Visualisation géographique des gares ferroviaires")

    if get_connection is None:
        st.info("Carte non disponible - Base de données non configurée")
        return

    try:
        conn = get_connection()
    except Exception:
        st.info("Carte non disponible - Connexion à la base de données impossible")
        return

    col1, col2 = st.columns(2)

    with col1:
        countries = pd.read_sql(
            "SELECT DISTINCT country FROM stations WHERE latitude IS NOT NULL ORDER BY country",
            conn,
        )
        selected_country = st.selectbox(
            "Filtrer par pays", ["Tous"] + countries["country"].tolist()
        )

    with col2:
        show_density = st.checkbox(
            "Afficher la densité",
            value=False,
            help="Affiche une carte de densité au lieu des points",
        )

    country_filter = (
        f"AND country = '{selected_country}'" if selected_country != "Tous" else ""
    )

    df_map = pd.read_sql(
        f"""
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
    """,
        conn,
    )

    if df_map.empty:
        st.warning("Aucune donnée géographique disponible.")
        return

    if show_density:
        fig = px.density_mapbox(
            df_map,
            lat="latitude",
            lon="longitude",
            radius=10,
            center=dict(lat=50, lon=10),
            zoom=3,
            mapbox_style="carto-positron",
            title="Densité des gares",
        )
    else:
        fig = px.scatter_mapbox(
            df_map,
            lat="latitude",
            lon="longitude",
            hover_name="name",
            hover_data={
                "city": True,
                "country": True,
                "uic_code": True,
                "latitude": False,
                "longitude": False,
            },
            color="country",
            zoom=3,
            height=500,
            center=dict(lat=50, lon=10),
            mapbox_style="carto-positron",
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
            x=0.5,
        ),
    )

    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Voir la liste des gares"):
        st.dataframe(
            df_map[["name", "city", "country", "uic_code", "latitude", "longitude"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "name": st.column_config.TextColumn("Gare", width="large"),
                "city": st.column_config.TextColumn("Ville", width="medium"),
                "country": st.column_config.TextColumn("Pays", width="small"),
                "uic_code": st.column_config.TextColumn("Code UIC", width="small"),
                "latitude": st.column_config.NumberColumn("Latitude", width="small"),
                "longitude": st.column_config.NumberColumn("Longitude", width="small"),
            },
        )
