"""
Page des opérateurs du dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from database import get_connection


def show():
    """Affiche la page des opérateurs."""
    
    st.title("🏢 Analyse par Opérateur")
    st.markdown("Statistiques détaillées par opérateur ferroviaire")
    
    conn = get_connection()
    
    # Filtres
    st.subheader("🔍 Filtres")
    
    col1, col2 = st.columns(2)
    
    with col1:
        countries = pd.read_sql(
            "SELECT DISTINCT country FROM operators ORDER BY country",
            conn
        )
        selected_country = st.selectbox(
            "Filtrer par pays",
            ["Tous"] + countries['country'].tolist()
        )
    
    with col2:
        min_trains = st.slider(
            "Nombre minimum de trains",
            min_value=0,
            max_value=100,
            value=0,
            step=5
        )
    
    st.markdown("---")
    
    # Requête
    country_filter = f"AND o.country = '{selected_country}'" if selected_country != "Tous" else ""
    
    df = pd.read_sql(f"""
        SELECT 
            o.operator_id,
            o.name as operator_name,
            o.country,
            o.website,
            COUNT(DISTINCT t.train_id) as nb_trains,
            COUNT(DISTINCT CASE WHEN t.train_type = 'day' THEN t.train_id END) as day_trains,
            COUNT(DISTINCT CASE WHEN t.train_type = 'night' THEN t.train_id END) as night_trains,
            COUNT(s.schedule_id) as nb_schedules,
            ROUND(AVG(s.duration_min), 0) as avg_duration,
            ROUND(AVG(s.distance_km), 0) as avg_distance
        FROM operators o
        LEFT JOIN trains t ON o.operator_id = t.operator_id
        LEFT JOIN schedules s ON t.train_id = s.train_id
        WHERE 1=1 {country_filter}
        GROUP BY o.operator_id, o.name, o.country, o.website
        HAVING COUNT(DISTINCT t.train_id) >= {min_trains}
        ORDER BY nb_trains DESC
    """, conn)
    
    if df.empty:
        st.warning("⚠️ Aucune donnée disponible pour les filtres sélectionnés.")
        return
    
    # Graphiques
    st.subheader("📊 Visualisations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Répartition par opérateur")
        
        fig = px.bar(
            df.head(15),
            x='operator_name',
            y='nb_trains',
            color='country',
            labels={
                'operator_name': 'Opérateur',
                'nb_trains': 'Nombre de trains'
            }
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            xaxis_title="",
            yaxis_title="Nombre de trains"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### Répartition Jour/Nuit par opérateur")
        
        df_melted = df.head(10).melt(
            id_vars=['operator_name'],
            value_vars=['day_trains', 'night_trains'],
            var_name='Type',
            value_name='Count'
        )
        df_melted['Type'] = df_melted['Type'].map({
            'day_trains': 'Jour 🌅',
            'night_trains': 'Nuit 🌙'
        })
        
        fig = px.bar(
            df_melted,
            x='operator_name',
            y='Count',
            color='Type',
            barmode='stack',
            color_discrete_map={
                'Jour 🌅': '#FFD700',
                'Nuit 🌙': '#191970'
            },
            labels={
                'operator_name': 'Opérateur',
                'Count': 'Nombre de trains'
            }
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            xaxis_title=""
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tableau détaillé
    st.subheader("📋 Données détaillées")
    
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "operator_id": st.column_config.NumberColumn("ID", width="small"),
            "operator_name": st.column_config.TextColumn("Opérateur", width="large"),
            "country": st.column_config.TextColumn("Pays", width="small"),
            "website": st.column_config.LinkColumn("Site web", width="medium"),
            "nb_trains": st.column_config.NumberColumn("Trains", width="small"),
            "day_trains": st.column_config.NumberColumn("Jour 🌅", width="small"),
            "night_trains": st.column_config.NumberColumn("Nuit 🌙", width="small"),
            "nb_schedules": st.column_config.NumberColumn("Dessertes", width="small"),
            "avg_duration": st.column_config.NumberColumn("Durée moy. (min)", width="small"),
            "avg_distance": st.column_config.NumberColumn("Dist. moy. (km)", width="small")
        }
    )
    
    # Export
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(
        label="📥 Exporter les données (CSV)",
        data=csv,
        file_name="operators_analysis.csv",
        mime="text/csv"
    )
