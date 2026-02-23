"""
Page de comparaison jour/nuit du dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from database import get_connection


def show():
    """Affiche la page de comparaison jour/nuit."""
    
    st.title("🌗 Comparaison Jour vs Nuit")
    st.markdown("Analyse détaillée des trains de jour et de nuit en Europe")
    
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
            ["Tous"] + countries['country'].tolist(),
            help="Sélectionnez un pays pour filtrer les données"
        )
    
    with col2:
        view_type = st.radio(
            "Type de visualisation",
            ["Graphique", "Tableau"],
            horizontal=True
        )
    
    st.markdown("---")
    
    # Requête avec filtre
    country_filter = f"AND o.country = '{selected_country}'" if selected_country != "Tous" else ""
    
    df = pd.read_sql(f"""
        SELECT 
            t.train_type,
            o.country,
            COUNT(DISTINCT t.train_id) as nb_trains,
            COUNT(s.schedule_id) as nb_schedules,
            ROUND(AVG(s.duration_min), 0) as avg_duration,
            ROUND(AVG(s.distance_km), 0) as avg_distance
        FROM trains t
        JOIN operators o ON t.operator_id = o.operator_id
        LEFT JOIN schedules s ON t.train_id = s.train_id
        WHERE 1=1 {country_filter}
        GROUP BY t.train_type, o.country
        ORDER BY o.country, t.train_type
    """, conn)
    
    if df.empty:
        st.warning("⚠️ Aucune donnée disponible pour les filtres sélectionnés.")
        return
    
    # Traduction des types
    df['Type'] = df['train_type'].map({
        'day': 'Jour 🌅',
        'night': 'Nuit 🌙'
    })
    
    if view_type == "Graphique":
        st.subheader("📊 Graphiques de comparaison")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Nombre de trains")
            
            fig = px.bar(
                df,
                x='country',
                y='nb_trains',
                color='Type',
                barmode='group',
                color_discrete_map={
                    'Jour 🌅': '#FFD700',
                    'Nuit 🌙': '#191970'
                },
                labels={
                    'nb_trains': 'Nombre de trains',
                    'country': 'Pays'
                }
            )
            fig.update_layout(
                xaxis_title="Pays",
                yaxis_title="Nombre de trains",
                legend_title="Type"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Durée moyenne (minutes)")
            
            fig = px.bar(
                df,
                x='country',
                y='avg_duration',
                color='Type',
                barmode='group',
                color_discrete_map={
                    'Jour 🌅': '#FFD700',
                    'Nuit 🌙': '#191970'
                },
                labels={
                    'avg_duration': 'Durée moyenne (min)',
                    'country': 'Pays'
                }
            )
            fig.update_layout(
                xaxis_title="Pays",
                yaxis_title="Durée (minutes)",
                legend_title="Type"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Deuxième ligne de graphiques
        col3, col4 = st.columns(2)
        
        with col3:
            st.markdown("#### Distance moyenne (km)")
            
            fig = px.bar(
                df,
                x='country',
                y='avg_distance',
                color='Type',
                barmode='group',
                color_discrete_map={
                    'Jour 🌅': '#FFD700',
                    'Nuit 🌙': '#191970'
                },
                labels={
                    'avg_distance': 'Distance moyenne (km)',
                    'country': 'Pays'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            st.markdown("#### Nombre de dessertes")
            
            fig = px.bar(
                df,
                x='country',
                y='nb_schedules',
                color='Type',
                barmode='group',
                color_discrete_map={
                    'Jour 🌅': '#FFD700',
                    'Nuit 🌙': '#191970'
                },
                labels={
                    'nb_schedules': 'Nombre de dessertes',
                    'country': 'Pays'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    else:
        # Vue tableau
        st.subheader("📋 Données détaillées")
        
        pivot_df = df.pivot(
            index='country',
            columns='Type',
            values=['nb_trains', 'avg_duration', 'avg_distance', 'nb_schedules']
        )
        
        st.dataframe(
            pivot_df,
            use_container_width=True,
            column_config={
                ("nb_trains", "Jour 🌅"): st.column_config.NumberColumn("Trains (Jour)"),
                ("nb_trains", "Nuit 🌙"): st.column_config.NumberColumn("Trains (Nuit)"),
                ("avg_duration", "Jour 🌅"): st.column_config.NumberColumn("Durée moyenne - Jour (min)"),
                ("avg_duration", "Nuit 🌙"): st.column_config.NumberColumn("Durée moyenne - Nuit (min)"),
                ("avg_distance", "Jour 🌅"): st.column_config.NumberColumn("Distance moyenne - Jour (km)"),
                ("avg_distance", "Nuit 🌙"): st.column_config.NumberColumn("Distance moyenne - Nuit (km)"),
            }
        )
    
    st.markdown("---")
    
    # Export
    st.subheader("📥 Export des données")
    
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(
        label="📥 Exporter les données (CSV)",
        data=csv,
        file_name="day_night_comparison.csv",
        mime="text/csv",
        help="Télécharge les données de comparaison au format CSV"
    )
