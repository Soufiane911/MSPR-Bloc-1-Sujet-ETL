"""
Page de qualité des données du dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from database import get_connection


def show():
    """Affiche la page de qualité des données."""
    
    st.title("✅ Qualité des Données")
    st.markdown("Contrôle de la qualité et de la complétude des données")
    
    conn = get_connection()
    
    # Statistiques globales
    st.subheader("📊 Vue d'ensemble de la qualité")
    
    df_quality = pd.read_sql("SELECT * FROM v_data_quality", conn)
    
    if not df_quality.empty:
        # Affichage des métriques
        cols = st.columns(len(df_quality))
        
        for idx, row in df_quality.iterrows():
            with cols[idx]:
                table_name = row['table_name'].capitalize()
                total = row['total_records']
                
                # Calcul du taux de complétude moyen
                completeness_cols = [c for c in row.index if 'complete' in c]
                avg_completeness = sum(row[c] for c in completeness_cols) / len(completeness_cols) / total * 100 if completeness_cols else 0
                
                st.metric(
                    label=f"📁 {table_name}",
                    value=f"{total:,}",
                    delta=f"{avg_completeness:.1f}% complet"
                )
    
    st.markdown("---")
    
    # Tableau détaillé
    st.subheader("📋 Détails par table")
    
    st.dataframe(
        df_quality,
        use_container_width=True,
        hide_index=True,
        column_config={
            "table_name": st.column_config.TextColumn("Table", width="medium"),
            "total_records": st.column_config.NumberColumn("Total", width="small"),
        }
    )
    
    st.markdown("---")
    
    # Statistiques par source
    st.subheader("📈 Répartition par source")
    
    df_sources = pd.read_sql("""
        SELECT 
            source_name,
            COUNT(*) as count
        FROM (
            SELECT source_name FROM operators
            UNION ALL
            SELECT source_name FROM stations
            UNION ALL
            SELECT source_name FROM trains
        ) combined
        WHERE source_name IS NOT NULL
        GROUP BY source_name
        ORDER BY count DESC
    """, conn)
    
    if not df_sources.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                df_sources,
                values='count',
                names='source_name',
                title="Répartition par source de données"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                df_sources,
                x='source_name',
                y='count',
                color='count',
                color_continuous_scale='Viridis',
                labels={
                    'source_name': 'Source',
                    'count': 'Nombre d\'enregistrements'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Valeurs manquantes
    st.subheader("⚠️ Valeurs manquantes potentielles")
    
    df_missing = pd.read_sql("""
        SELECT 
            'stations' as table_name,
            'coordinates' as field,
            COUNT(*) FILTER (WHERE latitude IS NULL OR longitude IS NULL) as missing_count,
            COUNT(*) as total_count
        FROM stations
        UNION ALL
        SELECT 
            'stations' as table_name,
            'uic_code' as field,
            COUNT(*) FILTER (WHERE uic_code IS NULL) as missing_count,
            COUNT(*) as total_count
        FROM stations
        UNION ALL
        SELECT 
            'trains' as table_name,
            'category' as field,
            COUNT(*) FILTER (WHERE category IS NULL) as missing_count,
            COUNT(*) as total_count
        FROM trains
        UNION ALL
        SELECT 
            'schedules' as table_name,
            'distance_km' as field,
            COUNT(*) FILTER (WHERE distance_km IS NULL) as missing_count,
            COUNT(*) as total_count
        FROM schedules
    """, conn)
    
    if not df_missing.empty:
        df_missing['missing_pct'] = (df_missing['missing_count'] / df_missing['total_count'] * 100).round(2)
        
        st.dataframe(
            df_missing,
            use_container_width=True,
            hide_index=True,
            column_config={
                "table_name": st.column_config.TextColumn("Table", width="medium"),
                "field": st.column_config.TextColumn("Champ", width="medium"),
                "missing_count": st.column_config.NumberColumn("Manquantes", width="small"),
                "total_count": st.column_config.NumberColumn("Total", width="small"),
                "missing_pct": st.column_config.ProgressColumn(
                    "% Manquant",
                    width="medium",
                    min_value=0,
                    max_value=100,
                    format="%.1f%%"
                )
            }
        )
    
    # Export
    st.markdown("---")
    st.subheader("📥 Export")
    
    col1, col2 = st.columns(2)
    
    with col1:
        csv_quality = df_quality.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 Qualité des données (CSV)",
            data=csv_quality,
            file_name="data_quality.csv",
            mime="text/csv"
        )
    
    with col2:
        csv_sources = df_sources.to_csv(index=False, encoding='utf-8-sig') if not df_sources.empty else ""
        if csv_sources:
            st.download_button(
                label="📥 Sources (CSV)",
                data=csv_sources,
                file_name="data_sources.csv",
                mime="text/csv"
            )
