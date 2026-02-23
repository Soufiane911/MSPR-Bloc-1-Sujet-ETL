"""
Page de vue d'ensemble du dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from database import get_connection


def show():
    """Affiche la page de vue d'ensemble."""
    
    st.title("📊 Vue d'ensemble")
    st.markdown("Indicateurs clés et statistiques globales des données ferroviaires européennes")
    
    # Connexion BDD
    conn = get_connection()
    
    # Section KPIs
    st.subheader("📈 Indicateurs clés")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_trains = pd.read_sql("SELECT COUNT(*) FROM trains", conn).iloc[0, 0]
        st.metric(
            label="🚂 Total Trains",
            value=f"{total_trains:,}",
            help="Nombre total de trains dans la base de données"
        )
    
    with col2:
        total_stations = pd.read_sql("SELECT COUNT(*) FROM stations", conn).iloc[0, 0]
        st.metric(
            label="🚉 Gares référencées",
            value=f"{total_stations:,}",
            help="Nombre total de gares et arrêts"
        )
    
    with col3:
        total_operators = pd.read_sql("SELECT COUNT(*) FROM operators", conn).iloc[0, 0]
        st.metric(
            label="🏢 Opérateurs",
            value=f"{total_operators:,}",
            help="Nombre d'opérateurs ferroviaires"
        )
    
    with col4:
        total_schedules = pd.read_sql("SELECT COUNT(*) FROM schedules", conn).iloc[0, 0]
        st.metric(
            label="📅 Dessertes",
            value=f"{total_schedules:,}",
            help="Nombre total de dessertes"
        )
    
    st.markdown("---")
    
    # Section graphiques
    st.subheader("📊 Analyses visuelles")
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown("#### 🌗 Répartition Jour/Nuit")
        
        df_type = pd.read_sql("""
            SELECT train_type, COUNT(*) as count 
            FROM trains 
            GROUP BY train_type
        """, conn)
        
        # Traduction des labels
        df_type['train_type_label'] = df_type['train_type'].map({
            'day': 'Jour 🌅',
            'night': 'Nuit 🌙'
        })
        
        fig = px.pie(
            df_type,
            values='count',
            names='train_type_label',
            color='train_type',
            color_discrete_map={
                'day': '#FFD700',
                'night': '#191970'
            },
            hole=0.4
        )
        fig.update_traces(
            textinfo='percent+label',
            textfont_size=14
        )
        fig.update_layout(
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.1,
                xanchor="center",
                x=0.5
            )
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Alternative textuelle pour accessibilité
        day_count = df_type[df_type['train_type'] == 'day']['count'].values
        night_count = df_type[df_type['train_type'] == 'night']['count'].values
        
        st.markdown(f"""
        <div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px;">
        <strong>Description alternative:</strong> Le graphique montre la répartition entre 
        trains de jour ({day_count[0] if len(day_count) > 0 else 0}) 
        et trains de nuit ({night_count[0] if len(night_count) > 0 else 0}).
        </div>
        """, unsafe_allow_html=True)
    
    with col_right:
        st.markdown("#### 🌍 Répartition par pays")
        
        df_country = pd.read_sql("""
            SELECT 
                country,
                COUNT(DISTINCT t.train_id) as nb_trains
            FROM operators o
            LEFT JOIN trains t ON o.operator_id = t.operator_id
            GROUP BY country
            HAVING COUNT(DISTINCT t.train_id) > 0
            ORDER BY nb_trains DESC
            LIMIT 10
        """, conn)
        
        fig = px.bar(
            df_country,
            x='country',
            y='nb_trains',
            color='nb_trains',
            color_continuous_scale='Blues',
            labels={
                'country': 'Pays',
                'nb_trains': 'Nombre de trains'
            }
        )
        fig.update_layout(
            xaxis_title="Pays",
            yaxis_title="Nombre de trains",
            coloraxis_showscale=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Section tableau des opérateurs
    st.subheader("🏢 Top Opérateurs")
    
    df_ops = pd.read_sql("""
        SELECT 
            o.name as "Opérateur",
            o.country as "Pays",
            COUNT(DISTINCT t.train_id) as "Trains",
            COUNT(DISTINCT CASE WHEN t.train_type = 'day' THEN t.train_id END) as "Jour",
            COUNT(DISTINCT CASE WHEN t.train_type = 'night' THEN t.train_id END) as "Nuit",
            COUNT(s.schedule_id) as "Dessertes"
        FROM operators o
        LEFT JOIN trains t ON o.operator_id = t.operator_id
        LEFT JOIN schedules s ON t.train_id = s.train_id
        GROUP BY o.operator_id, o.name, o.country
        HAVING COUNT(DISTINCT t.train_id) > 0
        ORDER BY "Trains" DESC
        LIMIT 15
    """, conn)
    
    st.dataframe(
        df_ops,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Opérateur": st.column_config.TextColumn("Opérateur", width="large"),
            "Pays": st.column_config.TextColumn("Pays", width="small"),
            "Trains": st.column_config.NumberColumn("Trains", width="small"),
            "Jour": st.column_config.NumberColumn("Jour 🌅", width="small"),
            "Nuit": st.column_config.NumberColumn("Nuit 🌙", width="small"),
            "Dessertes": st.column_config.NumberColumn("Dessertes", width="small")
        }
    )
    
    # Export CSV accessible
    csv = df_ops.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(
        label="📥 Télécharger les données (CSV)",
        data=csv,
        file_name="obrail_operators.csv",
        mime="text/csv",
        help="Télécharge les données des opérateurs au format CSV"
    )
