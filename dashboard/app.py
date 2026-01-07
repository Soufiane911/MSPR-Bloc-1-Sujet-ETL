import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Configuration de la page
st.set_page_config(
    page_title="Eco-Travel Dashboard | MSPR Bloc 1",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS Personnalisé ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .metric-value {
        font-size: 2em;
        font-weight: bold;
        color: #0e1117;
    }
    .metric-label {
        font-size: 1em;
        color: #6c757d;
    }
    .stApp header {
        background-color: transparent;
    }
</style>
""", unsafe_allow_html=True)

# --- Chargement des Données ---
@st.cache_data
def load_data():
    # Essayer de charger le fichier clusterisé (Data Science)
    CLUSTERED_PATH = Path(__file__).parent.parent / "etl" / "data" / "processed" / "gold" / "clustered_trips.csv"
    GOLD_PATH = Path(__file__).parent.parent / "etl" / "data" / "processed" / "gold" / "consolidated_trips.csv"
    
    if CLUSTERED_PATH.exists():
        df = pd.read_csv(CLUSTERED_PATH)
        df['has_clusters'] = True
        return df
    elif GOLD_PATH.exists():
        df = pd.read_csv(GOLD_PATH)
        df['has_clusters'] = False
        return df
    else:
        st.error(f"Aucun fichier de données trouvé (ni {CLUSTERED_PATH.name} ni {GOLD_PATH.name})")
        return pd.DataFrame()

df = load_data()

# --- Sidebar ---
st.sidebar.image("https://img.icons8.com/color/96/000000/train.png", width=80)
st.sidebar.title("Filtres")

if not df.empty:
    # Filtre Agence
    all_agencies = ["Toutes"] + sorted(df['agency_name'].unique().tolist())
    selected_agency = st.sidebar.selectbox("Opérateur", all_agencies)
    
    # Filtre Pays Destination
    all_countries = ["Tous"] + sorted(df['destination_country'].unique().tolist())
    selected_country = st.sidebar.selectbox("Pays Destination", all_countries)
    
    # Filtrage du DF
    df_filtered = df.copy()
    if selected_agency != "Toutes":
        df_filtered = df_filtered[df_filtered['agency_name'] == selected_agency]
    if selected_country != "Tous":
        df_filtered = df_filtered[df_filtered['destination_country'] == selected_country]
else:
    df_filtered = pd.DataFrame()
    st.sidebar.warning("Aucune donnée chargée")

# --- Header ---
st.title("🌍 Eco-Travel Dashboard")
st.markdown("### Analyse de l'offre de transport ferroviaire et routier en Europe")
st.markdown("Ce dashboard présente les données consolidées (Gold) issues du pipeline ETL du **MSPR Bloc 1**.")

# --- KPIs ---
if not df_filtered.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_trips = len(df_filtered)
        st.metric("Trajets analysés", f"{total_trips:,}")
        
    with col2:
        avg_co2 = df_filtered['total_emission_kgco2e'].mean()
        st.metric("Émission Moyenne", f"{avg_co2:.2f} kgCO2e")
        
    with col3:
        avg_dist = df_filtered['distance_km'].mean()
        st.metric("Distance Moyenne", f"{avg_dist:.0f} km")
        
    with col4:
        nb_agencies = df_filtered['agency_name'].nunique()
        st.metric("Opérateurs", nb_agencies)
    
    st.divider()

    # --- Onglets ---
    tab1, tab2, tab3, tab4 = st.tabs(["🌱 Impact Écologique", "📊 Performance & Offre", "🗺️ Carte Réseau", "🧠 Data Science"])
    
    # --- Tab 1 : Écologie ---
    with tab1:
        st.subheader("Analyse de l'empreinte carbone")
        
        c1, c2 = st.columns(2)
        
        with c1:
            # Bar Chart : CO2 par Type de Transport
            avg_co2_type = df_filtered.groupby('train_type')['total_emission_kgco2e'].mean().reset_index()
            fig_bar = px.bar(
                avg_co2_type, 
                x='train_type', 
                y='total_emission_kgco2e',
                color='train_type',
                title="Émissions moyennes par type de transport (kgCO2e)",
                labels={'total_emission_kgco2e': 'Émissions (kg)', 'train_type': 'Type'}
            )
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with c2:
            # Scatter : Distance vs Émissions
            # On prend un sample pour ne pas surcharger le graph si trop de données
            sample_size = min(5000, len(df_filtered))
            df_sample = df_filtered.sample(sample_size)
            
            fig_scat = px.scatter(
                df_sample,
                x='distance_km',
                y='total_emission_kgco2e',
                color='train_type',
                title=f"Distance vs Émissions (Échantillon {sample_size} trajets)",
                hover_data=['origin_stop_name', 'destination_stop_name']
            )
            st.plotly_chart(fig_scat, use_container_width=True)
            
        st.info("💡 **Insight :** On observe que les Bus et les vieux TER ont une pente plus forte (plus d'émissions par km) que les TGV (très plats sur l'axe X).")

    # --- Tab 2 : Performance ---
    with tab2:
        st.subheader("Analyse de l'offre et de la performance")
        
        c1, c2 = st.columns(2)
        
        with c1:
            # Top Destinations
            top_dest = df_filtered['destination_stop_name'].value_counts().head(10).reset_index()
            top_dest.columns = ['Gare', 'Nombre de trajets']
            
            fig_dest = px.bar(
                top_dest,
                x='Nombre de trajets',
                y='Gare',
                orientation='h',
                title="Top 10 des Gares de Destination",
                color='Nombre de trajets',
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig_dest, use_container_width=True)
            
        with c2:
            # Distribution des durées
            fig_hist = px.histogram(
                df_filtered,
                x='duration_h',
                nbins=30,
                title="Distribution des durées de trajet (heures)",
                color='service_type',
                labels={'duration_h': 'Durée (h)'}
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        st.markdown("### 🚄 Nouveaux Indicateurs Calculés")
        c3, c4 = st.columns(2)
        with c3:
            # Vitesse Commerciale
            # Filtrer les vitesses aberrantes (> 400km/h)
            df_speed = df_filtered[(df_filtered['avg_speed_kmh'] > 0) & (df_filtered['avg_speed_kmh'] < 400)]
            fig_speed = px.box(
                df_speed,
                x='train_type',
                y='avg_speed_kmh',
                color='train_type',
                title="Vitesse Commerciale par Type de Transport",
                labels={'avg_speed_kmh': 'Vitesse Moyenne (km/h)'}
            )
            st.plotly_chart(fig_speed, use_container_width=True)
            
        with c4:
            # Green Score
            fig_green = px.histogram(
                df_filtered,
                x='green_score',
                nbins=20,
                title="Distribution du 'Green Score' (Note / 100)",
                color='train_type',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig_green, use_container_width=True)
            
        st.info("ℹ️ **Le Green Score** est un indicateur composite calculé par notre ETL : il pénalise les émissions CO2 et valorise les trains de nuit.")

    # --- Tab 3 : Carte ---
    with tab3:
        st.subheader("Carte des connexions")
        
        # Filtrer les données avec Lat/Lon valides
        map_data = df_filtered.dropna(subset=['origin_lat', 'origin_lon', 'destination_lat', 'destination_lon'])
        
        if not map_data.empty:
            # On affiche les gares de départ
            st.map(map_data, latitude='origin_lat', longitude='origin_lon', size=20, color='#0044ff')
            
            st.markdown(f"**{len(map_data)}** trajets géolocalisés affichés (points de départ).")
            st.warning("⚠️ Note : Pour tracer des lignes complètes, une librairie avancée comme PyDeck serait nécessaire, mais 'st.map' suffit pour visualiser la densité des points de départ.")
        else:
            st.warning("⚠️ Aucune donnée de géolocalisation disponible pour les filtres sélectionnés.")

    # --- Tab 4 : Data Science ---
    with tab4:
        st.subheader("🧪 Analyse Avancée & Clustering (K-Means)")
        
        if df.get('has_clusters', False).any():
            st.markdown("""
            Nous avons utilisé un algorithme de **K-Means Clustering** pour segmenter automatiquement les offres de transport en groupes homogènes.
            L'algorithme a identifié des profils basés sur la vitesse, les émissions CO2 et la distance.
            """)
            
            c1, c2 = st.columns([2, 1])
            
            with c1:
                # Scatter Plot 3D ou 2D amélioré
                fig_cluster = px.scatter(
                    df_filtered,
                    x='avg_speed_kmh',
                    y='total_emission_kgco2e',
                    color='cluster_label',
                    size='distance_km',
                    hover_data=['route_name', 'agency_name'],
                    title="Segmentation des Trajets : Vitesse vs Émissions (Taille = Distance)",
                    labels={
                        'avg_speed_kmh': 'Vitesse Moyenne (km/h)',
                        'total_emission_kgco2e': 'Émissions Totales (kgCO2e)',
                        'cluster_label': 'Segment (Cluster)',
                        'distance_km': 'Distance (km)'
                    },
                    color_discrete_sequence=px.colors.qualitative.Bold
                )
                st.plotly_chart(fig_cluster, use_container_width=True)
                
            with c2:
                st.markdown("### 📋 Profils Identifiés")
                cluster_counts = df_filtered['cluster_label'].value_counts()
                st.write(cluster_counts)
                
                st.info("""
                **Interprétation :**
                * Les clusters permettent de voir rapidement si une offre est "Rapide & Polluante" ou "Lente & Écologique".
                * Cela aide les décideurs à visualiser le trade-off entre temps et impact carbone.
                """)
        else:
            st.warning("⚠️ Les données de clustering ne sont pas disponibles. Veuillez exécuter le script `analysis/02_science.py`.")

else:
    st.info("En attente de données...")

# --- Footer ---
st.markdown("---")
st.markdown("© 2025 - Dashboard réalisé pour l'école EPSI - MSPR Data Engineering")
