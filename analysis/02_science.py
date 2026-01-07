import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from pathlib import Path
import sys

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_PATH = PROJECT_ROOT / "etl" / "data" / "processed" / "gold" / "consolidated_trips.csv"
CLUSTERED_PATH = PROJECT_ROOT / "etl" / "data" / "processed" / "gold" / "clustered_trips.csv"
REPORT_PATH = PROJECT_ROOT / "analysis" / "reports" / "02_data_science_report.md"

def run_data_science_pipeline():
    print("🔬 Démarrage du pipeline Data Science (Clustering)...")
    
    if not GOLD_PATH.exists():
        print(f"❌ Erreur: Le fichier Gold n'existe pas : {GOLD_PATH}")
        sys.exit(1)

    df = pd.read_csv(GOLD_PATH)
    
    # 1. Feature Engineering (Selection)
    features = ['avg_speed_kmh', 'total_emission_kgco2e', 'distance_km']
    
    # Clean data for clustering
    df_model = df[features].dropna()
    
    if len(df_model) < 3:
        print("❌ Pas assez de données pour le clustering.")
        sys.exit(1)

    # 2. Scaling
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_model)
    
    # 3. K-Means Clustering
    k = 3
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)
    
    # Add clusters back to original DF (careful with indices if we dropped na)
    df.loc[df_model.index, 'cluster_id'] = clusters
    
    # 4. Cluster Analysis & Labeling
    cluster_stats = df.groupby('cluster_id')[features].mean()
    
    # Define logic to label clusters automatically
    # We look at Speed and Emission to name them
    labels = {}
    for cluster_id in range(k):
        stats = cluster_stats.loc[cluster_id]
        speed = stats['avg_speed_kmh']
        emission = stats['total_emission_kgco2e']
        
        # Simple heuristic logic
        if speed > cluster_stats['avg_speed_kmh'].mean() and emission > cluster_stats['total_emission_kgco2e'].mean():
            labels[cluster_id] = "Rapide & Polluant (TGV/Intercités)"
        elif speed < cluster_stats['avg_speed_kmh'].mean() and emission < cluster_stats['total_emission_kgco2e'].mean():
            labels[cluster_id] = "Lent & Éco (Bus/TER)"
        elif speed > cluster_stats['avg_speed_kmh'].mean() and emission < cluster_stats['total_emission_kgco2e'].mean():
             labels[cluster_id] = "Efficace (TGV Eco)"
        else:
            labels[cluster_id] = "Equilibré / Standard"
            
    df['cluster_label'] = df['cluster_id'].map(labels)
    
    # Save Result
    df.to_csv(CLUSTERED_PATH, index=False)
    print(f"✅ Fichier clusterisé sauvegardé : {CLUSTERED_PATH}")
    
    # 5. Generate Report
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("# 🧪 Rapport Data Science : Segmentation des Trajets\n\n")
        f.write(f"**Objectif :** Identifier des profils types de transport (Clustering K-Means).\n\n")
        
        f.write("## 1. Méthodologie\n")
        f.write("- **Algorithme :** K-Means (k=3)\n")
        f.write(f"- **Features utilisées :** {', '.join(features)}\n")
        f.write("- **Scaling :** StandardScaler (Normalisation)\n\n")
        
        f.write("## 2. Analyse des Clusters\n")
        f.write(cluster_stats.to_markdown())
        f.write("\n\n")
        
        f.write("## 3. Interprétation des Profils\n")
        for cid, label in labels.items():
            count = len(df[df['cluster_id'] == cid])
            f.write(f"- **Cluster {cid}** : `{label}` ({count} trajets)\n")
            
    print(f"✅ Rapport Data Science généré : {REPORT_PATH}")

if __name__ == "__main__":
    run_data_science_pipeline()
