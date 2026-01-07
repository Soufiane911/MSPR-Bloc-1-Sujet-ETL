import pandas as pd
import sys
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_PATH = PROJECT_ROOT / "etl" / "data" / "processed" / "gold" / "consolidated_trips.csv"
REPORT_PATH = PROJECT_ROOT / "analysis" / "reports" / "01_data_discovery_report.md"

def generate_discovery_report():
    print("🔍 Démarrage de l'analyse exploratoire (Data Discovery)...")
    
    if not GOLD_PATH.exists():
        print(f"❌ Erreur: Le fichier Gold n'existe pas : {GOLD_PATH}")
        sys.exit(1)
        
    df = pd.read_csv(GOLD_PATH)
    
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("# 📊 Rapport de Data Discovery\n\n")
        f.write(f"**Date de génération :** {pd.Timestamp.now()}\n\n")
        f.write(f"**Source de données :** `{GOLD_PATH.name}`\n\n")
        
        # 1. Aperçu Général
        f.write("## 1. Aperçu du Dataset\n")
        f.write(f"- **Nombre de lignes :** {df.shape[0]}\n")
        f.write(f"- **Nombre de colonnes :** {df.shape[1]}\n")
        f.write(f"- **Taille en mémoire :** {df.memory_usage(deep=True).sum() / 1024:.2f} KB\n\n")
        
        # 2. Types de données et Manquants
        f.write("## 2. Qualité des Données\n")
        f.write("| Colonne | Type | Valeurs Manquantes | % Manquant |\n")
        f.write("|---|---|---|---|\n")
        for col in df.columns:
            missing = df[col].isnull().sum()
            pct = (missing / len(df)) * 100
            dtype = str(df[col].dtype)
            f.write(f"| {col} | {dtype} | {missing} | {pct:.1f}% |\n")
        f.write("\n")
        
        # 3. Statistiques Descriptives (Numérique)
        f.write("## 3. Statistiques Descriptives (Variables Numériques)\n")
        desc = df.describe().transpose()
        # Convert to markdown table
        f.write(desc.to_markdown())
        f.write("\n\n")
        
        # 4. Statistiques Descriptives (Catégorielle)
        f.write("## 4. Répartition des Variables Catégorielles\n")
        cat_cols = ['agency_name', 'train_type', 'service_type', 'source_dataset', 'origin_country']
        for col in cat_cols:
            if col in df.columns:
                f.write(f"### Répartition : {col}\n")
                counts = df[col].value_counts().head(10)
                f.write(counts.to_markdown())
                f.write("\n\n")

        # 5. Analyse des Corrélations (Simplifiée)
        f.write("## 5. Corrélations Clés\n")
        numeric_cols = ['distance_km', 'duration_h', 'avg_speed_kmh', 'total_emission_kgco2e', 'green_score']
        # Filter only existing numeric columns
        existing_nums = [c for c in numeric_cols if c in df.columns]
        if existing_nums:
            corr = df[existing_nums].corr()
            f.write(corr.to_markdown())
            f.write("\n\n")
            
    print(f"✅ Rapport généré avec succès : {REPORT_PATH}")

if __name__ == "__main__":
    generate_discovery_report()
