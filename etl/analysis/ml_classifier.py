"""
Analyse ML pour la classification jour/nuit - ObRail Europe.

Ce script valide la classification métier avec un modèle Random Forest.
Génère les figures de la soutenance.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.database import engine


def load_data():
    """Charge les données depuis PostgreSQL."""
    query = """
    SELECT 
        t.train_id,
        t.train_type,
        t.category,
        s.duration_min,
        s.distance_km,
        EXTRACT(HOUR FROM s.departure_time) as departure_hour,
        CASE 
            WHEN t.category ILIKE '%couchette%' OR t.category ILIKE '%sleeper%' 
            THEN 1 ELSE 0 
        END as has_couchette
    FROM trains t
    JOIN schedules s ON t.train_id = s.train_id
    WHERE s.duration_min IS NOT NULL
    """
    return pd.read_sql(query, engine)


def calculate_night_pct(departure_hour, duration_hours):
    """Calcule le % du trajet en période nocturne (21h-5h)."""
    night_start, night_end = 21, 5
    night_minutes = 0
    
    for i in range(int(duration_hours * 60)):
        hour = (int(departure_hour) + i // 60) % 24
        if hour >= night_start or hour < night_end:
            night_minutes += 1
    
    total_minutes = duration_hours * 60
    return (night_minutes / total_minutes * 100) if total_minutes > 0 else 0


def prepare_features(df):
    """Prépare les features pour le ML."""
    df['duration_hours'] = df['duration_min'] / 60
    df['night_percentage'] = df.apply(
        lambda row: calculate_night_pct(row['departure_hour'], row['duration_hours']), 
        axis=1
    )
    
    features = ['duration_hours', 'night_percentage', 'has_couchette']
    X = df[features].fillna(0)
    y = (df['train_type'] == 'night').astype(int)
    
    return X, y, features


def train_model(X, y, features):
    """Entraîne le modèle Random Forest."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    
    y_pred = clf.predict(X_test)
    
    print("=" * 60)
    print("RÉSULTATS DU MODÈLE RANDOM FOREST")
    print("=" * 60)
    print(classification_report(y_test, y_pred, target_names=['Jour', 'Nuit']))
    
    # Feature importance
    importance = pd.DataFrame({
        'feature': features,
        'importance': clf.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nImportance des features:")
    print(importance.to_string(index=False))
    
    return clf, X_test, y_test, y_pred, importance


def plot_confusion_matrix(y_test, y_pred, output_path='soutenance/figures/fig4_matrice_confusion.png'):
    """Génère la matrice de confusion."""
    cm = confusion_matrix(y_test, y_pred)
    
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=['Jour', 'Nuit'],
                yticklabels=['Jour', 'Nuit'])
    plt.title('Matrice de Confusion - Classification Jour/Nuit')
    plt.ylabel('Vraie classe')
    plt.xlabel('Classe prédite')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"[OK] Matrice de confusion: {output_path}")


def plot_feature_importance(importance, output_path='soutenance/figures/fig5_importance_features.png'):
    """Génère le graphique d'importance des features."""
    plt.figure(figsize=(10, 6))
    sns.barplot(data=importance, x='importance', y='feature', palette='viridis')
    plt.title('Importance des Features - Random Forest')
    plt.xlabel('Importance relative')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"[OK] Feature importance: {output_path}")


def validate_back_on_track(clf, feature_cols):
    """Valide sur les trains Back-on-Track connus comme nuit."""
    print("\n" + "=" * 60)
    print("VALIDATION BACK-ON-TRACK")
    print("=" * 60)
    
    query = """
    SELECT 
        t.train_id,
        t.train_type,
        s.duration_min / 60.0 as duration_hours,
        EXTRACT(HOUR FROM s.departure_time) as departure_hour,
        1 as has_couchette
    FROM trains t
    JOIN schedules s ON t.train_id = s.train_id
    WHERE t.source_name LIKE '%back_on_track%'
    LIMIT 100
    """
    
    try:
        df = pd.read_sql(query, engine)
        if len(df) == 0:
            print("[WARN] Pas de donnees Back-on-Track trouvees")
            return
        
        df['night_percentage'] = df.apply(
            lambda row: calculate_night_pct(row['departure_hour'], row['duration_hours']), 
            axis=1
        )
        
        X = df[feature_cols].fillna(0)
        predictions = clf.predict(X)
        
        night_predicted = sum(predictions)
        print(f"Trains Back-on-Track testés: {len(df)}")
        print(f"Classifiés comme 'nuit' par le ML: {night_predicted} ({night_predicted/len(df)*100:.1f}%)")
        print(f"[OK] Validation reussie" if night_predicted == len(df) else "[WARN] Quelques differences")
        
    except Exception as e:
        print(f"Erreur validation: {e}")


def main():
    """Point d'entrée principal."""
    print("Analyse ML - Classification Jour/Nuit")
    print("=" * 60)
    
    try:
        df = load_data()
        print(f"[OK] Donnees chargees: {len(df)} trajets")
        print(f"  - Trains de jour: {sum(df['train_type'] == 'day')}")
        print(f"  - Trains de nuit: {sum(df['train_type'] == 'night')}")
        
        X, y, features = prepare_features(df)
        clf, X_test, y_test, y_pred, importance = train_model(X, y, features)
        
        plot_confusion_matrix(y_test, y_pred)
        plot_feature_importance(importance)
        validate_back_on_track(clf, features)
        
        print("\n" + "=" * 60)
        print("[OK] Analyse ML terminee avec succes")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERROR] Erreur: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
