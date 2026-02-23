"""
Analyse comparative équitable entre trains de jour et de nuit.

Ce module résout le problème de comparaison biaisée en utilisant des
métriques normalisées qui permettent une analyse statistiquement correcte.

Problème : Il y a naturellement 95%+ de trains de jour (sources GTFS nationales)
           vs seulement ~350 trains de nuit/jour (Back-on-Track européen).
           
Solution : Comparer la couverture réseau, les opérateurs, et les liaisons uniques
           plutôt que le volume brut de trains.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Optional
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.logging_config import setup_logging


class ComparativeAnalysis:
    """
    Analyse comparative équitable entre trains de jour et de nuit.
    
    Fournit des métriques normalisées pour une comparaison juste :
    - Distribution par source de données
    - Couverture par opérateur
    - Liaisons uniques par type
    - Opérateurs ayant des trains jour ET nuit
    """
    
    def __init__(self, engine=None):
        """
        Initialise l'analyseur comparatif.
        
        Args:
            engine: Moteur SQLAlchemy (optionnel)
        """
        self.logger = setup_logging("analysis.comparative")
        
        if engine is None:
            from config.database import engine as default_engine
            self.engine = default_engine
        else:
            self.engine = engine
    
    def get_distribution_by_source(self) -> pd.DataFrame:
        """
        Retourne la distribution des trains par source et type.
        
        Returns:
            pd.DataFrame: Distribution par source
        """
        query = """
        SELECT 
            source_name,
            train_type,
            COUNT(*) as nb_trains,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM trains
        GROUP BY source_name, train_type
        ORDER BY nb_trains DESC
        """
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        self.logger.info(f"Distribution par source: {len(df)} lignes")
        return df
    
    def get_distribution_by_country(self) -> pd.DataFrame:
        """
        Retourne la distribution des trains par pays et type.
        
        Returns:
            pd.DataFrame: Distribution par pays
        """
        query = """
        SELECT 
            COALESCE(o.country, 'Unknown') as country,
            t.train_type,
            COUNT(*) as nb_trains,
            COUNT(DISTINCT t.operator_id) as nb_operators
        FROM trains t
        LEFT JOIN operators o ON t.operator_id = o.operator_id
        GROUP BY o.country, t.train_type
        ORDER BY nb_trains DESC
        """
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        self.logger.info(f"Distribution par pays: {len(df)} lignes")
        return df
    
    def get_operators_by_type(self) -> pd.DataFrame:
        """
        Retourne les opérateurs par type de train.
        
        Returns:
            pd.DataFrame: Opérateurs par type
        """
        query = """
        SELECT 
            o.agency_name as operator_name,
            o.country,
            t.train_type,
            COUNT(*) as nb_trains
        FROM trains t
        JOIN operators o ON t.operator_id = o.operator_id
        GROUP BY o.agency_name, o.country, t.train_type
        ORDER BY nb_trains DESC
        """
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        return df
    
    def get_night_train_operators(self) -> pd.DataFrame:
        """
        Retourne les opérateurs de trains de nuit avec leurs statistiques.
        
        Returns:
            pd.DataFrame: Opérateurs de nuit
        """
        query = """
        SELECT 
            o.agency_name as operator_name,
            o.country,
            COUNT(*) as nb_night_trains,
            COUNT(DISTINCT t.route_name) as nb_routes
        FROM trains t
        JOIN operators o ON t.operator_id = o.operator_id
        WHERE t.train_type = 'night'
        GROUP BY o.agency_name, o.country
        ORDER BY nb_night_trains DESC
        """
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        self.logger.info(f"Opérateurs de nuit: {len(df)}")
        return df
    
    def get_operators_with_both_types(self) -> pd.DataFrame:
        """
        Retourne les opérateurs ayant des trains de jour ET de nuit.
        
        C'est la métrique la plus pertinente pour comparer car elle
        compare l'offre d'un même opérateur.
        
        Returns:
            pd.DataFrame: Opérateurs mixtes avec ratio jour/nuit
        """
        query = """
        WITH operator_stats AS (
            SELECT 
                o.operator_id,
                o.agency_name as operator_name,
                o.country,
                SUM(CASE WHEN t.train_type = 'day' THEN 1 ELSE 0 END) as day_trains,
                SUM(CASE WHEN t.train_type = 'night' THEN 1 ELSE 0 END) as night_trains,
                COUNT(*) as total_trains
            FROM trains t
            JOIN operators o ON t.operator_id = o.operator_id
            GROUP BY o.operator_id, o.agency_name, o.country
        )
        SELECT 
            operator_name,
            country,
            day_trains,
            night_trains,
            total_trains,
            ROUND(night_trains * 100.0 / NULLIF(total_trains, 0), 1) as night_percentage
        FROM operator_stats
        WHERE day_trains > 0 AND night_trains > 0
        ORDER BY night_percentage DESC
        """
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        self.logger.info(f"Opérateurs mixtes (jour+nuit): {len(df)}")
        return df
    
    def get_unique_routes_by_type(self) -> pd.DataFrame:
        """
        Retourne le nombre de liaisons uniques par type.
        
        Cette métrique mesure la couverture du réseau, pas le volume.
        
        Returns:
            pd.DataFrame: Liaisons uniques
        """
        query = """
        SELECT 
            train_type,
            COUNT(DISTINCT route_name) as unique_routes,
            COUNT(DISTINCT category) as unique_categories,
            COUNT(*) as total_trains
        FROM trains
        WHERE route_name IS NOT NULL AND route_name != ''
        GROUP BY train_type
        """
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        return df
    
    def get_comparison_summary(self) -> Dict[str, any]:
        """
        Retourne un résumé complet de la comparaison.
        
        Returns:
            Dict: Résumé avec toutes les métriques clés
        """
        query_totals = """
        SELECT 
            train_type,
            COUNT(*) as nb_trains,
            COUNT(DISTINCT operator_id) as nb_operators,
            COUNT(DISTINCT source_name) as nb_sources
        FROM trains
        GROUP BY train_type
        """
        
        with self.engine.connect() as conn:
            totals = pd.read_sql(query_totals, conn)
        
        summary = {
            'totals': totals.to_dict('records'),
            'operators_with_both': len(self.get_operators_with_both_types()),
            'night_operators': len(self.get_night_train_operators())
        }
        
        self.logger.info("✓ Résumé de comparaison généré")
        return summary
    
    def get_methodology_explanation(self) -> str:
        """
        Retourne l'explication méthodologique pour le dashboard.
        
        Returns:
            str: Texte explicatif
        """
        return """
## ⚠️ Pourquoi le déséquilibre 98%/2% est NORMAL

### Les sources de données sont différentes

| Source | Type | Portée | Volume |
|--------|------|--------|--------|
| SNCF Transilien | Jour | Régional (Île-de-France) | ~50k trains/semaine |
| GTFS Allemagne | Jour | National (DB) | ~90k trains/semaine |
| Back-on-Track | Nuit | **Européen** (26 pays) | ~350 trains/**jour** |

### Ce que ça signifie

Les trains de jour proviennent de **données GTFS nationales** qui incluent :
- Tous les trains régionaux (TER, S-Bahn...)
- Tous les trains de banlieue
- Toutes les fréquences (toutes les 15-30 min)

Les trains de nuit proviennent de **Back-on-Track** qui liste :
- Uniquement les trains de nuit européens
- ~200 lignes avec ~350 trains/jour
- Fréquence : 1-2 trains par jour par ligne

### Comment comparer correctement ?

✅ **Comparer par opérateur** : Certains opérateurs (ÖBB, Trenitalia) font jour ET nuit  
✅ **Comparer les liaisons uniques** : Couverture du réseau, pas volume  
✅ **Comparer par segment** : Longue distance vs régional  

❌ **Ne PAS comparer** les volumes bruts (comparer des pommes à des oranges)
        """


if __name__ == "__main__":
    analysis = ComparativeAnalysis()
    
    print("\n" + "=" * 70)
    print("ANALYSE COMPARATIVE JOUR/NUIT")
    print("=" * 70 + "\n")
    
    print("1. DISTRIBUTION PAR SOURCE")
    print("-" * 70)
    print(analysis.get_distribution_by_source().to_string(index=False))
    
    print("\n2. OPÉRATEURS DE TRAINS DE NUIT")
    print("-" * 70)
    print(analysis.get_night_train_operators().head(10).to_string(index=False))
    
    print("\n3. OPÉRATEURS MIXTES (JOUR + NUIT)")
    print("-" * 70)
    mixed = analysis.get_operators_with_both_types()
    if len(mixed) > 0:
        print(mixed.to_string(index=False))
    else:
        print("Aucun opérateur n'a à la fois des trains de jour ET de nuit dans les données.")
    
    print("\n4. RÉSUMÉ")
    print("-" * 70)
    summary = analysis.get_comparison_summary()
    for item in summary['totals']:
        print(f"  {item['train_type']}: {item['nb_trains']} trains, {item['nb_operators']} opérateurs")
