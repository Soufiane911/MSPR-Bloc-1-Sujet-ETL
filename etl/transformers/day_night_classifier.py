"""
Module de classification jour/nuit pour l'ETL ObRail Europe.

Classifie les trains selon leur fonction (permettre le sommeil ou non).

Règles de classification:
- Train de NUIT si:
  1. A des couchettes/sleepers (preuve directe de vocation "nuit")
  2. OU durée >= 4h ET au moins 50% du trajet pendant la nuit (21h-5h)
- Train de JOUR sinon

Périodes:
- JOUR : 05h00 → 20h59
- NUIT : 21h00 → 04h59
"""

import pandas as pd
from datetime import time
from typing import Union, Tuple
from config.logging_config import setup_logging


class DayNightClassifier:
    """
    Classifie les trains en trains de jour ou trains de nuit.
    
    Un train de NUIT est un train dont la FONCTION est de permettre
    le sommeil pendant le trajet (remplacement d'une nuit d'hôtel).
    
    Critères:
    - Couchettes/sleepers présents → NUIT (preuve directe)
    - Durée >= 4h ET % nuit >= 50% → NUIT
    - Sinon → JOUR
    """
    
    # Bornes horaires pour le calcul du % de nuit
    NIGHT_START_HOUR = 21   # 21:00
    NIGHT_END_HOUR = 5      # 05:00
    
    # Seuils de classification
    MIN_DURATION_NIGHT = 4  # 4 heures minimum
    MIN_NIGHT_PCT = 50      # 50% du trajet en période nocturne
    
    # Labels
    LABEL_DAY = 'day'
    LABEL_NIGHT = 'night'
    
    def __init__(self):
        """Initialise le classificateur."""
        self.logger = setup_logging("transformer.classifier")
        self.stats = {'day': 0, 'night': 0, 'couchettes': 0, 'duree_nuit': 0}
    
    @classmethod
    def calc_night_percentage(cls, departure_time: str, arrival_time: str) -> float:
        """
        Calcule le pourcentage du trajet passé en période nocturne (21h-5h).
        
        Args:
            departure_time: Heure de départ (format "HH:MM:SS")
            arrival_time: Heure d'arrivée (format "HH:MM:SS")
            
        Returns:
            float: Pourcentage de nuit (0-100)
        """
        try:
            dep_parts = str(departure_time).split(':')
            arr_parts = str(arrival_time).split(':')
            
            dep_min = int(dep_parts[0]) * 60 + int(dep_parts[1])
            arr_min = int(arr_parts[0]) * 60 + int(arr_parts[1])
            
            # Durée totale
            if arr_min <= dep_min:
                duree = (24 * 60 - dep_min) + arr_min
            else:
                duree = arr_min - dep_min
            
            if duree == 0:
                return 0
            
            # Minutes de nuit (21h-5h)
            nuit_mins = 0
            cur = dep_min
            nuit_debut = cls.NIGHT_START_HOUR * 60  # 21h00
            nuit_fin = cls.NIGHT_END_HOUR * 60       # 05h00
            
            for _ in range(int(duree)):
                if cur >= nuit_debut or cur < nuit_fin:
                    nuit_mins += 1
                cur = (cur + 1) % (24 * 60)
            
            return (nuit_mins / duree) * 100
        except:
            return 0
    
    @classmethod
    def classify(cls, departure_time: str, arrival_time: str, 
                 duration_hours: float, has_couchette: bool = False,
                 has_sleeper: bool = False) -> Tuple[str, str]:
        """
        Classifie un train selon les règles métier.
        
        Args:
            departure_time: Heure de départ (format "HH:MM:SS")
            arrival_time: Heure d'arrivée (format "HH:MM:SS")
            duration_hours: Durée du trajet en heures
            has_couchette: Présence de couchettes
            has_sleeper: Présence de lits/sleepers
            
        Returns:
            Tuple[str, str]: (classification, raison)
        """
        # Règle 1: Couchettes = preuve directe
        if has_couchette or has_sleeper:
            return cls.LABEL_NIGHT, 'couchettes'
        
        # Règle 2: Durée >= 4h ET % nuit >= 50%
        if duration_hours >= cls.MIN_DURATION_NIGHT:
            night_pct = cls.calc_night_percentage(departure_time, arrival_time)
            if night_pct >= cls.MIN_NIGHT_PCT:
                return cls.LABEL_NIGHT, 'duree_4h_nuit_50pct'
        
        # Sinon = JOUR
        return cls.LABEL_DAY, 'jour'
    
    def add_classification_column(self, df: pd.DataFrame, 
                                   departure_col: str = 'departure_time',
                                   arrival_col: str = 'arrival_time',
                                   duration_col: str = 'duration_hours',
                                   couchette_col: str = 'has_couchette',
                                   sleeper_col: str = 'has_sleeper',
                                   output_col: str = 'train_type',
                                   raison_col: str = 'classification_raison') -> pd.DataFrame:
        """
        Ajoute les colonnes de classification à un DataFrame.
        
        Args:
            df: DataFrame avec les colonnes de données
            departure_col: Colonne heure de départ
            arrival_col: Colonne heure d'arrivée
            duration_col: Colonne durée (heures)
            couchette_col: Colonne présence couchettes
            sleeper_col: Colonne présence sleepers
            output_col: Colonne de sortie classification
            raison_col: Colonne de sortie raison
            
        Returns:
            pd.DataFrame: DataFrame avec colonnes de classification
        """
        df = df.copy()
        
        self.logger.info("Classification des trains (nouvelle méthode)...")
        
        # Application de la classification
        results = df.apply(
            lambda row: pd.Series(self.classify(
                departure_time=row.get(departure_col, ''),
                arrival_time=row.get(arrival_col, ''),
                duration_hours=row.get(duration_col, 0),
                has_couchette=row.get(couchette_col, False),
                has_sleeper=row.get(sleeper_col, False)
            )),
            axis=1
        )
        
        df[output_col] = results[0]
        df[raison_col] = results[1]
        
        # Calcul du % de nuit pour information
        df['night_percentage'] = df.apply(
            lambda row: self.calc_night_percentage(
                row.get(departure_col, ''),
                row.get(arrival_col, '')
            ),
            axis=1
        )
        
        # Statistiques
        counts = df[output_col].value_counts()
        self.stats['day'] = counts.get(self.LABEL_DAY, 0)
        self.stats['night'] = counts.get(self.LABEL_NIGHT, 0)
        
        # Comptage par raison
        raison_counts = df[raison_col].value_counts()
        self.stats['couchettes'] = raison_counts.get('couchettes', 0)
        self.stats['duree_nuit'] = raison_counts.get('duree_4h_nuit_50pct', 0)
        
        self.logger.info("=" * 50)
        self.logger.info("Résultat de la classification:")
        self.logger.info(f"  🌅 Trains de jour: {self.stats['day']:,}")
        self.logger.info(f"  🌙 Trains de nuit: {self.stats['night']:,}")
        self.logger.info(f"     - Avec couchettes: {self.stats['couchettes']:,}")
        self.logger.info(f"     - Durée >= 4h + nuit >= 50%: {self.stats['duree_nuit']:,}")
        self.logger.info(f"  📊 Total: {self.stats['day'] + self.stats['night']:,}")
        self.logger.info("=" * 50)
        
        return df
    
    def classify_from_timedelta(self, df: pd.DataFrame,
                                 timedelta_column: str = 'departure_time',
                                 output_column: str = 'train_type') -> pd.DataFrame:
        """
        Classifie à partir d'une colonne timedelta (format GTFS).
        
        Note: Cette méthode est conservée pour compatibilité mais
        utilise l'ancienne classification par heure de départ uniquement.
        Pour une classification complète, utiliser add_classification_column.
        """
        df = df.copy()
        
        self.logger.info(f"Classification depuis timedelta ({timedelta_column})...")
        
        # Conversion timedelta -> heure
        def timedelta_to_hour(td):
            if pd.isna(td):
                return None
            total_seconds = td.total_seconds()
            hours = int(total_seconds // 3600) % 24
            return hours
        
        df['departure_hour'] = df[timedelta_column].apply(timedelta_to_hour)
        df[output_column] = df['departure_hour'].apply(self.classify_simple)
        
        # Nettoyage
        df = df.drop(columns=['departure_hour'])
        
        # Statistiques
        counts = df[output_column].value_counts()
        self.stats['day'] = counts.get(self.LABEL_DAY, 0)
        self.stats['night'] = counts.get(self.LABEL_NIGHT, 0)
        
        self.logger.info(f"✓ Classification terminée: {self.stats['day']} jour, {self.stats['night']} nuit")
        
        return df
    
    @classmethod
    def classify_simple(cls, hour: int) -> str:
        """
        Classification simple basée uniquement sur l'heure de départ.
        
        Args:
            hour: Heure de départ (0-23)
            
        Returns:
            str: 'day' ou 'night'
        """
        # Période jour: 5h-21h
        # Période nuit: 21h-5h
        if 5 <= hour < 21:
            return cls.LABEL_DAY
        else:
            return cls.LABEL_NIGHT
    
    def get_stats(self) -> dict:
        """
        Retourne les statistiques de classification.
        
        Returns:
            dict: Statistiques détaillées jour/nuit
        """
        return self.stats.copy()
    
    def get_classification_ratio(self) -> dict:
        """
        Retourne le ratio de classification.
        
        Returns:
            dict: Ratios jour/nuit avec détails
        """
        total = self.stats['day'] + self.stats['night']
        
        if total == 0:
            return {'day_pct': 0, 'night_pct': 0}
        
        return {
            'day_pct': round(self.stats['day'] / total * 100, 2),
            'night_pct': round(self.stats['night'] / total * 100, 2),
            'total': total,
            'night_by_couchettes': self.stats.get('couchettes', 0),
            'night_by_duration': self.stats.get('duree_nuit', 0)
        }
