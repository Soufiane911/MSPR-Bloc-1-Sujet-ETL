"""Extracteur local pour la base Back-on-Track Night Train."""

import json
import pandas as pd
from pathlib import Path
from typing import Dict
from extractors.base_extractor import BaseExtractor


class BackOnTrackExtractor(BaseExtractor):
    """
    Extracteur pour la base de données Back-on-Track Night Train.

    Cette source fournit des données sur les trains de nuit en Europe,
    parfaitement adaptées pour la comparaison jour/nuit du projet ObRail.
    """

    ENDPOINTS = [
        "agencies",  # Opérateurs ferroviaires
        "stops",  # Gares et arrêts
        "routes",  # Lignes ferroviaires
        "trips",  # Voyages
        "trip_stop",  # Arrêts par voyage
        "calendar",  # Calendriers de service
        "calendar_dates",  # Dates spécifiques
        "translations",  # Traductions
        "classes",  # Classes de service
    ]

    def __init__(self):
        """Initialise l'extracteur Back-on-Track."""
        super().__init__(source_name="back_on_track")
        self.local_path = (
            Path(__file__).parent.parent.parent / "data" / "raw" / "back_on_track"
        )

    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Extrait tous les endpoints de Back-on-Track depuis les fichiers locaux.

        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire des DataFrames extraits

        Raises:
            FileNotFoundError: Si les fichiers n'ont pas ete telecharges
        """
        self.logger.info("=" * 60)
        self.logger.info("Démarrage de l'extraction Back-on-Track")
        self.logger.info("=" * 60)

        if not self.local_path.exists():
            raise FileNotFoundError(
                f"Données Back-on-Track introuvables dans {self.local_path}"
            )

        for endpoint in self.ENDPOINTS:
            file_path = self.local_path / f"{endpoint}.json"
            if not file_path.exists():
                self.logger.warning(f"[WARN] Fichier absent: {file_path.name}")
                continue

            with open(file_path, "r", encoding="utf-8") as file_handle:
                payload = json.load(file_handle)

            df = pd.DataFrame.from_dict(payload, orient="index")
            df = df.reset_index(names="id_from_key")
            self.data[endpoint] = df
            self.logger.info(f"[OK] {endpoint}: {len(df)} enregistrements extraits")

        self.logger.info("-" * 60)
        self.logger.info(f"Extraction terminée: {len(self.data)} endpoints")
        self.logger.info("=" * 60)

        return {"catalog": self.data}

    def validate(self) -> bool:
        """
        Valide l'intégrité des données extraites.

        Vérifie que les endpoints requis sont présents et non vides,
        et que les relations entre tables sont cohérentes.

        Returns:
            bool: True si les données sont valides, False sinon
        """
        self.logger.info("Validation des données Back-on-Track...")

        # Vérification des endpoints requis
        required_endpoints = ["agencies", "routes", "stops", "trips"]

        for endpoint in required_endpoints:
            if endpoint not in self.data:
                self.logger.error(f"[ERROR] Donnees manquantes pour {endpoint}")
                return False
            if self.data[endpoint].empty:
                self.logger.error(f"[ERROR] Donnees vides pour {endpoint}")
                return False

        # Validation des relations
        try:
            agency_ids = set(self.data["agencies"]["agency_id"])
            route_agencies = set(self.data["routes"]["agency_id"])

            orphaned_routes = route_agencies - agency_ids
            if orphaned_routes:
                self.logger.warning(
                    f"[WARN] {len(orphaned_routes)} agency_id des routes "
                    f"ne sont pas dans agencies: {list(orphaned_routes)[:5]}"
                )

            # Validation des stops
            if "trip_stop" in self.data:
                stop_ids = set(self.data["stops"]["stop_id"])
                trip_stop_ids = set(self.data["trip_stop"]["stop_id"])

                orphaned_stops = trip_stop_ids - stop_ids
                if orphaned_stops:
                    self.logger.warning(
                        f"[WARN] {len(orphaned_stops)} stop_id des trip_stop "
                        f"ne sont pas dans stops"
                    )

        except KeyError as e:
            self.logger.error(f"[ERROR] Erreur de validation: colonne manquante {e}")
            return False

        self.logger.info("[OK] Validation Back-on-Track reussie")
        return True

    def get_summary(self) -> Dict[str, int]:
        """
        Retourne un résumé des données extraites.

        Returns:
            Dict[str, int]: Nombre d'enregistrements par endpoint
        """
        return {endpoint: len(df) for endpoint, df in self.data.items()}
