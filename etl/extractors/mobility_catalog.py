"""Extracteur local pour le catalogue Mobility Database."""

import pandas as pd
from pathlib import Path
from typing import Dict
from extractors.base_extractor import BaseExtractor


class MobilityCatalogExtractor(BaseExtractor):
    """
    Extracteur pour le catalogue Mobility Database.

    Ce catalogue contient plus de 2000 flux GTFS à travers le monde,
    permettant de découvrir et filtrer les sources par pays.
    """

    # Pays européens pour filtrage
    EUROPEAN_COUNTRIES = [
        "FR",
        "DE",
        "IT",
        "ES",
        "CH",
        "AT",
        "BE",
        "NL",
        "UK",
        "SE",
        "NO",
        "DK",
        "FI",
        "IE",
        "PT",
        "PL",
        "CZ",
        "HU",
        "RO",
        "BG",
        "HR",
        "SI",
        "SK",
        "LT",
        "LV",
        "EE",
        "LU",
    ]

    def __init__(self):
        """Initialise l'extracteur Mobility Catalog."""
        super().__init__(source_name="mobility_catalog")
        self.local_path = (
            Path(__file__).parent.parent.parent
            / "data"
            / "raw"
            / "mobility_catalog"
            / "data.csv"
        )

    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Charge le catalogue depuis le fichier local téléchargé.

        Returns:
            pd.DataFrame: Catalogue complet des flux GTFS
        """
        self.logger.info("=" * 60)
        self.logger.info("Chargement du catalogue Mobility Database")
        self.logger.info("=" * 60)

        try:
            if not self.local_path.exists():
                raise FileNotFoundError(
                    f"Catalogue Mobility Database introuvable dans {self.local_path}"
                )

            df = pd.read_csv(self.local_path)
            self.data["catalog"] = df

            self.logger.info(f"[OK] Catalogue telecharge: {len(df)} flux")
            self.logger.info(f"  - Colonnes: {len(df.columns)}")
            self.logger.info("=" * 60)

            return self.data

        except Exception as e:
            self.logger.error(f"[ERROR] Erreur telechargement catalogue: {str(e)}")
            raise

    def get_european_feeds(self) -> pd.DataFrame:
        """
        Filtre le catalogue pour les flux européens.

        Returns:
            pd.DataFrame: Flux GTFS européens
        """
        if "catalog" not in self.data:
            self.extract()

        df = self.data["catalog"]

        # Filtrer pour les flux européens GTFS
        europe_feeds = df[
            (df["location.country_code"].isin(self.EUROPEAN_COUNTRIES))
            & (df["data_type"] == "gtfs")
        ].copy()

        self.logger.info(f"Flux européens trouvés: {len(europe_feeds)}")

        return europe_feeds

    def get_feeds_by_country(self, country_code: str) -> pd.DataFrame:
        """
        Récupère les flux pour un pays spécifique.

        Args:
            country_code: Code pays ISO 3166-1 alpha-2

        Returns:
            pd.DataFrame: Flux pour le pays spécifié
        """
        if "catalog" not in self.data:
            self.extract()

        df = self.data["catalog"]

        country_feeds = df[
            (df["location.country_code"] == country_code) & (df["data_type"] == "gtfs")
        ].copy()

        self.logger.info(f"Flux pour {country_code}: {len(country_feeds)}")

        return country_feeds

    def get_feeds_by_operator(self, operator_name: str) -> pd.DataFrame:
        """
        Recherche les flux par nom d'opérateur.

        Args:
            operator_name: Nom (partiel) de l'opérateur

        Returns:
            pd.DataFrame: Flux correspondants
        """
        if "catalog" not in self.data:
            self.extract()

        df = self.data["catalog"]

        operator_feeds = df[
            df["provider"].str.contains(operator_name, case=False, na=False)
        ].copy()

        self.logger.info(
            f"Flux pour opérateur '{operator_name}': {len(operator_feeds)}"
        )

        return operator_feeds

    def validate(self) -> bool:
        """
        Valide le catalogue.

        Returns:
            bool: True si le catalogue est valide
        """
        if "catalog" not in self.data:
            self.logger.error("[ERROR] Catalogue non charge")
            return False

        df = self.data["catalog"]

        # Vérification des colonnes requises
        required_columns = [
            "mdb_source_id",
            "data_type",
            "provider",
            "urls.direct_download",
        ]

        for col in required_columns:
            if col not in df.columns:
                self.logger.error(f"[ERROR] Colonne requise manquante: {col}")
                return False

        self.logger.info("[OK] Validation catalogue reussie")
        return True

    def get_summary(self) -> Dict[str, int]:
        """
        Retourne un résumé du catalogue.

        Returns:
            Dict[str, int]: Statistiques du catalogue
        """
        if "catalog" not in self.data:
            self.extract()

        df = self.data["catalog"]
        europe = self.get_european_feeds()

        return {
            "total_feeds": int(len(df)),
            "european_feeds": int(len(europe)),
            "countries": int(df["location.country_code"].nunique()),
            "european_countries": int(europe["location.country_code"].nunique()),
        }
