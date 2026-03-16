"""
Extracteur générique pour les flux GTFS.

Gère le téléchargement et l'extraction des fichiers GTFS
pour les opérateurs ferroviaires nationaux.
"""

import pandas as pd
from typing import Dict, Optional
from pathlib import Path
from extractors.base_extractor import BaseExtractor


class GTFSExtractor(BaseExtractor):
    """
    Extracteur générique pour les flux GTFS.

    Gère le téléchargement, l'extraction et la lecture des fichiers GTFS
    au format standard (agency.txt, stops.txt, routes.txt, etc.)
    """

    CORE_GTFS_FILES = [
        "agency",
        "stops",
        "routes",
        "trips",
        "stop_times",
        "calendar",
        "calendar_dates",
    ]
    OPTIONAL_GTFS_FILES = ["shapes", "transfers", "feed_info"]

    GTFS_USECOLS = {
        "agency": [
            "agency_id",
            "agency_name",
            "agency_url",
            "agency_timezone",
            "agency_lang",
            "agency_phone",
        ],
        "stops": [
            "stop_id",
            "stop_code",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "stop_timezone",
            "location_type",
            "parent_station",
            "platform_code",
        ],
        "routes": [
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_desc",
            "route_type",
            "route_color",
            "route_text_color",
        ],
        "trips": [
            "route_id",
            "service_id",
            "trip_id",
            "trip_headsign",
            "trip_short_name",
            "direction_id",
            "block_id",
            "shape_id",
            "wheelchair_accessible",
            "bikes_allowed",
        ],
        "stop_times": [
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
            "stop_headsign",
            "pickup_type",
            "drop_off_type",
        ],
        "calendar": [
            "service_id",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "start_date",
            "end_date",
        ],
        "calendar_dates": ["service_id", "date", "exception_type"],
    }

    def __init__(
        self,
        source_name: str,
        country_code: str,
        include_optional_files: bool = False,
    ):
        """
        Initialise l'extracteur GTFS.

        Args:
            source_name: Nom de la source (ex: "SNCF", "Deutsche Bahn")
            country_code: Code pays ISO 3166-1 alpha-2
            include_optional_files: Charger aussi shapes/transfers/feed_info
        """
        super().__init__(source_name=source_name)
        self.country_code = country_code
        self.include_optional_files = include_optional_files
        self.extract_path = (
            Path(__file__).parent.parent.parent
            / "data"
            / "raw"
            / source_name.lower().replace(" ", "_")
        )

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]
        return df

    def _get_files_to_read(self) -> list[str]:
        files = list(self.CORE_GTFS_FILES)
        if self.include_optional_files:
            files.extend(self.OPTIONAL_GTFS_FILES)
        return files

    def extract(self) -> Dict[str, pd.DataFrame]:
        """
        Extrait les données GTFS depuis les fichiers locaux.

        Les fichiers doivent avoir été préalablement téléchargés par le downloader
        dans data/raw/{source_name}/

        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire des DataFrames GTFS

        Raises:
            FileNotFoundError: Si les fichiers GTFS ne sont pas présents
        """
        self.logger.info("=" * 60)
        self.logger.info(f"Extraction GTFS: {self.source_name}")
        self.logger.info(f"Chemin: {self.extract_path}")
        self.logger.info("=" * 60)

        try:
            if not self.extract_path.exists():
                raise FileNotFoundError(
                    f"Fichiers GTFS non trouvés dans {self.extract_path}. "
                    f"Exécutez d'abord la phase de téléchargement (--skip-download=False)"
                )

            self.logger.info("Lecture des fichiers GTFS locaux...")

            files_to_read = self._get_files_to_read()
            self.logger.info(
                f"Mode lecture rapide: {len(files_to_read)} fichiers GTFS utiles"
            )

            for gtfs_file in files_to_read:
                file_path = self.extract_path / f"{gtfs_file}.txt"

                if file_path.exists():
                    try:
                        wanted_cols = self.GTFS_USECOLS.get(gtfs_file)
                        usecols = None
                        if wanted_cols is not None:
                            wanted = {col.strip() for col in wanted_cols}
                            usecols = lambda col_name: str(col_name).strip() in wanted

                        df = pd.read_csv(
                            file_path,
                            dtype=str,
                            low_memory=False,
                            usecols=usecols,
                        )
                        df = self._normalize_columns(df)
                        self.data[gtfs_file] = df
                        self.logger.info(f"[OK] {gtfs_file}.txt: {len(df)} lignes")
                    except Exception as e:
                        self.logger.warning(
                            f"[WARN] Erreur lecture {gtfs_file}.txt: {e}"
                        )
                else:
                    self.logger.debug(f"○ Fichier optionnel absent: {gtfs_file}.txt")

            self.logger.info("-" * 60)
            self.logger.info(f"Extraction terminée: {len(self.data)} fichiers GTFS")
            self.logger.info("=" * 60)

            return self.data

        except FileNotFoundError as e:
            self.logger.error(f"[ERROR] Fichiers GTFS manquants: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"[ERROR] Erreur extraction GTFS: {str(e)}")
            raise

    def validate(self) -> bool:
        """
        Valide la structure GTFS.

        Vérifie que les fichiers requis sont présents selon la spécification GTFS.

        Returns:
            bool: True si la structure est valide, False sinon
        """
        self.logger.info(f"Validation GTFS: {self.source_name}")

        # Fichiers requis selon la spec GTFS
        required_files = ["agency", "stops", "routes", "trips", "stop_times"]

        missing_files = []
        for req_file in required_files:
            if req_file not in self.data:
                missing_files.append(req_file)
                self.logger.error(
                    f"[ERROR] Fichier GTFS requis manquant: {req_file}.txt"
                )

        if missing_files:
            self.logger.error(
                f"[ERROR] Validation echouee: {len(missing_files)} fichiers manquants"
            )
            return False

        # Vérification des données non vides
        empty_files = []
        for req_file in required_files:
            if self.data[req_file].empty:
                empty_files.append(req_file)
                self.logger.error(f"[ERROR] Fichier vide: {req_file}.txt")

        if empty_files:
            self.logger.error(
                f"[ERROR] Validation echouee: {len(empty_files)} fichiers vides"
            )
            return False

        self.logger.info("[OK] Validation GTFS reussie")
        return True

    def get_feed_info(self) -> Optional[Dict[str, str]]:
        """
        Retourne les informations du flux GTFS si disponibles.

        Returns:
            Dict[str, str]: Métadonnées du flux ou None
        """
        if "feed_info" in self.data and not self.data["feed_info"].empty:
            return self.data["feed_info"].iloc[0].to_dict()
        return None
