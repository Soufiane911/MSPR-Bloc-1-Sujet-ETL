"""
Téléchargeur intelligent pour les sources de données.

Intègre la vérification de fraîcheur pour télécharger uniquement
si nécessaire, avec gestion du cache et fallback en cas d'échec.
"""

import zipfile
import io
import requests
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple, Union, Any
from datetime import datetime, timezone

from config.freshness import check_freshness, update_cache_after_download, CACHE_DIR


# Dossier pour les données brutes
RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"


class SmartDownloader:
    """
    Téléchargeur intelligent avec vérification de fraîcheur.

    Fonctionnalités:
    - Vérifie si les données sont à jour avant téléchargement
    - Gère le cache des métadonnées
    - Fallback sur données locales en cas d'échec réseau
    - Extraction automatique des ZIP (GTFS)
    """

    def __init__(
        self,
        source_name: str,
        url: str,
        source_type: str = "gtfs",
        endpoints: Optional[list[str]] = None,
        freshness_url: Optional[str] = None,
    ):
        """
        Initialise le téléchargeur.

        Args:
            source_name: Nom de la source (ex: "sncf_intercites")
            url: URL de téléchargement
            source_type: Type de source ('gtfs', 'json', 'csv')
        """
        self.source_name = source_name
        self.url = url
        self.source_type = source_type
        self.endpoints = endpoints or []
        self.freshness_url = freshness_url or url
        self.local_path = RAW_DIR / source_name
        self.logger = logging.getLogger(f"downloader.{source_name}")

        # Créer les dossiers nécessaires
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def should_download(
        self,
        max_age_hours: int = 168,
        refresh: bool = False,
        force: bool = False,
    ) -> Tuple[bool, str]:
        """
        Détermine si un téléchargement est nécessaire.

        Args:
            max_age_hours: Âge maximum en heures
            force: Force le téléchargement

        Returns:
            Tuple (needs_download, reason)
        """
        needs_download, reason, _ = check_freshness(
            source_name=self.source_name,
            url=self.freshness_url,
            local_path=self.local_path,
            max_age_hours=max_age_hours,
            refresh=refresh,
            force=force,
        )
        return needs_download, reason

    def download(self, timeout: int = 120) -> Tuple[bool, Union[bytes, Dict[str, str]]]:
        """
        Télécharge les données depuis l'URL.

        Args:
            timeout: Timeout en secondes

        Returns:
            Tuple (success, data_or_error)
        """
        self.logger.info(f"Téléchargement: {self.source_name}")
        self.logger.info(f"URL: {self.url}")

        try:
            response = requests.get(self.url, timeout=timeout, stream=True)
            response.raise_for_status()

            content_length = len(response.content)
            self.logger.info(f"[OK] Telecharge: {content_length / 1024 / 1024:.2f} MB")

            # Sauvegarder les métadonnées dans le cache
            update_cache_after_download(
                source_name=self.source_name,
                url=self.url,
                local_path=self.local_path,
                response_headers=dict(response.headers),
            )

            return True, response.content

        except requests.RequestException as e:
            self.logger.error(f"[ERROR] Erreur telechargement: {e}")
            return False, {"error": str(e)}

    def extract_gtfs(self, content: bytes) -> Dict:
        """
        Extrait un fichier ZIP GTFS.

        Args:
            content: Contenu du fichier ZIP

        Returns:
            Dict avec les fichiers extraits
        """
        self.local_path.mkdir(parents=True, exist_ok=True)

        extracted_files = {}

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                z.extractall(self.local_path)

                for name in z.namelist():
                    extracted_files[name] = str(self.local_path / name)

                self.logger.info(f"[OK] Extrait: {len(extracted_files)} fichiers")

        except zipfile.BadZipFile as e:
            self.logger.error(f"[ERROR] ZIP invalide: {e}")
            return {}

        return extracted_files

    def download_json_api(self, timeout: int = 120) -> Tuple[bool, Dict[str, Any]]:
        self.local_path.mkdir(parents=True, exist_ok=True)
        downloaded_files = []
        last_headers = {}

        try:
            for endpoint in self.endpoints:
                endpoint_url = f"{self.url}/{endpoint}.json"
                response = requests.get(endpoint_url, timeout=timeout)
                response.raise_for_status()

                file_path = self.local_path / f"{endpoint}.json"
                file_path.write_bytes(response.content)
                downloaded_files.append(str(file_path))
                last_headers = dict(response.headers)

            update_cache_after_download(
                source_name=self.source_name,
                url=self.freshness_url,
                local_path=self.local_path,
                response_headers=last_headers,
            )
            return True, {"files": downloaded_files}
        except requests.RequestException as e:
            self.logger.error(f"[ERROR] Erreur telechargement JSON API: {e}")
            return False, {"error": str(e)}

    def smart_download(
        self,
        max_age_hours: int = 168,
        refresh: bool = False,
        force: bool = False,
        extract: bool = True,
    ) -> Dict:
        """
        Téléchargement intelligent avec vérification de fraîcheur.

        Args:
            max_age_hours: Âge maximum en heures
            force: Force le téléchargement
            extract: Extraire automatiquement (pour GTFS)

        Returns:
            Dict avec le résultat de l'opération
        """
        result = {
            "source": self.source_name,
            "url": self.url,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "downloaded": False,
            "used_cache": False,
            "local_path": str(self.local_path),
            "files": [],
        }

        # 1. Vérifier si téléchargement nécessaire
        needs_download, reason = self.should_download(max_age_hours, refresh, force)
        result["check_reason"] = reason

        if not needs_download:
            self.logger.info(f"[OK] Donnees a jour: {reason}")
            result["used_cache"] = True
            result["files"] = (
                list(self.local_path.glob("*")) if self.local_path.exists() else []
            )
            return result

        self.logger.info(f"Téléchargement nécessaire: {reason}")

        # 2. Télécharger
        if self.source_type == "json_api":
            success, data = self.download_json_api()
        else:
            success, data = self.download()

        if success:
            result["downloaded"] = True

            # 3. Extraire si GTFS
            if extract and self.source_type == "gtfs":
                if not isinstance(data, bytes):
                    raise TypeError("Le téléchargement GTFS doit retourner des octets")
                extracted = self.extract_gtfs(data)
                result["files"] = list(extracted.keys())

            elif self.source_type in ["json", "csv"]:
                if not isinstance(data, bytes):
                    raise TypeError(
                        "Le téléchargement de fichier doit retourner des octets"
                    )
                # Sauvegarder directement
                self.local_path.mkdir(parents=True, exist_ok=True)
                filename = "data.json" if self.source_type == "json" else "data.csv"
                file_path = self.local_path / filename

                with open(file_path, "wb") as f:
                    f.write(data)

                result["files"] = [str(file_path)]
            elif self.source_type == "json_api":
                result["files"] = data.get("files", [])

        else:
            # 4. Fallback sur données locales
            if self.local_path.exists():
                self.logger.warning(
                    "[WARN] Utilisation donnees locales (echec telechargement)"
                )
                result["used_cache"] = True
                result["error"] = (
                    data.get("error") if isinstance(data, dict) else str(data)
                )
                result["files"] = list(self.local_path.glob("*"))
            else:
                result["error"] = (
                    data.get("error") if isinstance(data, dict) else str(data)
                )

        return result


def download_source(
    source_name: str,
    url: str,
    source_type: str = "gtfs",
    endpoints: Optional[list[str]] = None,
    freshness_url: Optional[str] = None,
    max_age_hours: int = 168,
    refresh: bool = False,
    force: bool = False,
) -> Dict:
    """
    Fonction utilitaire pour télécharger une source.

    Args:
        source_name: Nom de la source
        url: URL de téléchargement
        source_type: Type ('gtfs', 'json', 'csv')
        max_age_hours: Âge maximum en heures
        force: Force le téléchargement

    Returns:
        Dict avec le résultat
    """
    downloader = SmartDownloader(
        source_name,
        url,
        source_type,
        endpoints=endpoints,
        freshness_url=freshness_url,
    )
    return downloader.smart_download(max_age_hours, refresh, force)


def download_all_sources(
    sources_config: Dict,
    refresh: bool = False,
    force: bool = False,
    parallel: bool = False,
) -> Dict:
    """
    Télécharge toutes les sources configurées.

    Args:
        sources_config: Config des sources (depuis sources.py)
        force: Force le téléchargement
        parallel: Télécharger en parallèle (pas implémenté)

    Returns:
        Dict avec le résultat par source
    """
    results = {}

    for source_name, config in sources_config.items():
        if not config.get("enabled", True):
            results[source_name] = {"skipped": True, "reason": "disabled"}
            continue

        url = config.get("url")
        if not url:
            results[source_name] = {"skipped": True, "reason": "no_url"}
            continue

        source_type = config.get("type", "gtfs")
        max_age = config.get("max_age_hours", 168)

        results[source_name] = download_source(
            source_name=source_name,
            url=url,
            source_type=source_type,
            endpoints=config.get("endpoints"),
            freshness_url=config.get("freshness_url"),
            max_age_hours=max_age,
            refresh=refresh,
            force=force,
        )

    return results
