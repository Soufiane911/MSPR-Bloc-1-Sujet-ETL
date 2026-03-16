"""
Vérification de fraîcheur des sources de données.

Compare les métadonnées distantes (Last-Modified, ETag) avec les données
locales pour déterminer si un téléchargement est nécessaire.
"""

import os
import json
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple, Optional, Any


# Dossier cache pour les métadonnées
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / ".cache"


def ensure_cache_dir():
    """Crée le dossier cache si inexistant."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_cache_path(source_name: str) -> Path:
    """Retourne le chemin du fichier cache pour une source."""
    return CACHE_DIR / f"{source_name}.json"


def load_cache(source_name: str) -> Optional[Dict]:
    """Charge les métadonnées de cache pour une source."""
    cache_path = get_cache_path(source_name)
    if cache_path.exists():
        with open(cache_path, "r") as f:
            return json.load(f)
    return None


def save_cache(source_name: str, metadata: Dict):
    """Sauvegarde les métadonnées de cache pour une source."""
    ensure_cache_dir()
    cache_path = get_cache_path(source_name)
    with open(cache_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


def get_remote_headers(url: str, timeout: int = 5) -> Dict:
    """
    Récupère les headers HTTP d'une URL via requête HEAD.

    Returns:
        Dict avec last_modified, etag, content_length, status_code
    """
    try:
        response = requests.head(url, timeout=timeout, allow_redirects=True)

        return {
            "last_modified": response.headers.get("Last-Modified"),
            "etag": response.headers.get("ETag"),
            "content_length": response.headers.get("Content-Length"),
            "status_code": response.status_code,
            "accessible": response.status_code == 200,
        }
    except requests.RequestException as e:
        return {
            "last_modified": None,
            "etag": None,
            "content_length": None,
            "status_code": None,
            "accessible": False,
            "error": str(e),
        }


def parse_http_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse une date HTTP (format RFC 7231) en datetime."""
    if not date_str:
        return None

    formats = [
        "%a, %d %b %Y %H:%M:%S GMT",  # RFC 7231
        "%a, %d-%b-%Y %H:%M:%S GMT",  # RFC 850
        "%a %b %d %H:%M:%S %Y",  # ANSI C asctime
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def check_freshness(
    source_name: str,
    url: str,
    local_path: Optional[Path] = None,
    max_age_hours: int = 168,
    refresh: bool = False,
    force: bool = False,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Vérifie si les données locales sont à jour.

    Args:
        source_name: Nom de la source
        url: URL distante
        local_path: Chemin des données locales (optionnel)
        max_age_hours: Âge maximum en heures avant téléchargement forcé
        force: Force le téléchargement même si à jour

    Returns:
        Tuple (needs_download: bool, reason: str, metadata: Dict)
    """
    result: Dict[str, Any] = {
        "source": source_name,
        "url": url,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }

    # 1. Force demandé
    if force:
        return True, "forced_by_user", result

    # 2. Données locales absentes
    if local_path and not local_path.exists():
        return True, "no_local_data", result

    # 3. Cache absent
    cache = load_cache(source_name)
    if not cache:
        return True, "no_cache_metadata", result

    result["cache"] = cache

    # 4. Âge local > max_age_hours
    last_download = cache.get("last_download")
    if last_download:
        last_dt = datetime.fromisoformat(last_download.replace("Z", "+00:00"))
        age_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600

        if age_hours > max_age_hours:
            return True, f"expired_by_age ({age_hours:.1f}h > {max_age_hours}h)", result

    # 5. En mode normal, on vérifie uniquement l'âge du cache local
    # Pas de requête réseau pour accélérer le démarrage
    if not refresh:
        return False, "fresh_cache_local", result

    # 6. Vérification headers distants
    remote_headers = get_remote_headers(url)
    result["remote_headers"] = remote_headers

    if not remote_headers.get("accessible"):
        # Serveur inaccessible -> garder données locales
        return False, "remote_unavailable_using_cache", result

    # 7. Comparer Last-Modified
    remote_modified = parse_http_date(remote_headers.get("last_modified"))
    cached_modified = parse_http_date(cache.get("last_modified_remote"))

    if remote_modified and cached_modified:
        if remote_modified > cached_modified:
            return True, "remote_updated", result

    # 8. Comparer ETag
    remote_etag = remote_headers.get("etag")
    cached_etag = cache.get("etag")

    if remote_etag and cached_etag:
        if remote_etag != cached_etag:
            return True, "etag_changed", result

    # 9. Comparer Content-Length (fallback)
    remote_length = remote_headers.get("content_length")
    cached_length = cache.get("content_length")

    if remote_length and cached_length:
        if remote_length != cached_length:
            return True, "content_length_changed", result

    return False, "up_to_date", result


def update_cache_after_download(
    source_name: str, url: str, local_path: Path, response_headers: Dict
):
    """Met à jour le cache après un téléchargement réussi."""
    metadata = {
        "source": source_name,
        "url": url,
        "last_download": datetime.now(timezone.utc).isoformat(),
        "last_modified_remote": response_headers.get("Last-Modified"),
        "etag": response_headers.get("ETag"),
        "content_length": response_headers.get("Content-Length"),
        "local_path": str(local_path),
        "status": "success",
    }
    save_cache(source_name, metadata)


def get_all_sources_status(sources: Dict) -> Dict:
    """
    Retourne le statut de fraîcheur de toutes les sources.

    Args:
        sources: Dictionnaire des sources depuis config/sources.py

    Returns:
        Dict avec le statut de chaque source
    """
    ensure_cache_dir()
    status = {}

    for source_name, config in sources.items():
        # Récupérer l'URL (peut être 'url' ou 'base_url')
        url = config.get("url") or config.get("base_url", "")
        if not url:
            status[source_name] = {"status": "no_url", "needs_download": False}
            continue

        cache = load_cache(source_name)

        needs_download, reason, _ = check_freshness(
            source_name=source_name,
            url=config.get("freshness_url") or url,
            max_age_hours=config.get("max_age_hours", 168),
            refresh=True,
        )

        status[source_name] = {
            "status": "needs_update" if needs_download else "up_to_date",
            "needs_download": needs_download,
            "reason": reason,
            "last_download": cache.get("last_download") if cache else None,
            "enabled": config.get("enabled", True),
        }

    return status


def format_status_table(status: Dict) -> str:
    """Formate le statut en tableau ASCII."""
    lines = []
    lines.append("=" * 70)
    lines.append("STATUT DES SOURCES DE DONNÉES")
    lines.append("=" * 70)
    lines.append(f"{'Source':<25} | {'Dernier DL':<20} | {'Statut':<15}")
    lines.append("-" * 70)

    for source_name, info in status.items():
        last_dl = info.get("last_download") or "Jamais"
        if last_dl and last_dl != "Jamais":
            # Formater la date
            try:
                dt = datetime.fromisoformat(last_dl.replace("Z", "+00:00"))
                last_dl = dt.strftime("%d/%m/%Y %H:%M")
            except:
                last_dl = "Erreur date"

        statut = "A jour" if not info.get("needs_download") else "Obsolete"
        if not info.get("enabled", True):
            statut = "Desactive"

        lines.append(f"{source_name:<25} | {last_dl:<20} | {statut:<15}")

    lines.append("=" * 70)
    return "\n".join(lines)
