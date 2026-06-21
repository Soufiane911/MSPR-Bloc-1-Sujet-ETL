"""
Router pour la veille technologique (technological watch).

Endpoints:
  GET  /watch/items          — liste paginée, filtres source / tag
  GET  /watch/items/{id}     — item unique
  GET  /watch/sources        — statut par source
  POST /watch/refresh        — déclenche un fetch de toutes les sources
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional

from app.models.watch import WatchItem, WatchRefreshResult, WatchSourceStatus
from app.services.watch_service import WatchService

router = APIRouter()
watch_service = WatchService()


@router.get("/")
def get_watch_overview():
    """Vue d'ensemble de la veille (totaux, dernière collecte, statut des sources)."""
    return watch_service.get_overview()


@router.get("/items", response_model=List[WatchItem])
def get_watch_items(
    source: Optional[str] = Query(
        None,
        description="Filtrer par source: newsapi, hackernews, reddit, rss, google_alerts, feedly",
    ),
    tag: Optional[str] = Query(None, description="Filtrer par tag (recherche exacte dans le tableau)"),
    limit: int = Query(50, ge=1, le=500, description="Nombre maximum de résultats"),
    offset: int = Query(0, ge=0, description="Décalage pour la pagination"),
):
    """
    Retourne les items de veille technologique agrégés depuis toutes les sources.

    Les items sont triés par date de publication décroissante.
    """
    items = watch_service.get_items(source=source, tag=tag, limit=limit, offset=offset)
    return items


@router.get("/items/{item_id}", response_model=WatchItem)
def get_watch_item(item_id: int):
    """Retourne un item de veille par son identifiant."""
    item = watch_service.get_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item non trouvé")
    return item


@router.get("/sources", response_model=List[WatchSourceStatus])
def get_sources_status():
    """Retourne le statut de chaque source (nombre d'items, dernière collecte)."""
    rows = watch_service.get_sources_status()
    return [
        WatchSourceStatus(
            source=r["source"],
            enabled=r.get("enabled", False),
            last_fetch=r.get("last_fetch"),
            item_count=r.get("item_count", 0),
        )
        for r in rows
    ]


@router.post("/refresh", response_model=WatchRefreshResult)
def refresh_watch():
    """
    Déclenche une collecte synchrone de toutes les sources de veille.

    Retourne le nombre d'items insérés, ignorés et les éventuelles erreurs par source.
    """
    result = watch_service.refresh()
    return result
