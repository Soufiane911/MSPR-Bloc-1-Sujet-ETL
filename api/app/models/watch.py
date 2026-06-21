"""
Modèles Pydantic pour la veille technologique.
"""

from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, List
from datetime import datetime


class WatchItem(BaseModel):
    """Représente un item de veille technologique."""

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "item_id": 1,
                "source": "newsapi",
                "title": "European Night Train Network Expands",
                "url": "https://example.com/article",
                "summary": "New night train routes connecting...",
                "author": "Jane Doe",
                "published_at": "2026-06-01T08:00:00",
                "fetched_at": "2026-06-01T09:00:00",
                "tags": ["night train", "europe", "rail"],
            }
        },
    )

    item_id: int = Field(..., description="Identifiant unique")
    source: str = Field(..., description="Source: newsapi, hackernews, reddit, rss, google_alerts, feedly")
    title: str = Field(..., description="Titre de l'article")
    url: str = Field(..., description="URL de l'article")
    summary: Optional[str] = Field(None, description="Résumé ou description")
    author: Optional[str] = Field(None, description="Auteur")
    published_at: Optional[datetime] = Field(None, description="Date de publication")
    fetched_at: datetime = Field(..., description="Date de collecte")
    tags: Optional[List[str]] = Field(default_factory=list, description="Tags associés")


class WatchItemCreate(BaseModel):
    """Schéma interne pour l'insertion d'un item."""

    source: str
    title: str
    url: str
    summary: Optional[str] = None
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    tags: Optional[List[str]] = None


class WatchRefreshResult(BaseModel):
    """Résultat d'un refresh de la veille."""

    inserted: int = Field(..., description="Nombre de nouveaux items insérés")
    skipped: int = Field(..., description="Nombre d'items ignorés (déjà présents)")
    errors: List[str] = Field(default_factory=list, description="Erreurs éventuelles par source")


class WatchSourceStatus(BaseModel):
    """Statut d'une source de veille."""

    source: str
    enabled: bool
    last_fetch: Optional[datetime] = None
    item_count: int = 0
