"""
Service de veille technologique — opérations base de données.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from app.database import engine
from app.models.watch import WatchItemCreate, WatchRefreshResult
from app.services.watch_fetcher import fetch_all_sources, _url_hash, get_source_catalog

logger = logging.getLogger(__name__)


class WatchService:
    """Gestion des items de veille en base de données."""

    def __init__(self) -> None:
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Crée la table de veille si la base a été initialisée avant cette feature."""
        ddl = """
            CREATE TABLE IF NOT EXISTS watch_items (
                item_id SERIAL PRIMARY KEY,
                source VARCHAR(50) NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                summary TEXT,
                author VARCHAR(255),
                published_at TIMESTAMP,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tags TEXT[],
                url_hash VARCHAR(64) NOT NULL,
                CONSTRAINT uq_watch_url_hash UNIQUE (url_hash)
            );

            CREATE INDEX IF NOT EXISTS idx_watch_source
                ON watch_items(source);
            CREATE INDEX IF NOT EXISTS idx_watch_published_at
                ON watch_items(published_at DESC);
            CREATE INDEX IF NOT EXISTS idx_watch_fetched_at
                ON watch_items(fetched_at DESC);
        """
        with engine.begin() as conn:
            conn.execute(text(ddl))

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_items(
        self,
        source: Optional[str] = None,
        tag: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT item_id, source, title, url, summary, author,
                   published_at, fetched_at, tags
            FROM watch_items
            WHERE 1=1
        """
        params: Dict[str, Any] = {}

        if source:
            query += " AND source = :source"
            params["source"] = source

        if tag:
            query += " AND :tag = ANY(tags)"
            params["tag"] = tag

        query += " ORDER BY COALESCE(published_at, fetched_at) DESC"
        query += " LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        with engine.connect() as conn:
            rows = conn.execute(text(query), params).mappings().all()
        return [dict(r) for r in rows]

    def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        query = """
            SELECT item_id, source, title, url, summary, author,
                   published_at, fetched_at, tags
            FROM watch_items
            WHERE item_id = :item_id
        """
        with engine.connect() as conn:
            row = conn.execute(text(query), {"item_id": item_id}).mappings().first()
        return dict(row) if row else None

    def get_sources_status(self) -> List[Dict[str, Any]]:
        query = """
            SELECT source,
                   COUNT(*)           AS item_count,
                   MAX(fetched_at)    AS last_fetch
            FROM watch_items
            GROUP BY source
            ORDER BY source
        """
        with engine.connect() as conn:
            rows = conn.execute(text(query)).mappings().all()
        counts = {r["source"]: dict(r) for r in rows}

        merged: List[Dict[str, Any]] = []
        for src in get_source_catalog():
            current = counts.get(src["source"], {})
            merged.append({
                "source": src["source"],
                "enabled": src["enabled"],
                "last_fetch": current.get("last_fetch"),
                "item_count": current.get("item_count", 0),
            })
        return merged

    def get_overview(self) -> Dict[str, Any]:
        query = """
            SELECT COUNT(*) AS total_items,
                   MAX(fetched_at) AS last_fetch
            FROM watch_items
        """
        with engine.connect() as conn:
            row = conn.execute(text(query)).mappings().first()

        sources = self.get_sources_status()
        enabled_sources = [s["source"] for s in sources if s["enabled"]]
        return {
            "total_items": row.get("total_items", 0) if row else 0,
            "last_fetch": row.get("last_fetch") if row else None,
            "enabled_sources": enabled_sources,
            "sources": sources,
        }

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_item(self, item: WatchItemCreate, conn) -> bool:
        """Insert a single item; returns True if inserted, False if duplicate."""
        h = _url_hash(item.url)
        try:
            conn.execute(
                text("""
                    INSERT INTO watch_items
                        (source, title, url, summary, author, published_at, tags, url_hash)
                    VALUES
                        (:source, :title, :url, :summary, :author, :published_at, :tags, :url_hash)
                    ON CONFLICT (url_hash) DO NOTHING
                """),
                {
                    "source": item.source,
                    "title": item.title[:500],
                    "url": item.url,
                    "summary": item.summary,
                    "author": item.author,
                    "published_at": item.published_at,
                    "tags": item.tags or [],
                    "url_hash": h,
                },
            )
            return True
        except Exception as exc:
            logger.warning("insert_item failed for %s: %s", item.url, exc)
            return False

    def refresh(self) -> WatchRefreshResult:
        """Fetch all sources and persist new items."""
        items, errors = fetch_all_sources()

        inserted = 0
        skipped = 0
        with engine.begin() as conn:
            for item in items:
                if not item.url:
                    skipped += 1
                    continue
                # Check if already present
                h = _url_hash(item.url)
                exists = conn.execute(
                    text("SELECT 1 FROM watch_items WHERE url_hash = :h"),
                    {"h": h},
                ).first()
                if exists:
                    skipped += 1
                    continue
                ok = self.insert_item(item, conn)
                if ok:
                    inserted += 1
                else:
                    skipped += 1

        return WatchRefreshResult(inserted=inserted, skipped=skipped, errors=errors)
