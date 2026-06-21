"""
Fetchers de veille technologique.

Sources supportées :
  - google_alerts  : flux RSS exportés depuis Google Alerts
  - newsapi        : API NewsAPI.org (clé NEWSAPI_KEY requise)
  - hackernews     : Algolia HN Search API (sans auth)
  - reddit         : Reddit JSON API (sans auth officielle)
  - rss            : flux RSS generiques (blogs ferroviaires / UIC / OpenRail)
  - feedly         : Feedly Developer API (clé FEEDLY_TOKEN + stream ID requis)
"""

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List

import httpx
import feedparser

from app.models.watch import WatchItemCreate

logger = logging.getLogger(__name__)

SOURCE_GOOGLE_ALERTS = "google_alerts"
SOURCE_RSS = "rss"
SOURCE_NEWSAPI = "newsapi"
SOURCE_HACKERNEWS = "hackernews"
SOURCE_REDDIT = "reddit"
SOURCE_FEEDLY = "feedly"

ALL_SOURCE_NAMES = [
    SOURCE_GOOGLE_ALERTS,
    SOURCE_RSS,
    SOURCE_NEWSAPI,
    SOURCE_HACKERNEWS,
    SOURCE_REDDIT,
    SOURCE_FEEDLY,
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RAIL_KEYWORDS = [
    "night train", "train de nuit", "rail", "railway", "railroad",
    "SNCF", "Deutsche Bahn", "Trenitalia", "Renfe", "ÖBB", "GTFS",
    "interrail", "Eurostar", "TGV", "ICE", "sleeper train",
    "ferroviaire", "eisenbahn",
]

RSS_FEEDS = [
    # UIC news
    "https://uic.org/news/rss.xml",
    # OpenRailAssociation blog
    "https://openrailassociation.org/feed/",
    # Back-on-Track blog
    "https://back-on-track.eu/feed/",
    # European Railway Review
    "https://www.railwaygazette.com/rss",
    # International Railway Journal
    "https://www.railjournal.com/feed/",
]

def _list_from_env(name: str) -> List[str]:
    return [v.strip() for v in os.getenv(name, "").split(",") if v.strip()]


def get_enabled_sources() -> List[str]:
    """Return source names enabled through WATCH_ENABLED_SOURCES env var."""
    configured = _list_from_env("WATCH_ENABLED_SOURCES")
    if not configured:
        return ALL_SOURCE_NAMES
    allowed = set(ALL_SOURCE_NAMES)
    return [name for name in configured if name in allowed]


def get_source_catalog() -> List[Dict[str, bool | str]]:
    """Return static source catalog with enabled flag."""
    enabled = set(get_enabled_sources())
    return [{"source": name, "enabled": name in enabled} for name in ALL_SOURCE_NAMES]


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    # feedparser returns a time.struct_time
    try:
        import time
        ts = time.mktime(value)
        return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Google Alerts RSS
# ---------------------------------------------------------------------------

def fetch_google_alerts() -> List[WatchItemCreate]:
    google_alert_rss_urls = _list_from_env("GOOGLE_ALERTS_RSS_URLS")
    items: List[WatchItemCreate] = []
    for feed_url in google_alert_rss_urls:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                items.append(WatchItemCreate(
                    source=SOURCE_GOOGLE_ALERTS,
                    title=entry.get("title", ""),
                    url=entry.get("link", feed_url),
                    summary=entry.get("summary"),
                    published_at=_parse_dt(entry.get("published_parsed")),
                    tags=[SOURCE_GOOGLE_ALERTS],
                ))
        except Exception as exc:
            logger.warning("google_alerts feed %s error: %s", feed_url, exc)
    return items


# ---------------------------------------------------------------------------
# Generic RSS feeds
# ---------------------------------------------------------------------------

def fetch_rss() -> List[WatchItemCreate]:
    items: List[WatchItemCreate] = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                items.append(WatchItemCreate(
                    source=SOURCE_RSS,
                    title=entry.get("title", ""),
                    url=entry.get("link", feed_url),
                    summary=entry.get("summary"),
                    author=entry.get("author"),
                    published_at=_parse_dt(entry.get("published_parsed")),
                    tags=[SOURCE_RSS, feed.feed.get("title", "")],
                ))
        except Exception as exc:
            logger.warning("rss feed %s error: %s", feed_url, exc)
    return items


# ---------------------------------------------------------------------------
# NewsAPI
# ---------------------------------------------------------------------------

NEWSAPI_ENDPOINT = "https://newsapi.org/v2/everything"
NEWSAPI_QUERY = (
    "night train OR sleeper train OR train de nuit OR railway OR ferroviaire"
)


def fetch_newsapi() -> List[WatchItemCreate]:
    newsapi_key = os.getenv("NEWSAPI_KEY", "")
    if not newsapi_key:
        logger.info("NEWSAPI_KEY not set, skipping NewsAPI fetch")
        return []
    items: List[WatchItemCreate] = []
    try:
        resp = httpx.get(
            NEWSAPI_ENDPOINT,
            params={
                "q": NEWSAPI_QUERY,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 50,
                "apiKey": newsapi_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        for article in resp.json().get("articles", []):
            pub = article.get("publishedAt")
            pub_dt = datetime.fromisoformat(pub.replace("Z", "+00:00")).replace(tzinfo=None) if pub else None
            items.append(WatchItemCreate(
                source=SOURCE_NEWSAPI,
                title=article.get("title", ""),
                url=article.get("url", ""),
                summary=article.get("description"),
                author=article.get("author"),
                published_at=pub_dt,
                tags=[SOURCE_NEWSAPI],
            ))
    except Exception as exc:
        logger.warning("newsapi error: %s", exc)
    return items


# ---------------------------------------------------------------------------
# HackerNews (Algolia Search API — no auth)
# ---------------------------------------------------------------------------

HN_ENDPOINT = "https://hn.algolia.com/api/v1/search_by_date"
HN_QUERY = "night train OR railway OR GTFS OR rail Europe"


def fetch_hackernews() -> List[WatchItemCreate]:
    items: List[WatchItemCreate] = []
    try:
        resp = httpx.get(
            HN_ENDPOINT,
            params={"query": HN_QUERY, "hitsPerPage": 30},
            timeout=15,
        )
        resp.raise_for_status()
        for hit in resp.json().get("hits", []):
            url = hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
            created = hit.get("created_at")
            pub_dt = datetime.fromisoformat(created.replace("Z", "+00:00")).replace(tzinfo=None) if created else None
            items.append(WatchItemCreate(
                source=SOURCE_HACKERNEWS,
                title=hit.get("title", ""),
                url=url,
                summary=hit.get("story_text"),
                author=hit.get("author"),
                published_at=pub_dt,
                tags=[SOURCE_HACKERNEWS],
            ))
    except Exception as exc:
        logger.warning("hackernews error: %s", exc)
    return items


# ---------------------------------------------------------------------------
# Reddit (public JSON API — no auth for read-only)
# ---------------------------------------------------------------------------

REDDIT_SUBREDDITS = ["trains", "eurail", "europe", "transit"]
REDDIT_ENDPOINT = "https://www.reddit.com/r/{sub}/search.json"
REDDIT_QUERY = "night train OR sleeper OR railway OR ferroviaire"


def fetch_reddit() -> List[WatchItemCreate]:
    items: List[WatchItemCreate] = []
    headers = {"User-Agent": "ObRailEurope-VeilleBot/1.0"}
    try:
        for sub in REDDIT_SUBREDDITS:
            resp = httpx.get(
                REDDIT_ENDPOINT.format(sub=sub),
                params={"q": REDDIT_QUERY, "restrict_sr": 1, "sort": "new", "limit": 15},
                headers=headers,
                timeout=15,
                follow_redirects=True,
            )
            resp.raise_for_status()
            for post in resp.json().get("data", {}).get("children", []):
                data = post.get("data", {})
                created = data.get("created_utc")
                pub_dt = datetime.utcfromtimestamp(created) if created else None
                url = data.get("url", "")
                if not url:
                    continue
                items.append(WatchItemCreate(
                    source=SOURCE_REDDIT,
                    title=data.get("title", ""),
                    url=url,
                    summary=data.get("selftext") or None,
                    author=data.get("author"),
                    published_at=pub_dt,
                    tags=[SOURCE_REDDIT, sub],
                ))
    except Exception as exc:
        logger.warning("reddit error: %s", exc)
    return items


# ---------------------------------------------------------------------------
# Feedly (Developer API — requires FEEDLY_TOKEN + FEEDLY_STREAM_IDs)
# ---------------------------------------------------------------------------

FEEDLY_ENDPOINT = "https://cloud.feedly.com/v3/streams/contents"


def fetch_feedly() -> List[WatchItemCreate]:
    """
    Fetch articles from Feedly streams.

    Requires:
      FEEDLY_TOKEN   — Developer access token from https://feedly.com/v3/auth/dev
      FEEDLY_STREAM_IDS — comma-separated stream IDs (feeds or categories)

    Free developer tokens are personal-use only.
    Production use requires a Feedly Team plan.
    """
    feedly_token = os.getenv("FEEDLY_TOKEN", "")
    feedly_stream_ids = _list_from_env("FEEDLY_STREAM_IDS")
    if not feedly_token or not feedly_stream_ids:
        logger.info("FEEDLY_TOKEN or FEEDLY_STREAM_IDS not set, skipping Feedly fetch")
        return []
    items: List[WatchItemCreate] = []
    headers = {"Authorization": f"OAuth {feedly_token}"}
    for stream_id in feedly_stream_ids:
        try:
            resp = httpx.get(
                FEEDLY_ENDPOINT,
                params={"streamId": stream_id, "count": 40, "ranked": "newest"},
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            for entry in resp.json().get("items", []):
                url = (entry.get("canonicalUrl")
                       or (entry.get("alternate") or [{}])[0].get("href", ""))
                if not url:
                    continue
                published_ms = entry.get("published")
                pub_dt = datetime.utcfromtimestamp(published_ms / 1000) if published_ms else None
                summary_html = (entry.get("summary") or {}).get("content")
                items.append(WatchItemCreate(
                    source=SOURCE_FEEDLY,
                    title=entry.get("title", ""),
                    url=url,
                    summary=summary_html,
                    author=(entry.get("author") or None),
                    published_at=pub_dt,
                    tags=[SOURCE_FEEDLY, stream_id],
                ))
        except Exception as exc:
            logger.warning("feedly stream %s error: %s", stream_id, exc)
    return items


# ---------------------------------------------------------------------------
# Master fetch function
# ---------------------------------------------------------------------------

SOURCE_FETCHERS = {
    SOURCE_GOOGLE_ALERTS: fetch_google_alerts,
    SOURCE_RSS: fetch_rss,
    SOURCE_NEWSAPI: fetch_newsapi,
    SOURCE_HACKERNEWS: fetch_hackernews,
    SOURCE_REDDIT: fetch_reddit,
    SOURCE_FEEDLY: fetch_feedly,
}


def fetch_all_sources() -> tuple[List[WatchItemCreate], List[str]]:
    """Run all fetchers and return (items, error_messages)."""
    all_items: List[WatchItemCreate] = []
    errors: List[str] = []
    for name in get_enabled_sources():
        fetcher = SOURCE_FETCHERS[name]
        try:
            fetched = fetcher()
            all_items.extend(fetched)
            logger.info("source=%s fetched=%d", name, len(fetched))
        except Exception as exc:
            msg = f"{name}: {exc}"
            errors.append(msg)
            logger.error("fetch_all_sources %s", msg)
    return all_items, errors
